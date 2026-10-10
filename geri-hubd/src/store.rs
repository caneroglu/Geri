use base64::Engine;
use geri_core::v4::{AgentPolicy, Reject, becomes_current, decide, validate_batch};
use geri_proto::v4::{
    self, Batch, BlobRef, Command, CommandAudit, CommandRequest, CommandResult, CommandStatus,
    Receipt, ReceiptStatus, Record, RecordKind, RecordPage, RecordReceipt, Source,
    SourceDeclaration, StoredRecord, Visibility,
};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

const BLOB_GC_CHUNK: usize = 100;
const COMMAND_RETENTION_MS: u64 = 90 * 24 * 60 * 60 * 1000;

struct PendingCommand {
    id: i64,
    entity_id: String,
    action: String,
    raw_args: String,
    issued_by: String,
    issued_at_ms: u64,
    control_json: Option<String>,
}

const RECORD_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS records_source_cursor ON records(source_id, cursor);
CREATE INDEX IF NOT EXISTS records_time ON records(accepted_at_ms, cursor);
CREATE UNIQUE INDEX IF NOT EXISTS records_sequence ON records(source_id, sequence) WHERE sequence > 0;
CREATE INDEX IF NOT EXISTS records_source_time ON records(source_id, accepted_at_ms);
CREATE INDEX IF NOT EXISTS records_source_events ON records(source_id, cursor) WHERE kind IN ('event','raw');
CREATE INDEX IF NOT EXISTS receipts_pruned ON receipts(pruned_at_ms);
";

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sources (
 source_id TEXT PRIMARY KEY, label TEXT NOT NULL, kind TEXT NOT NULL,
 created_at_ms INTEGER NOT NULL, visibility TEXT NOT NULL DEFAULT 'private',
 last_sequence INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS entities (
 source_id TEXT NOT NULL, entity_id TEXT NOT NULL, label TEXT NOT NULL, kind TEXT NOT NULL, control_json TEXT, group_name TEXT, ordinal INTEGER,
 PRIMARY KEY(source_id, entity_id)
);
CREATE TABLE IF NOT EXISTS records (
 cursor INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL, entity_id TEXT NOT NULL,
 record_id TEXT NOT NULL, sequence INTEGER NOT NULL, kind TEXT NOT NULL,
 event_time_ms INTEGER, accepted_at_ms INTEGER NOT NULL, content_type TEXT NOT NULL,
 raw_json TEXT NOT NULL, search_text TEXT NOT NULL, blob_sha TEXT,
 UNIQUE(source_id, record_id)
);
CREATE TABLE IF NOT EXISTS current_state (
 source_id TEXT NOT NULL, entity_id TEXT NOT NULL, cursor INTEGER NOT NULL,
 PRIMARY KEY(source_id, entity_id)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
INSERT OR IGNORE INTO meta(key, value) VALUES('pruned_through', 0);
CREATE TABLE IF NOT EXISTS receipts (
 source_id TEXT NOT NULL, record_id TEXT NOT NULL, sequence INTEGER NOT NULL,
 cursor INTEGER NOT NULL, raw_sha TEXT NOT NULL, pruned_at_ms INTEGER,
 PRIMARY KEY(source_id, record_id)
);
CREATE TABLE IF NOT EXISTS blobs(sha256 TEXT PRIMARY KEY,size INTEGER NOT NULL,created_at_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS commands (
 id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL, entity_id TEXT NOT NULL,
 action TEXT NOT NULL, args_json TEXT NOT NULL, issued_by TEXT NOT NULL,
 issued_at_ms INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
 dispatched_at_ms INTEGER, result_json TEXT, completed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS commands_pending ON commands(source_id,status,id);
CREATE TABLE IF NOT EXISTS agent_counters (
 agent_id TEXT PRIMARY KEY, max INTEGER NOT NULL, seen INTEGER NOT NULL
);
";

#[derive(Debug)]
pub enum StoreError {
    Invalid(Reject),
    Forbidden,
    MissingBlob,
    SequenceExhausted,
    Db(rusqlite::Error),
    Io(io::Error),
    Json(serde_json::Error),
    Expired(u64),
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(e) => write!(f, "invalid record: {e:?}"),
            Self::Forbidden => write!(f, "forbidden"),
            Self::MissingBlob => write!(f, "missing blob"),
            Self::SequenceExhausted => write!(f, "source sequence is exhausted"),
            Self::Db(e) => write!(f, "SQLite: {e}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::Json(e) => write!(f, "JSON: {e}"),
            Self::Expired(c) => write!(f, "cursor expired; earliest {c}"),
        }
    }
}
impl std::error::Error for StoreError {}
impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Db(e)
    }
}
impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

fn allow_unordered(conn: &Connection) -> Result<(), StoreError> {
    let sql: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='records'",
        [],
        |r| r.get(0),
    )?;
    if !sql.contains("UNIQUE(source_id, sequence)") {
        return Ok(());
    }
    let table = SCHEMA
        .split_once("CREATE TABLE IF NOT EXISTS records (")
        .and_then(|(_, rest)| rest.split_once(");"))
        .map(|(columns, _)| format!("CREATE TABLE records_v2 ({columns});"))
        .expect("records table in SCHEMA");
    conn.execute_batch(&format!(
        "BEGIN IMMEDIATE;
         {table}
         INSERT INTO records_v2 SELECT cursor,source_id,entity_id,record_id,sequence,kind,
           event_time_ms,accepted_at_ms,content_type,raw_json,search_text,blob_sha FROM records;
         CREATE TEMP TABLE keep_seq AS SELECT seq FROM sqlite_sequence WHERE name='records';
         DROP TABLE records;
         ALTER TABLE records_v2 RENAME TO records;
         DELETE FROM sqlite_sequence WHERE name='records';
         INSERT INTO sqlite_sequence(name, seq)
           SELECT 'records', MAX(seq) FROM (SELECT seq FROM keep_seq
             UNION ALL SELECT MAX(cursor) FROM records) WHERE seq IS NOT NULL HAVING COUNT(*) > 0;
         DROP TABLE keep_seq;
         COMMIT;"
    ))?;
    Ok(())
}

fn add_receipt_time(conn: &Connection) -> Result<(), StoreError> {
    let columns = conn
        .prepare("PRAGMA table_info(receipts)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|c| c == "pruned_at_ms") {
        conn.execute_batch("ALTER TABLE receipts ADD COLUMN pruned_at_ms INTEGER")?;
    }
    conn.execute(
        "UPDATE receipts SET pruned_at_ms=?1 WHERE pruned_at_ms IS NULL",
        [unix_ms()],
    )?;
    Ok(())
}

fn add_command_time_and_indexes(conn: &Connection) -> Result<(), StoreError> {
    let columns = conn
        .prepare("PRAGMA table_info(commands)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|c| c == "completed_at_ms") {
        conn.execute_batch("ALTER TABLE commands ADD COLUMN completed_at_ms INTEGER")?;
    }
    conn.execute(
        "UPDATE commands SET completed_at_ms=COALESCE(dispatched_at_ms,issued_at_ms) WHERE status IN ('done','cancelled') AND completed_at_ms IS NULL",
        [],
    )?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS commands_recent_by_source ON commands(source_id,id DESC);
         CREATE INDEX IF NOT EXISTS commands_terminal_age ON commands(status,completed_at_ms,id);",
    )?;
    Ok(())
}

pub const DEFAULT_TOMBSTONE_MS: u64 = 90 * 24 * 60 * 60 * 1000;

pub struct HudData {
    declaration: SourceDeclaration,
    visibility: Visibility,
    created_at_ms: u64,
    last_seen_ms: u64,
    state: Vec<StoredRecord>,
    events: Vec<StoredRecord>,
    audit: Vec<CommandAudit>,
}

impl HudData {
    pub fn apply(self, previous: Option<geri_view::SourceView>) -> geri_view::SourceView {
        let mut view = previous.unwrap_or_else(|| {
            geri_view::SourceView::new(
                self.declaration.clone(),
                self.visibility,
                self.created_at_ms,
            )
        });
        view.declaration = self.declaration;
        view.visibility = self.visibility;
        view.state(self.state);
        if self.last_seen_ms > view.last_seen_ms {
            view.disconnected = false;
        }
        view.last_seen_ms = view.last_seen_ms.max(self.last_seen_ms);
        view.events = self.events.into_iter().map(Into::into).collect();
        view.command_audit(self.audit);
        view
    }
}

const DUPLICATE_WEBHOOK: &str = "SELECT cursor FROM records WHERE source_id=?1 AND record_id=?2 UNION ALL SELECT cursor FROM receipts WHERE source_id=?1 AND record_id=?2 LIMIT 1";

pub struct Store {
    db: Mutex<Connection>,
    blobs: PathBuf,
    tombstone_ms: u64,
}

impl Store {
    pub fn open(path: &Path, blobs: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(blobs)?;
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        conn.execute_batch(SCHEMA)?;
        allow_unordered(&conn)?;
        add_receipt_time(&conn)?;
        conn.execute_batch(RECORD_INDEXES)?;
        add_command_time_and_indexes(&conn)?;
        let columns = conn
            .prepare("PRAGMA table_info(entities)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        for (name, kind) in [
            ("control_json", "TEXT"),
            ("group_name", "TEXT"),
            ("ordinal", "INTEGER"),
        ] {
            if !columns.iter().any(|c| c == name) {
                conn.execute(
                    &format!("ALTER TABLE entities ADD COLUMN {name} {kind}"),
                    [],
                )?;
            }
        }
        Ok(Self {
            db: Mutex::new(conn),
            blobs: blobs.to_owned(),
            tombstone_ms: DEFAULT_TOMBSTONE_MS,
        })
    }

    pub fn with_tombstones(mut self, ms: u64) -> Self {
        self.tombstone_ms = ms;
        self
    }

    #[cfg(test)]
    pub fn memory(blobs: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(blobs)?;
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        conn.execute_batch(RECORD_INDEXES)?;
        Ok(Self {
            db: Mutex::new(conn),
            blobs: blobs.to_owned(),
            tombstone_ms: DEFAULT_TOMBSTONE_MS,
        })
    }

    pub fn blob_path(&self, digest: &str) -> PathBuf {
        self.blobs.join(&digest[..2]).join(digest)
    }

    pub fn put_blob(&self, digest: &str, bytes: &[u8]) -> Result<BlobRef, StoreError> {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || format!("{:x}", Sha256::digest(bytes)) != digest
        {
            return Err(StoreError::Invalid(Reject::Payload));
        }
        let conn = self.db.lock().unwrap();
        let path = self.blob_path(digest);
        fs::create_dir_all(path.parent().unwrap())?;
        if fs::metadata(&path).map_or(true, |m| m.len() != bytes.len() as u64) {
            static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);
            let tmp = path.with_extension(format!(
                "tmp-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            fs::rename(&tmp, &path)?;
            #[cfg(not(windows))]
            fs::File::open(path.parent().unwrap())?.sync_all()?;
        }
        conn.execute(
            "INSERT INTO blobs(sha256,size,created_at_ms) VALUES(?1,?2,?3) ON CONFLICT(sha256) DO UPDATE SET size=excluded.size,created_at_ms=MAX(blobs.created_at_ms,excluded.created_at_ms)",
            params![digest, bytes.len() as u64, unix_ms()],
        )?;
        Ok(BlobRef {
            sha256: digest.into(),
            size: bytes.len() as u64,
        })
    }

    pub fn get_blob(&self, digest: &str) -> Result<Option<Vec<u8>>, StoreError> {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(StoreError::Invalid(Reject::Payload));
        }
        match fs::read(self.blob_path(digest)) {
            Ok(bytes) if format!("{:x}", Sha256::digest(&bytes)) == digest => Ok(Some(bytes)),
            Ok(_) => Err(StoreError::Invalid(Reject::Payload)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn has_blob(&self, blob: &BlobRef) -> bool {
        fs::metadata(self.blob_path(&blob.sha256)).is_ok_and(|m| m.len() == blob.size)
    }

    pub fn ingest(
        &self,
        policy: &AgentPolicy,
        batch: &Batch,
        now_ms: u64,
    ) -> Result<Receipt, StoreError> {
        validate_batch(batch).map_err(StoreError::Invalid)?;
        for r in &batch.records {
            if !policy.can_write(&r.source_id) {
                return Err(StoreError::Forbidden);
            }
        }
        let mut conn = self.db.lock().unwrap();
        let tx = conn.transaction()?;
        let mut receipts = Vec::with_capacity(batch.records.len());
        for r in &batch.records {
            tx.execute("INSERT OR IGNORE INTO sources(source_id,label,kind,created_at_ms) VALUES(?1,?1,'unknown',?2)", params![r.source_id, now_ms])?;
            let existing: Option<(u64,u64,String)> = tx.query_row(
                "SELECT sequence,cursor,raw_json FROM records WHERE source_id=?1 AND record_id=?2",
                params![r.source_id,r.record_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))
            ).optional()?;
            let expired: Option<(u64, u64, String)> = if existing.is_none() {
                tx.query_row("SELECT sequence,cursor,raw_sha FROM receipts WHERE source_id=?1 AND record_id=?2",
                    params![r.source_id,r.record_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))).optional()?
            } else {
                None
            };
            let last: u64 = tx.query_row(
                "SELECT last_sequence FROM sources WHERE source_id=?1",
                [&r.source_id],
                |row| row.get(0),
            )?;
            let previous = existing.as_ref().or(expired.as_ref());
            let mut receipt = decide(r, previous.map(|(seq, cursor, _)| (*seq, *cursor)), last);
            let same = if let Some((_, _, raw)) = &existing {
                serde_json::from_str::<Record>(raw).ok().as_ref() == Some(r)
            } else if let Some((_, _, sha)) = &expired {
                *sha == format!("{:x}", Sha256::digest(serde_json::to_vec(r)?))
            } else {
                false
            };
            if receipt.status == ReceiptStatus::Duplicate && !same {
                receipt.status = ReceiptStatus::Conflict;
                receipt.cursor = None;
            }
            if receipt.status == ReceiptStatus::Accepted {
                if r.blob.as_ref().is_some_and(|b| !self.has_blob(b)) {
                    return Err(StoreError::MissingBlob);
                }
                let raw = serde_json::to_string(r)?;
                let search = r
                    .indexed
                    .as_ref()
                    .or(r.data.as_ref())
                    .map(serde_json::to_string)
                    .transpose()?
                    .unwrap_or_default();
                tx.execute("INSERT INTO records(source_id,entity_id,record_id,sequence,kind,event_time_ms,accepted_at_ms,content_type,raw_json,search_text,blob_sha) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                    params![r.source_id,r.entity_id,r.record_id,r.sequence,kind(r.kind),r.event_time_ms,now_ms,r.content_type,raw,search,r.blob.as_ref().map(|b| &b.sha256)])?;
                let cursor = tx.last_insert_rowid() as u64;
                if r.sequence > 0 {
                    tx.execute(
                        "UPDATE sources SET last_sequence=?2 WHERE source_id=?1",
                        params![r.source_id, r.sequence],
                    )?;
                }
                tx.execute("INSERT OR IGNORE INTO entities(source_id,entity_id,label,kind) VALUES(?1,?2,?2,'unknown')", params![r.source_id,r.entity_id])?;
                let current: Option<Option<u64>> = if r.kind == RecordKind::State && r.sequence == 0
                {
                    tx.query_row("SELECT r.event_time_ms FROM current_state c JOIN records r ON r.cursor=c.cursor WHERE c.source_id=?1 AND c.entity_id=?2",
                        params![r.source_id, r.entity_id], |row| row.get(0)).optional()?
                } else {
                    None
                };
                if r.kind == RecordKind::State && becomes_current(r, current.flatten()) {
                    tx.execute("INSERT INTO current_state(source_id,entity_id,cursor) VALUES(?1,?2,?3) ON CONFLICT(source_id,entity_id) DO UPDATE SET cursor=excluded.cursor", params![r.source_id,r.entity_id,cursor])?;
                }
                receipt.cursor = Some(cursor);
            }
            receipts.push(receipt);
        }
        tx.commit()?;
        Ok(Receipt {
            v: v4::VERSION,
            records: receipts,
        })
    }

    pub fn ingest_webhook(
        &self,
        policy: &AgentPolicy,
        source: &str,
        entity: &str,
        external_id: &str,
        content_type: &str,
        body: &[u8],
        now_ms: u64,
    ) -> Result<RecordReceipt, StoreError> {
        if !policy.can_write(source) {
            return Err(StoreError::Forbidden);
        }
        if !v4::valid_id(source)
            || !v4::valid_id(entity)
            || external_id.is_empty()
            || external_id.len() > 1024
            || content_type.len() > 128
        {
            return Err(StoreError::Invalid(Reject::InvalidId));
        }
        let record_id = format!("{:x}", Sha256::digest(external_id.as_bytes()));
        let existing: Option<u64> = self
            .db
            .lock()
            .unwrap()
            .query_row(DUPLICATE_WEBHOOK, params![source, record_id], |r| r.get(0))
            .optional()?;
        if let Some(cursor) = existing {
            return Ok(RecordReceipt {
                source_id: source.into(),
                record_id,
                status: ReceiptStatus::Duplicate,
                cursor: Some(cursor),
                expected_sequence: None,
            });
        }
        let inline = body.len() <= 32 * 1024;
        let data = if inline {
            let parsed = if content_type.starts_with("application/json") {
                serde_json::from_slice::<serde_json::Value>(body).ok()
            } else {
                None
            };
            Some(
                serde_json::json!({"external_id":external_id,"original_content_type":content_type,
                "raw_base64":base64::engine::general_purpose::STANDARD.encode(body),"parsed":parsed}),
            )
        } else {
            None
        };
        let blob = if inline {
            None
        } else {
            let digest = format!("{:x}", Sha256::digest(body));
            Some(self.put_blob(&digest, body)?)
        };
        let mut conn = self.db.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("INSERT OR IGNORE INTO sources(source_id,label,kind,created_at_ms) VALUES(?1,?1,'webhook',?2)", params![source,now_ms])?;
        if let Some(cursor) = tx
            .query_row(DUPLICATE_WEBHOOK, params![source, record_id], |r| {
                r.get::<_, u64>(0)
            })
            .optional()?
        {
            return Ok(RecordReceipt {
                source_id: source.into(),
                record_id,
                status: ReceiptStatus::Duplicate,
                cursor: Some(cursor),
                expected_sequence: None,
            });
        }
        let record = Record {
            source_id: source.into(),
            entity_id: entity.into(),
            record_id: record_id.clone(),
            sequence: 0,
            kind: RecordKind::Event,
            event_time_ms: None,
            content_type: if inline {
                "application/json".into()
            } else {
                content_type.into()
            },
            data,
            indexed: if inline {
                None
            } else {
                Some(
                    serde_json::json!({"external_id":external_id,"original_content_type":content_type}),
                )
            },
            blob,
        };
        geri_core::v4::validate_record(&record).map_err(StoreError::Invalid)?;
        let raw = serde_json::to_string(&record)?;
        let search = record
            .indexed
            .as_ref()
            .or(record.data.as_ref())
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_default();
        tx.execute("INSERT INTO records(source_id,entity_id,record_id,sequence,kind,event_time_ms,accepted_at_ms,content_type,raw_json,search_text,blob_sha) VALUES(?1,?2,?3,?4,'event',NULL,?5,?6,?7,?8,?9)",
            params![source,entity,record_id,record.sequence,now_ms,record.content_type,raw,search,record.blob.as_ref().map(|b| &b.sha256)])?;
        let cursor = tx.last_insert_rowid() as u64;
        tx.execute("INSERT OR IGNORE INTO entities(source_id,entity_id,label,kind) VALUES(?1,?2,?2,'webhook')", params![source,entity])?;
        tx.commit()?;
        Ok(RecordReceipt {
            source_id: source.into(),
            record_id,
            status: ReceiptStatus::Accepted,
            cursor: Some(cursor),
            expected_sequence: None,
        })
    }

    pub fn sources(&self, policy: &AgentPolicy) -> Result<Vec<Source>, StoreError> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT source_id,label,kind,created_at_ms,visibility FROM sources ORDER BY source_id",
        )?;
        let rows = stmt.query_map([], |row| {
            let visibility: String = row.get(4)?;
            Ok(Source {
                source_id: row.get(0)?,
                label: row.get(1)?,
                kind: row.get(2)?,
                created_at_ms: row.get(3)?,
                visibility: parse_visibility(&visibility),
            })
        })?;
        let rows = rows.collect::<Result<Vec<_>, _>>()?;
        Ok(rows
            .into_iter()
            .filter(|s| policy.can_read(s.visibility) || policy.can_write(&s.source_id))
            .collect())
    }

    pub fn upsert_source(
        &self,
        policy: &AgentPolicy,
        declaration: &SourceDeclaration,
        now_ms: u64,
    ) -> Result<u64, StoreError> {
        geri_core::v4::validate_source(declaration).map_err(StoreError::Invalid)?;
        if !policy.can_write(&declaration.source_id) {
            return Err(StoreError::Forbidden);
        }
        let mut conn = self.db.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("INSERT INTO sources(source_id,label,kind,created_at_ms) VALUES(?1,?2,?3,?4) ON CONFLICT(source_id) DO UPDATE SET label=excluded.label,kind=excluded.kind",
            params![declaration.source_id,declaration.label,declaration.kind,now_ms])?;
        let previous = {
            let mut stmt = tx.prepare("SELECT entity_id,control_json FROM entities WHERE source_id=?1 AND ordinal IS NOT NULL")?;
            stmt.query_map([&declaration.source_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?
        };
        tx.execute(
            "UPDATE entities SET ordinal=NULL WHERE source_id=?1 AND ordinal IS NOT NULL",
            [&declaration.source_id],
        )?;
        for (ordinal, entity) in declaration.entities.iter().enumerate() {
            let old = previous
                .get(&entity.entity_id)
                .and_then(|raw| raw.as_ref())
                .and_then(|raw| serde_json::from_str::<geri_proto::Control>(raw).ok());
            // Etiket/grup değişimi yetkiyi değiştirmez; shape veya danger değişimi değiştirir.
            if old.as_ref() != entity.control.as_ref() || old.is_none() {
                tx.execute("UPDATE commands SET status='cancelled',completed_at_ms=?3 WHERE source_id=?1 AND entity_id=?2 AND status='queued'",
                    params![declaration.source_id, entity.entity_id, now_ms])?;
            }
            let control = entity
                .control
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            tx.execute("INSERT INTO entities(source_id,entity_id,label,kind,control_json,group_name,ordinal) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(source_id,entity_id) DO UPDATE SET label=excluded.label,kind=excluded.kind,control_json=excluded.control_json,group_name=excluded.group_name,ordinal=excluded.ordinal",
                params![entity.source_id,entity.entity_id,entity.label,entity.kind,control,entity.group,ordinal as i64])?;
        }
        tx.execute("UPDATE commands SET status='cancelled',completed_at_ms=?2 WHERE source_id=?1 AND status='queued' AND NOT EXISTS (SELECT 1 FROM entities e WHERE e.source_id=commands.source_id AND e.entity_id=commands.entity_id AND e.ordinal IS NOT NULL AND e.control_json IS NOT NULL)", params![declaration.source_id, now_ms])?;
        tx.execute("UPDATE entities SET control_json=NULL WHERE source_id=?1 AND ordinal IS NULL AND control_json IS NOT NULL", [&declaration.source_id])?;
        let last: u64 = tx.query_row(
            "SELECT last_sequence FROM sources WHERE source_id=?1",
            [&declaration.source_id],
            |row| row.get(0),
        )?;
        if last >= geri_proto::v4::MAX_SEQUENCE {
            return Err(StoreError::SequenceExhausted);
        }
        tx.commit()?;
        Ok(last + 1)
    }

    pub fn visibility(&self, source: &str) -> Result<Option<Visibility>, StoreError> {
        let conn = self.db.lock().unwrap();
        let v: Option<String> = conn
            .query_row(
                "SELECT visibility FROM sources WHERE source_id=?1",
                [source],
                |r| r.get(0),
            )
            .optional()?;
        Ok(v.as_deref().map(parse_visibility))
    }

    pub fn set_visibility(&self, source: &str, visibility: Visibility) -> Result<bool, StoreError> {
        let conn = self.db.lock().unwrap();
        Ok(conn.execute(
            "UPDATE sources SET visibility=?2 WHERE source_id=?1",
            params![
                source,
                if visibility == Visibility::Public {
                    "public"
                } else {
                    "private"
                }
            ],
        )? == 1)
    }

    pub fn hud_data(&self, id: &str) -> Result<Option<HudData>, StoreError> {
        let conn = self.db.lock().unwrap();
        let source: Option<(String, String, u64, String)> = conn
            .query_row(
                "SELECT label,kind,created_at_ms,visibility FROM sources WHERE source_id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((label, kind, created_at_ms, visibility)) = source else {
            return Ok(None);
        };
        let entities = conn
            .prepare_cached("SELECT entity_id,label,kind,control_json,group_name FROM entities WHERE source_id=?1 ORDER BY ordinal IS NULL,ordinal,entity_id")?
            .query_map([id], |r| {
                let control: Option<String> = r.get(3)?;
                Ok(v4::Entity {
                    source_id: id.into(),
                    entity_id: r.get(0)?,
                    label: r.get(1)?,
                    kind: r.get(2)?,
                    group: r.get(4)?,
                    control: control.and_then(|j| serde_json::from_str(&j).ok()),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let last_seen_ms: u64 = conn.query_row(
            "SELECT COALESCE(MAX(accepted_at_ms),0) FROM records WHERE source_id=?1",
            [id],
            |r| r.get(0),
        )?;
        let state = state_rows(&conn, id)?;
        let events = conn
            .prepare_cached("SELECT raw_json,cursor,accepted_at_ms FROM records WHERE source_id=?1 AND kind IN ('event','raw') ORDER BY cursor DESC LIMIT 12")?
            .query_map([id], parse_stored)?
            .collect::<Result<Vec<_>, _>>()?;
        let audit = audit_rows(&conn, Some(id), 12)?;
        Ok(Some(HudData {
            declaration: SourceDeclaration {
                source_id: id.into(),
                label,
                kind,
                entities,
            },
            visibility: parse_visibility(&visibility),
            created_at_ms,
            last_seen_ms,
            state,
            events,
            audit,
        }))
    }

    pub fn state(&self, source: &str) -> Result<Vec<StoredRecord>, StoreError> {
        state_rows(&self.db.lock().unwrap(), source)
    }

    pub fn records(
        &self,
        source: Option<&str>,
        cursor: u64,
        from_ms: Option<u64>,
        to_ms: Option<u64>,
        q: Option<&str>,
        limit: usize,
    ) -> Result<RecordPage, StoreError> {
        let conn = self.db.lock().unwrap();
        let floor: u64 = conn.query_row(
            "SELECT value FROM meta WHERE key='pruned_through'",
            [],
            |r| r.get(0),
        )?;
        if cursor > 0 && cursor < floor {
            return Err(StoreError::Expired(floor.saturating_add(1)));
        }
        let records = if let Some(source) = source {
            let mut stmt = conn.prepare("SELECT raw_json,cursor,accepted_at_ms FROM records WHERE cursor>?1 AND source_id=?2 AND (?3 IS NULL OR accepted_at_ms>=?3) AND (?4 IS NULL OR accepted_at_ms<=?4) AND (?5 IS NULL OR instr(search_text,?5)>0) ORDER BY cursor LIMIT ?6")?;
            stmt.query_map(
                params![cursor, source, from_ms, to_ms, q, limit.clamp(1, 500)],
                parse_stored,
            )?
            .collect::<Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare("SELECT raw_json,cursor,accepted_at_ms FROM records WHERE cursor>?1 AND (?2 IS NULL OR accepted_at_ms>=?2) AND (?3 IS NULL OR accepted_at_ms<=?3) AND (?4 IS NULL OR instr(search_text,?4)>0) ORDER BY cursor LIMIT ?5")?;
            stmt.query_map(
                params![cursor, from_ms, to_ms, q, limit.clamp(1, 500)],
                parse_stored,
            )?
            .collect::<Result<Vec<_>, _>>()?
        };
        let next_cursor = records.last().map(|r| r.cursor);
        Ok(RecordPage {
            records,
            next_cursor,
        })
    }

    pub fn prune(&self, now_ms: u64) -> Result<bool, StoreError> {
        let cutoff = now_ms.saturating_sub(v4::RETENTION_MS);
        let mut conn = self.db.lock().unwrap();
        let tx = conn.transaction()?;
        let has_expired: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM records INDEXED BY records_time WHERE accepted_at_ms<?1)",
            [cutoff],
            |row| row.get(0),
        )?;
        let rows = if has_expired {
            tx.prepare("SELECT source_id,record_id,sequence,cursor,raw_json FROM records WHERE accepted_at_ms<?1 AND cursor NOT IN (SELECT cursor FROM current_state) ORDER BY cursor LIMIT 1000")?
                .query_map([cutoff], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, u64>(2)?,
                        r.get::<_, u64>(3)?,
                        r.get::<_, String>(4)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        let more_records = rows.len() == 1000;
        if let Some(max) = rows.last().map(|row| row.3) {
            for (source, id, sequence, cursor, raw) in rows {
                let sha = format!("{:x}", Sha256::digest(raw.as_bytes()));
                tx.execute("INSERT OR IGNORE INTO receipts(source_id,record_id,sequence,cursor,raw_sha,pruned_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
                    params![source,id,sequence,cursor,sha,now_ms])?;
            }
            tx.execute(
                "DELETE FROM records WHERE accepted_at_ms<?1 AND cursor<=?2 AND cursor NOT IN (SELECT cursor FROM current_state)",
                params![cutoff, max],
            )?;
            tx.execute(
                "UPDATE meta SET value=MAX(value,?1) WHERE key='pruned_through'",
                [max],
            )?;
        }
        let more_receipts = if self.tombstone_ms > 0 {
            let horizon = now_ms.saturating_sub(self.tombstone_ms);
            tx.execute(
                "DELETE FROM receipts WHERE rowid IN (SELECT rowid FROM receipts WHERE pruned_at_ms<?1 LIMIT 1000)",
                [horizon],
            )? == 1000
        } else {
            false
        };
        let command_horizon = now_ms.saturating_sub(COMMAND_RETENTION_MS);
        let more_commands = tx.execute(
            "DELETE FROM commands WHERE id IN (
                 SELECT id FROM commands
                 WHERE status IN ('done','cancelled') AND completed_at_ms<?1
                 ORDER BY completed_at_ms,id LIMIT 1000
             )",
            [command_horizon],
        )? == 1000;
        tx.commit()?;
        drop(conn);
        let orphans = self.orphan_blobs(cutoff)?;
        let more_blobs = orphans.len() == 1000;
        self.delete_orphan_blobs(&orphans, cutoff)?;
        Ok(more_records || more_receipts || more_blobs || more_commands)
    }

    fn orphan_blobs(&self, cutoff: u64) -> Result<Vec<String>, StoreError> {
        let conn = self.db.lock().unwrap();
        Ok(conn
            .prepare("SELECT sha256 FROM blobs WHERE created_at_ms<?1 AND sha256 NOT IN (SELECT blob_sha FROM records WHERE blob_sha IS NOT NULL) LIMIT 1000")?
            .query_map([cutoff], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// Aday seçimi hak vermez: referans ve upload yaşı silme kilidi altında yeniden okunur.
    fn delete_orphan_blobs(&self, candidates: &[String], cutoff: u64) -> Result<(), StoreError> {
        for chunk in candidates.chunks(BLOB_GC_CHUNK) {
            let mut conn = self.db.lock().unwrap();
            let tx = conn.transaction()?;
            for sha in chunk {
                let orphan = tx.prepare_cached("SELECT 1 FROM blobs WHERE sha256=?1 AND created_at_ms<?2 AND NOT EXISTS (SELECT 1 FROM records WHERE blob_sha=?1)")?
                    .exists(params![sha, cutoff])?;
                if !orphan {
                    continue;
                }
                // ingest/put_blob/backup bu kontrol ile dosya silme arasına giremez.
                match fs::remove_file(self.blob_path(sha)) {
                    Ok(()) => sync_dir(self.blob_path(sha).parent().unwrap())?,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
                tx.execute("DELETE FROM blobs WHERE sha256=?1", [sha])?;
            }
            tx.commit()?;
        }
        Ok(())
    }

    pub fn issue_command(
        &self,
        by: &str,
        req: &CommandRequest,
        now_ms: u64,
    ) -> Result<Option<Command>, StoreError> {
        if !v4::valid_id(&req.source_id)
            || !v4::valid_id(&req.entity_id)
            || !v4::valid_id(&req.action)
        {
            return Err(StoreError::Invalid(Reject::InvalidId));
        }
        let args = serde_json::to_string(&req.args)?;
        if args.len() > 4096 {
            return Err(StoreError::Invalid(Reject::Payload));
        }
        let conn = self.db.lock().unwrap();
        let control: Option<Option<String>> = conn
            .query_row(
                "SELECT CASE WHEN ordinal IS NOT NULL THEN control_json END FROM entities WHERE source_id=?1 AND entity_id=?2",
                params![req.source_id, req.entity_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(control) = control else {
            return Ok(None);
        };
        let control = control.and_then(|s| serde_json::from_str::<geri_proto::Control>(&s).ok());
        let Some(normalized) =
            geri_core::v4::validate_command(control.as_ref(), &req.action, &req.args)
        else {
            return Err(StoreError::Invalid(Reject::Payload));
        };
        let args = serde_json::to_string(&normalized)?;
        conn.execute("INSERT INTO commands(source_id,entity_id,action,args_json,issued_by,issued_at_ms) VALUES(?1,?2,?3,?4,?5,?6)", params![req.source_id,req.entity_id,req.action,args,by,now_ms])?;
        Ok(Some(Command {
            command_id: format!("cmd-{}", conn.last_insert_rowid()),
            source_id: req.source_id.clone(),
            entity_id: req.entity_id.clone(),
            action: req.action.clone(),
            args: normalized,
            issued_by: by.into(),
            issued_at_ms: now_ms,
        }))
    }

    pub fn queued_among<'a>(
        &self,
        sources: impl IntoIterator<Item = &'a String>,
    ) -> Result<Vec<String>, StoreError> {
        let conn = self.db.lock().unwrap();
        let mut stmt = conn.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE source_id=?1 AND status='queued')",
        )?;
        let mut queued = Vec::new();
        for source in sources {
            if stmt.query_row([source], |r| r.get::<_, bool>(0))? {
                queued.push(source.clone());
            }
        }
        Ok(queued)
    }

    pub fn accept_counter(&self, agent: &str, n: u64) -> Result<Result<(), u64>, StoreError> {
        let conn = self.db.lock().unwrap();
        let mut window = conn
            .query_row(
                "SELECT max, seen FROM agent_counters WHERE agent_id=?1",
                [agent],
                |r| {
                    Ok(geri_sign::Window {
                        max: r.get::<_, i64>(0)? as u64,
                        seen: r.get::<_, i64>(1)? as u64,
                    })
                },
            )
            .optional()?
            .unwrap_or_default();
        if !window.accept(n) {
            return Ok(Err(window.max));
        }
        conn.execute(
            "INSERT INTO agent_counters(agent_id, max, seen) VALUES(?1, ?2, ?3)
             ON CONFLICT(agent_id) DO UPDATE SET max=excluded.max, seen=excluded.seen",
            params![agent, window.max as i64, window.seen as i64],
        )?;
        Ok(Ok(()))
    }

    pub fn claim_command(&self, source: &str, now_ms: u64) -> Result<Option<Command>, StoreError> {
        let mut conn = self.db.lock().unwrap();
        let tx = conn.transaction()?;
        loop {
            let row = tx.query_row(
                "SELECT c.id,c.entity_id,c.action,c.args_json,c.issued_by,c.issued_at_ms,e.control_json FROM commands c LEFT JOIN entities e ON e.source_id=c.source_id AND e.entity_id=c.entity_id AND e.ordinal IS NOT NULL WHERE c.source_id=?1 AND c.status='queued' ORDER BY c.id LIMIT 1",
                [source], |r| Ok(PendingCommand {
                    id: r.get(0)?, entity_id: r.get(1)?, action: r.get(2)?, raw_args: r.get(3)?,
                    issued_by: r.get(4)?, issued_at_ms: r.get(5)?, control_json: r.get(6)?,
                })
            ).optional()?;
            let Some(PendingCommand {
                id,
                entity_id,
                action,
                raw_args,
                issued_by,
                issued_at_ms,
                control_json,
            }) = row
            else {
                tx.commit()?;
                return Ok(None);
            };
            let control =
                control_json.and_then(|s| serde_json::from_str::<geri_proto::Control>(&s).ok());
            let args = serde_json::from_str::<serde_json::Value>(&raw_args).ok();
            // Eski kuyruğu da doğrula; dispatch anında argümanları yeniden normalize etme.
            if !args.as_ref().is_some_and(|args| {
                geri_core::v4::validate_command(control.as_ref(), &action, args).as_ref()
                    == Some(args)
            }) {
                tx.execute(
                    "UPDATE commands SET status='cancelled',completed_at_ms=?2 WHERE id=?1",
                    params![id, now_ms],
                )?;
                continue;
            }
            tx.execute(
                "UPDATE commands SET status='dispatched',dispatched_at_ms=?2 WHERE id=?1",
                params![id, now_ms],
            )?;
            tx.commit()?;
            return Ok(Some(Command {
                command_id: format!("cmd-{id}"),
                source_id: source.into(),
                entity_id,
                action,
                args: args.unwrap(),
                issued_by,
                issued_at_ms,
            }));
        }
    }

    pub fn finish_command(&self, source: &str, result: &CommandResult) -> Result<bool, StoreError> {
        let Some(id) = result
            .command_id
            .strip_prefix("cmd-")
            .and_then(|s| s.parse::<i64>().ok())
        else {
            return Ok(false);
        };
        let conn = self.db.lock().unwrap();
        let json = serde_json::to_string(result)?;
        let changed = conn.execute("UPDATE commands SET status='done',result_json=?3,completed_at_ms=?4 WHERE id=?1 AND source_id=?2 AND status='dispatched'", params![id,source,json,unix_ms()])?;
        if changed == 1 {
            return Ok(true);
        }
        let old: Option<String> = conn
            .query_row(
                "SELECT result_json FROM commands WHERE id=?1 AND source_id=?2 AND status='done'",
                params![id, source],
                |r| r.get(0),
            )
            .optional()?;
        Ok(old.as_deref() == Some(&json))
    }

    pub fn command_audit(
        &self,
        source: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CommandAudit>, StoreError> {
        audit_rows(&self.db.lock().unwrap(), source, limit)
    }

    pub fn backup(&self, destination: &Path) -> Result<(), StoreError> {
        self.backup_with(destination, || {})
    }

    // Test yalnız zamanlamayı durdurur; snapshot/kopyalama yolu üretimle aynıdır.
    fn backup_with(
        &self,
        destination: &Path,
        after_snapshot: impl FnOnce(),
    ) -> Result<(), StoreError> {
        fs::create_dir(destination)?;
        fs::create_dir(destination.join("blobs"))?;
        let db_path = destination.join("db.sqlite");
        // Snapshot ve bütün blob kopyaları bitene kadar GC/ingest/upload bekler.
        let conn = self.db.lock().unwrap();
        conn.backup(rusqlite::MAIN_DB, &db_path, None)?;
        let blobs = backup_blobs(&conn)?;
        after_snapshot();
        for blob in &blobs {
            let to = destination
                .join("blobs")
                .join(&blob.sha256[..2])
                .join(&blob.sha256);
            fs::create_dir_all(to.parent().unwrap())?;
            fs::copy(self.blob_path(&blob.sha256), &to)?;
            if hash_file(&to)? != (blob.size, blob.sha256.clone()) {
                return Err(backup_invalid("blob hash or size mismatch"));
            }
            sync_file(&to)?;
            sync_dir(to.parent().unwrap())?;
        }
        sync_file(&db_path)?;
        sync_dir(&destination.join("blobs"))?;
        let manifest = BackupManifest {
            version: 1,
            db_sha256: hash_file(&db_path)?.1,
            blobs,
        };
        // Manifest tamamlanma işaretidir; kopyalar dayanıklı olmadan görünür olmaz.
        let temp = destination.join("manifest.tmp");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec(&manifest)?)?;
        file.sync_all()?;
        drop(file);
        fs::rename(temp, destination.join("manifest.json"))?;
        sync_dir(destination)?;
        if let Some(parent) = destination.parent().filter(|p| !p.as_os_str().is_empty()) {
            sync_dir(parent)?;
        }
        drop(conn);
        Ok(())
    }

    /// Restore öncesinde tamamlanma işaretini, SQLite snapshot'ını ve tüm blob'ları doğrular.
    pub fn verify_backup(destination: &Path) -> Result<(), StoreError> {
        let manifest: BackupManifest =
            serde_json::from_slice(&fs::read(destination.join("manifest.json"))?)?;
        if manifest.version != 1
            || hash_file(&destination.join("db.sqlite"))?.1 != manifest.db_sha256
        {
            return Err(backup_invalid("snapshot hash or manifest version mismatch"));
        }
        let conn = Connection::open_with_flags(
            destination.join("db.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let expected = backup_blobs(&conn)?;
        if expected != manifest.blobs {
            return Err(backup_invalid(
                "snapshot blob references differ from manifest",
            ));
        }
        for blob in expected {
            let path = destination
                .join("blobs")
                .join(&blob.sha256[..2])
                .join(&blob.sha256);
            if hash_file(&path)? != (blob.size, blob.sha256) {
                return Err(backup_invalid("backup blob hash or size mismatch"));
            }
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BackupManifest {
    version: u32,
    db_sha256: String,
    blobs: Vec<BlobRef>,
}

fn backup_invalid(message: &'static str) -> StoreError {
    StoreError::Io(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn backup_blobs(conn: &Connection) -> Result<Vec<BlobRef>, StoreError> {
    let mut blobs = BTreeMap::new();
    let mut statement =
        conn.prepare("SELECT blob_sha,raw_json FROM records WHERE blob_sha IS NOT NULL")?;
    let rows = statement.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (sha, raw) = row?;
        let record: Record = serde_json::from_str(&raw)?;
        geri_core::v4::validate_record(&record).map_err(StoreError::Invalid)?;
        let Some(blob) = record.blob else {
            return Err(backup_invalid("missing record blob reference"));
        };
        if sha != blob.sha256
            || blobs
                .insert(sha, blob.clone())
                .is_some_and(|previous| previous != blob)
        {
            return Err(backup_invalid("inconsistent blob references"));
        }
    }
    Ok(blobs.into_values().collect())
}

fn hash_file(path: &Path) -> Result<(u64, String), StoreError> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut size = 0u64;
    let mut buffer = [0; 16 * 1024];
    loop {
        let len = file.read(&mut buffer)?;
        if len == 0 {
            break;
        }
        hash.update(&buffer[..len]);
        size += len as u64;
    }
    Ok((size, format!("{:x}", hash.finalize())))
}

fn sync_file(path: &Path) -> Result<(), StoreError> {
    fs::OpenOptions::new().write(true).open(path)?.sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), StoreError> {
    #[cfg(not(windows))]
    fs::File::open(path)?.sync_all()?;
    #[cfg(windows)]
    let _ = path; // Windows'ta standart File API'si dizin FlushFileBuffers desteklemez.
    Ok(())
}

fn parse_visibility(v: &str) -> Visibility {
    if v == "public" {
        Visibility::Public
    } else {
        Visibility::Private
    }
}

fn state_rows(conn: &Connection, source: &str) -> Result<Vec<StoredRecord>, StoreError> {
    conn.prepare_cached("SELECT r.raw_json,r.cursor,r.accepted_at_ms FROM current_state s JOIN records r ON r.cursor=s.cursor WHERE s.source_id=?1 ORDER BY s.entity_id")?
        .query_map([source], parse_stored)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(StoreError::Db)
}

fn audit_rows(
    conn: &Connection,
    source: Option<&str>,
    limit: usize,
) -> Result<Vec<CommandAudit>, StoreError> {
    let query = if source.is_some() {
        "SELECT id,source_id,entity_id,action,args_json,issued_by,issued_at_ms,status,dispatched_at_ms,result_json FROM commands WHERE source_id=?1 ORDER BY id DESC LIMIT ?2"
    } else {
        "SELECT id,source_id,entity_id,action,args_json,issued_by,issued_at_ms,status,dispatched_at_ms,result_json FROM commands ORDER BY id DESC LIMIT ?1"
    };
    let mut stmt = conn.prepare_cached(query)?;
    let rows = match source {
        Some(source) => stmt.query_map(params![source, limit.clamp(1, 500)], command_audit_row)?,
        None => stmt.query_map([limit.clamp(1, 500)], command_audit_row)?,
    };
    rows.collect::<Result<Vec<_>, _>>().map_err(StoreError::Db)
}

fn command_audit_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CommandAudit> {
    let args: String = row.get(4)?;
    let result: Option<String> = row.get(9)?;
    let parsed = |raw: String| serde_json::from_str(&raw).unwrap_or(serde_json::Value::String(raw));
    Ok(CommandAudit {
        command: Command {
            command_id: format!("cmd-{}", row.get::<_, i64>(0)?),
            source_id: row.get(1)?,
            entity_id: row.get(2)?,
            action: row.get(3)?,
            args: parsed(args),
            issued_by: row.get(5)?,
            issued_at_ms: row.get(6)?,
        },
        status: CommandStatus::from_text(&row.get::<_, String>(7)?),
        dispatched_at_ms: row.get(8)?,
        result: result.map(parsed),
    })
}

fn kind(k: RecordKind) -> &'static str {
    match k {
        RecordKind::State => "state",
        RecordKind::Event => "event",
        RecordKind::Raw => "raw",
    }
}
fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn parse_stored(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredRecord> {
    let raw: String = row.get(0)?;
    let record: Record = serde_json::from_str(&raw).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(StoredRecord {
        record,
        cursor: row.get(1)?,
        accepted_at_ms: row.get(2)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn policy() -> AgentPolicy {
        AgentPolicy {
            agent_id: "router-agent".into(),
            writable_sources: ["router".into()].into(),
            role: Default::default(),
        }
    }
    fn batch(seq: u64) -> Batch {
        Batch {
            v: 4,
            records: vec![Record {
                source_id: "router".into(),
                entity_id: "cpu".into(),
                record_id: format!("r-{seq}"),
                sequence: seq,
                kind: RecordKind::State,
                event_time_ms: None,
                content_type: "application/json".into(),
                data: Some(serde_json::json!({"pct": seq})),
                indexed: None,
                blob: None,
            }],
        }
    }

    #[test]
    fn hubd_accepts_last_exact_sequence_and_rejects_the_next_integer() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        let max = geri_proto::v4::MAX_SEQUENCE;
        let receipt = store.ingest(&policy(), &batch(max), 1).unwrap();
        assert_eq!(receipt.records[0].status, ReceiptStatus::Accepted);
        assert!(matches!(
            store.ingest(&policy(), &batch(max + 1), 2),
            Err(StoreError::Invalid(Reject::Sequence))
        ));
        let last: u64 = store
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT last_sequence FROM sources WHERE source_id='router'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last, max);
    }

    #[test]
    fn source_declaration_reports_sequence_exhaustion_without_committing_changes() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        let declaration = SourceDeclaration {
            source_id: "router".into(),
            label: "original".into(),
            kind: "test".into(),
            entities: vec![],
        };
        store.upsert_source(&policy(), &declaration, 1).unwrap();
        store
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE sources SET last_sequence=?1 WHERE source_id='router'",
                [geri_proto::v4::MAX_SEQUENCE],
            )
            .unwrap();
        let mut changed = declaration;
        changed.label = "should-roll-back".into();
        assert!(matches!(
            store.upsert_source(&policy(), &changed, 2),
            Err(StoreError::SequenceExhausted)
        ));
        let label: String = store
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT label FROM sources WHERE source_id='router'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(label, "original");
    }

    fn control_declaration() -> SourceDeclaration {
        SourceDeclaration {
            source_id: "router".into(),
            label: "Router".into(),
            kind: "test".into(),
            entities: vec![
                v4::Entity::new("router", "reboot")
                    .control(geri_proto::Shape::Trigger, geri_proto::Danger::Destructive),
            ],
        }
    }

    fn press() -> CommandRequest {
        CommandRequest {
            source_id: "router".into(),
            entity_id: "reboot".into(),
            action: "press".into(),
            args: serde_json::Value::Null,
        }
    }

    #[test]
    fn removed_control_cancels_queue_preserves_history_and_never_revives() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        let blobs = dir.path().join("b");
        let store = Store::open(&path, &blobs).unwrap();
        let declaration = control_declaration();
        store.upsert_source(&policy(), &declaration, 1).unwrap();
        let cmd = store
            .issue_command("operator", &press(), 2)
            .unwrap()
            .unwrap();
        let mut removed = declaration.clone();
        removed.entities.clear();
        store.upsert_source(&policy(), &removed, 3).unwrap();
        assert!(matches!(
            store.issue_command("operator", &press(), 4),
            Err(StoreError::Invalid(_))
        ));
        assert!(store.claim_command("router", 4).unwrap().is_none());
        let audit = store.command_audit(Some("router"), 10).unwrap();
        assert_eq!(audit[0].status, CommandStatus::Cancelled);
        assert_eq!(audit[0].command, cmd);
        assert_eq!(audit[0].dispatched_at_ms, None);
        assert_eq!(audit[0].result, None);
        let conn = store.db.lock().unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM entities WHERE entity_id='reboot' AND control_json IS NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        drop(conn);
        drop(store);
        let store = Store::open(&path, &blobs).unwrap();
        store.upsert_source(&policy(), &declaration, 5).unwrap();
        assert!(store.claim_command("router", 6).unwrap().is_none());
        assert!(
            !store
                .finish_command(
                    "router",
                    &CommandResult {
                        command_id: cmd.command_id,
                        ok: true,
                        result: serde_json::Value::Null
                    }
                )
                .unwrap()
        );
    }

    #[test]
    fn same_control_redeclaration_preserves_queue_and_dispatched_results() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        let mut declaration = control_declaration();
        store.upsert_source(&policy(), &declaration, 1).unwrap();
        let cmd = store
            .issue_command("operator", &press(), 2)
            .unwrap()
            .unwrap();
        declaration.entities[0].label = "Yeni etiket".into();
        declaration.entities[0].group = Some("system".into());
        store.upsert_source(&policy(), &declaration, 3).unwrap();
        assert_eq!(store.claim_command("router", 4).unwrap(), Some(cmd.clone()));
        declaration.entities.clear();
        store.upsert_source(&policy(), &declaration, 5).unwrap();
        let result = CommandResult {
            command_id: cmd.command_id,
            ok: true,
            result: serde_json::json!({"rebooted": true}),
        };
        assert!(store.finish_command("router", &result).unwrap());
        assert!(store.finish_command("router", &result).unwrap());
        assert!(store.claim_command("router", 6).unwrap().is_none());
        assert_eq!(
            store.command_audit(None, 10).unwrap()[0].status,
            CommandStatus::Done
        );
    }

    #[test]
    fn control_definition_changes_cancel_only_affected_queued_commands() {
        for change in 0..3 {
            let dir = tempdir().unwrap();
            let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
            let mut declaration = control_declaration();
            let mut other = declaration.entities[0].clone();
            other.entity_id = "other".into();
            declaration.entities.push(other);
            store.upsert_source(&policy(), &declaration, 1).unwrap();
            store.issue_command("operator", &press(), 2).unwrap();
            let mut request = press();
            request.entity_id = "other".into();
            let kept = store
                .issue_command("operator", &request, 2)
                .unwrap()
                .unwrap();
            match change {
                0 => declaration.entities[0].control = None,
                1 => {
                    declaration.entities[0].control.as_mut().unwrap().danger =
                        geri_proto::Danger::Safe
                }
                _ => {
                    declaration.entities[0].control.as_mut().unwrap().shape =
                        geri_proto::Shape::Bool
                }
            }
            store.upsert_source(&policy(), &declaration, 3).unwrap();
            assert_eq!(store.claim_command("router", 4).unwrap(), Some(kept));
            assert!(store.claim_command("router", 5).unwrap().is_none());
            assert_eq!(
                store.command_audit(None, 10).unwrap()[1].status,
                CommandStatus::Cancelled
            );
        }
    }

    #[test]
    fn claim_rechecks_legacy_control_action_and_args_without_blocking_valid_queue() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        store
            .upsert_source(&policy(), &control_declaration(), 1)
            .unwrap();
        {
            let conn = store.db.lock().unwrap();
            conn.execute("INSERT INTO entities(source_id,entity_id,label,kind,control_json) VALUES('router','removed','old','t',?1)", [serde_json::to_string(&control_declaration().entities[0].control).unwrap()]).unwrap();
            for (entity, action, args) in [
                ("removed", "press", "null"),
                ("missing", "press", "null"),
                ("reboot", "set", "true"),
                ("reboot", "press", "true"),
                ("reboot", "press", "broken"),
            ] {
                conn.execute("INSERT INTO commands(source_id,entity_id,action,args_json,issued_by,issued_at_ms) VALUES('router',?1,?2,?3,'operator',1)", params![entity, action, args]).unwrap();
            }
        }
        let valid = store
            .issue_command("operator", &press(), 2)
            .unwrap()
            .unwrap();
        assert_eq!(store.claim_command("router", 3).unwrap(), Some(valid));
        let audits = store.command_audit(None, 10).unwrap();
        assert!(
            audits[1..]
                .iter()
                .all(|a| a.status == CommandStatus::Cancelled)
        );
        assert!(store.claim_command("router", 4).unwrap().is_none());
    }

    #[test]
    fn failed_declaration_rolls_back_revocation_and_cancellation() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        let mut declaration = control_declaration();
        store.upsert_source(&policy(), &declaration, 1).unwrap();
        let cmd = store
            .issue_command("operator", &press(), 2)
            .unwrap()
            .unwrap();
        store.db.lock().unwrap().execute_batch("CREATE TRIGGER fail_update BEFORE UPDATE OF label ON entities BEGIN SELECT RAISE(ABORT,'test failure'); END").unwrap();
        declaration.entities[0].control.as_mut().unwrap().danger = geri_proto::Danger::Safe;
        assert!(store.upsert_source(&policy(), &declaration, 3).is_err());
        assert_eq!(store.claim_command("router", 4).unwrap(), Some(cmd));
    }

    fn unordered(id: &str, time: Option<u64>, pct: u64) -> Batch {
        let mut b = batch(0);
        b.records[0].record_id = id.into();
        b.records[0].event_time_ms = time;
        b.records[0].data = Some(serde_json::json!({"pct": pct}));
        b
    }

    fn status(store: &Store, b: &Batch) -> ReceiptStatus {
        store.ingest(&policy(), b, 100).unwrap().records[0].status
    }

    fn current_pct(store: &Store) -> serde_json::Value {
        store.state("router").unwrap()[0]
            .record
            .data
            .clone()
            .unwrap()["pct"]
            .clone()
    }

    #[test]
    fn unordered_records_live_beside_the_sequence() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        assert_eq!(status(&store, &batch(1)), ReceiptStatus::Accepted);
        assert_eq!(
            status(&store, &unordered("u-a", Some(500), 10)),
            ReceiptStatus::Accepted
        );
        assert_eq!(
            status(&store, &unordered("u-b", Some(700), 20)),
            ReceiptStatus::Accepted
        );
        assert_eq!(status(&store, &batch(2)), ReceiptStatus::Accepted);
        assert_eq!(status(&store, &batch(4)), ReceiptStatus::Gap);
        assert_eq!(
            status(&store, &unordered("u-a", Some(500), 10)),
            ReceiptStatus::Duplicate
        );
        assert_eq!(
            status(&store, &unordered("u-a", Some(500), 11)),
            ReceiptStatus::Conflict
        );
        let mut stolen = unordered("r-1", None, 1);
        stolen.records[0].data = Some(serde_json::json!({"pct": 1}));
        assert_eq!(status(&store, &stolen), ReceiptStatus::Conflict);
        let declared = store
            .upsert_source(
                &policy(),
                &SourceDeclaration {
                    source_id: "router".into(),
                    label: "R".into(),
                    kind: "t".into(),
                    entities: vec![],
                },
                100,
            )
            .unwrap();
        assert_eq!(declared, 3, "sıra sırasızlardan etkilenmedi");
        assert_eq!(
            store
                .records(None, 0, None, None, None, 100)
                .unwrap()
                .records
                .len(),
            4
        );
    }

    #[test]
    fn a_late_unordered_state_does_not_become_current() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("b")).unwrap();
        status(&store, &unordered("u-new", Some(700), 20));
        assert_eq!(current_pct(&store), 20);
        assert_eq!(
            status(&store, &unordered("u-old", Some(500), 10)),
            ReceiptStatus::Accepted
        );
        assert_eq!(current_pct(&store), 20);
        assert_eq!(
            status(&store, &unordered("u-newer", Some(900), 30)),
            ReceiptStatus::Accepted
        );
        assert_eq!(current_pct(&store), 30);
        status(&store, &unordered("u-timeless", None, 40));
        assert_eq!(current_pct(&store), 40);
        status(&store, &batch(1));
        assert_eq!(current_pct(&store), 1);
    }

    #[test]
    fn an_old_database_is_rebuilt_without_losing_cursors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("old.sqlite");
        {
            let old = SCHEMA.replace(
                "UNIQUE(source_id, record_id)\n",
                "UNIQUE(source_id, record_id), UNIQUE(source_id, sequence)\n",
            );
            assert_ne!(old, SCHEMA);
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(&old).unwrap();
            conn.execute("INSERT INTO sources(source_id,label,kind,created_at_ms,last_sequence) VALUES('router','r','t',1,4)", []).unwrap();
            for seq in 1..=4u64 {
                let r = &batch(seq).records[0];
                conn.execute("INSERT INTO records(source_id,entity_id,record_id,sequence,kind,accepted_at_ms,content_type,raw_json,search_text) VALUES('router','cpu',?1,?2,'state',1,'application/json',?3,'')",
                    params![r.record_id, seq, serde_json::to_string(r).unwrap()]).unwrap();
            }
            conn.execute("DELETE FROM records WHERE sequence=4", [])
                .unwrap();
        }

        let store = Store::open(&path, &dir.path().join("b")).unwrap();
        let ddl: String = store
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name='records'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!ddl.contains("UNIQUE(source_id, sequence)"), "{ddl}");
        let cursors = |store: &Store| -> Vec<u64> {
            let page = store.records(None, 0, None, None, None, 100).unwrap();
            page.records.iter().map(|r| r.cursor).collect()
        };
        assert_eq!(cursors(&store), [1, 2, 3]);
        assert_eq!(status(&store, &batch(5)), ReceiptStatus::Accepted);
        assert_eq!(cursors(&store), [1, 2, 3, 5]);
        let dup = store.db.lock().unwrap().execute(
            "INSERT INTO records(source_id,entity_id,record_id,sequence,kind,accepted_at_ms,content_type,raw_json,search_text) VALUES('router','cpu','dup',5,'state',1,'a','{}','')",
            [],
        );
        assert!(dup.is_err());
        assert_eq!(
            status(&store, &unordered("u-1", None, 1)),
            ReceiptStatus::Accepted
        );
        assert_eq!(
            status(&store, &unordered("u-2", None, 2)),
            ReceiptStatus::Accepted
        );
        drop(store);
        let store = Store::open(&path, &dir.path().join("b")).unwrap();
        assert_eq!(cursors(&store), [1, 2, 3, 5, 6, 7]);
    }

    #[test]
    fn durable_ack_duplicate_gap_restart_and_retention() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        let blobs = dir.path().join("blobs");
        let store = Store::open(&path, &blobs).unwrap();
        let a = store.ingest(&policy(), &batch(1), 100).unwrap();
        assert_eq!(a.records[0].status, ReceiptStatus::Accepted);
        assert_eq!(
            store.ingest(&policy(), &batch(1), 101).unwrap().records[0].status,
            ReceiptStatus::Duplicate
        );
        assert_eq!(
            store.ingest(&policy(), &batch(3), 102).unwrap().records[0].expected_sequence,
            Some(2)
        );
        assert_eq!(store.state("router").unwrap().len(), 1);
        drop(store);
        let store = Store::open(&path, &blobs).unwrap();
        assert_eq!(
            store
                .records(Some("router"), 0, None, None, None, 10)
                .unwrap()
                .records
                .len(),
            1
        );
        assert_eq!(
            store.ingest(&policy(), &batch(2), 103).unwrap().records[0].status,
            ReceiptStatus::Accepted
        );
        let digest = format!("{:x}", Sha256::digest(b"binary"));
        let blob = store.put_blob(&digest, b"binary").unwrap();
        let blob_batch = Batch {
            v: 4,
            records: vec![Record {
                source_id: "router".into(),
                entity_id: "file".into(),
                record_id: "binary-1".into(),
                sequence: 3,
                kind: RecordKind::Raw,
                event_time_ms: None,
                content_type: "application/octet-stream".into(),
                data: None,
                indexed: Some(serde_json::json!({"title":"binary"})),
                blob: Some(blob),
            }],
        };
        store.ingest(&policy(), &blob_batch, 103).unwrap();
        assert_eq!(
            store
                .records(Some("router"), 0, None, None, Some("binary"), 10)
                .unwrap()
                .records
                .len(),
            1
        );
        let backup = dir.path().join("snapshot");
        store.backup(&backup).unwrap();
        let restored = Store::open(&backup.join("db.sqlite"), &backup.join("blobs")).unwrap();
        assert_eq!(
            restored
                .records(Some("router"), 0, None, None, None, 10)
                .unwrap()
                .records
                .len(),
            3
        );
        assert_eq!(
            restored.get_blob(&digest).unwrap(),
            Some(b"binary".to_vec())
        );
        store
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE blobs SET created_at_ms=100 WHERE sha256=?1",
                [&digest],
            )
            .unwrap();
        store.prune(v4::RETENTION_MS + 104).unwrap();
        assert!(matches!(
            store.records(None, 1, None, None, None, 10),
            Err(StoreError::Expired(_))
        ));
        assert!(store.get_blob(&digest).unwrap().is_none());
        assert_eq!(
            store.ingest(&policy(), &blob_batch, 105).unwrap().records[0].status,
            ReceiptStatus::Duplicate
        );
        assert_eq!(
            store.ingest(&policy(), &batch(1), 105).unwrap().records[0].status,
            ReceiptStatus::Duplicate
        );
        let mut changed = batch(1);
        changed.records[0].data = Some(serde_json::json!({"pct":99}));
        assert_eq!(
            store.ingest(&policy(), &changed, 105).unwrap().records[0].status,
            ReceiptStatus::Conflict
        );
    }

    #[test]
    fn failed_second_insert_rolls_back_entire_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        let store = Store::open(&path, &dir.path().join("blobs")).unwrap();
        store.db.lock().unwrap().execute_batch("CREATE TRIGGER fail_second BEFORE INSERT ON records WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT,'simulated failure'); END;").unwrap();
        let mut first = batch(1);
        let mut second = batch(2);
        let batch = Batch {
            v: 4,
            records: vec![first.records.remove(0), second.records.remove(0)],
        };
        assert!(matches!(
            store.ingest(&policy(), &batch, 100),
            Err(StoreError::Db(_))
        ));
        drop(store);
        let store = Store::open(&path, &dir.path().join("blobs")).unwrap();
        assert!(
            store
                .records(None, 0, None, None, None, 10)
                .unwrap()
                .records
                .is_empty()
        );
    }

    #[test]
    fn retention_works_in_bounded_batches() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("blobs")).unwrap();
        let records: Vec<Record> = (1..=1001).map(|n| batch(n).records.remove(0)).collect();
        for chunk in records.chunks(128) {
            store
                .ingest(
                    &policy(),
                    &Batch {
                        v: 4,
                        records: chunk.to_vec(),
                    },
                    100,
                )
                .unwrap();
        }
        assert!(store.prune(v4::RETENTION_MS + 101).unwrap());
        let remaining: u64 = store
            .db
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 1);
        let Err(StoreError::Expired(earliest)) = store.records(None, 999, None, None, None, 10)
        else {
            panic!("cursor inside the purged range must expire");
        };
        assert_eq!(earliest, 1001);
        let resumed = store
            .records(None, earliest - 1, None, None, None, 10)
            .unwrap();
        assert_eq!(resumed.records[0].cursor, earliest);
        assert!(!store.prune(v4::RETENTION_MS + 101).unwrap());
        assert_eq!(
            store.ingest(&policy(), &batch(1001), 102).unwrap().records[0].status,
            ReceiptStatus::Duplicate
        );
        assert_eq!(
            store.ingest(&policy(), &batch(1), 102).unwrap().records[0].status,
            ReceiptStatus::Duplicate
        );
    }

    #[test]
    fn command_history_expires_after_ninety_days_but_active_commands_remain() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("blobs")).unwrap();
        let now = COMMAND_RETENTION_MS + 10;
        {
            let conn = store.db.lock().unwrap();
            conn.execute_batch("INSERT INTO commands(source_id,entity_id,action,args_json,issued_by,issued_at_ms,status,completed_at_ms) VALUES
                ('router','reboot','press','null','operator',1,'done',1),
                ('router','reboot','press','null','operator',2,'cancelled',2),
                ('router','reboot','press','null','operator',3,'done',10),
                ('router','reboot','press','null','operator',4,'queued',NULL),
                ('router','reboot','press','null','operator',5,'dispatched',NULL);").unwrap();
        }
        store.prune(now).unwrap();
        let conn = store.db.lock().unwrap();
        let rows: Vec<(String, Option<i64>)> = conn
            .prepare("SELECT status,completed_at_ms FROM commands ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            [
                ("done".into(), Some(10)),
                ("queued".into(), None),
                ("dispatched".into(), None)
            ]
        );
    }

    #[test]
    fn old_command_rows_gain_a_completion_time_on_migration() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE commands(
                id INTEGER PRIMARY KEY, source_id TEXT NOT NULL, entity_id TEXT NOT NULL,
                action TEXT NOT NULL, args_json TEXT NOT NULL, issued_by TEXT NOT NULL,
                issued_at_ms INTEGER NOT NULL, status TEXT NOT NULL,
                dispatched_at_ms INTEGER, result_json TEXT
            );
            INSERT INTO commands VALUES(1,'router','reboot','press','null','operator',5,'done',NULL,NULL);
            INSERT INTO commands VALUES(2,'router','reboot','press','null','operator',3,'cancelled',7,NULL);").unwrap();
        }
        let store = Store::open(&path, &dir.path().join("blobs")).unwrap();
        let conn = store.db.lock().unwrap();
        let times: Vec<i64> = conn
            .prepare("SELECT completed_at_ms FROM commands ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(times, [5, 7]);
    }

    #[test]
    fn webhooks_do_not_take_the_sources_sequence() {
        let dir = tempdir().unwrap();
        let store = Store::memory(&dir.path().join("blobs")).unwrap();
        let hook = store
            .ingest_webhook(
                &policy(),
                "router",
                "alert",
                "ev-1",
                "application/json",
                b"{}",
                100,
            )
            .unwrap();
        assert_eq!(hook.status, ReceiptStatus::Accepted);
        let page = store
            .records(Some("router"), 0, None, None, None, 10)
            .unwrap();
        assert_eq!(page.records[0].record.sequence, 0);
        assert_eq!(status(&store, &batch(1)), ReceiptStatus::Accepted);
        let again = store
            .ingest_webhook(
                &policy(),
                "router",
                "alert",
                "ev-1",
                "application/json",
                b"{}",
                101,
            )
            .unwrap();
        assert_eq!(again.status, ReceiptStatus::Duplicate);
        assert_eq!(status(&store, &batch(2)), ReceiptStatus::Accepted);
    }

    #[test]
    fn hud_queries_read_through_indexes() {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db.sqlite"), &dir.path().join("blobs")).unwrap();
        let conn = store.db.lock().unwrap();
        let plan = |sql: &str| -> String {
            conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(["router"], |r| r.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .join(" | ")
        };
        let last = plan("SELECT COALESCE(MAX(accepted_at_ms),0) FROM records WHERE source_id=?1");
        assert!(last.contains("records_source_time"), "{last}");
        let events = plan(
            "SELECT raw_json,cursor,accepted_at_ms FROM records WHERE source_id=?1 AND kind IN ('event','raw') ORDER BY cursor DESC LIMIT 12",
        );
        assert!(events.contains("records_source_events"), "{events}");
        assert!(!events.contains("TEMP B-TREE"), "{events}");
    }

    #[test]
    fn current_state_outlives_retention_until_it_is_replaced() {
        let dir = tempdir().unwrap();
        let store = Store::memory(&dir.path().join("blobs")).unwrap();
        store.ingest(&policy(), &batch(1), 100).unwrap();
        store.prune(v4::RETENTION_MS + 200).unwrap();
        assert_eq!(store.state("router").unwrap().len(), 1, "son durum kalmalı");
        store
            .ingest(&policy(), &batch(2), v4::RETENTION_MS + 300)
            .unwrap();
        store.prune(v4::RETENTION_MS + 400).unwrap();
        let state = store.state("router").unwrap();
        assert_eq!(state[0].record.sequence, 2);
        let all = store.records(None, 0, None, None, None, 10).unwrap();
        assert_eq!(all.records.len(), 1);
        assert_eq!(status(&store, &batch(1)), ReceiptStatus::Duplicate);
    }

    #[test]
    fn tombstones_expire_after_their_window_unless_kept_forever() {
        for (window, kept) in [(1_000, false), (0, true)] {
            let dir = tempdir().unwrap();
            let store = Store::memory(&dir.path().join("blobs"))
                .unwrap()
                .with_tombstones(window);
            store.ingest(&policy(), &batch(1), 100).unwrap();
            store.ingest(&policy(), &batch(2), 100).unwrap();
            let now = v4::RETENTION_MS + 200;
            store.prune(now).unwrap();
            let count = |store: &Store| -> u64 {
                store
                    .db
                    .lock()
                    .unwrap()
                    .query_row("SELECT COUNT(*) FROM receipts", [], |r| r.get(0))
                    .unwrap()
            };
            assert_eq!(count(&store), 1);
            store.prune(now + 2_000).unwrap();
            assert_eq!(count(&store) == 1, kept, "window {window}");
        }
    }

    #[test]
    fn old_receipts_get_a_time_when_migrated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db.sqlite");
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch("CREATE TABLE receipts (source_id TEXT NOT NULL, record_id TEXT NOT NULL, sequence INTEGER NOT NULL, cursor INTEGER NOT NULL, raw_sha TEXT NOT NULL, PRIMARY KEY(source_id, record_id)); INSERT INTO receipts VALUES('router','r-1',1,1,'x');").unwrap();
        }
        let store = Store::open(&path, &dir.path().join("blobs")).unwrap();
        let at: Option<u64> = store
            .db
            .lock()
            .unwrap()
            .query_row("SELECT pruned_at_ms FROM receipts", [], |r| r.get(0))
            .unwrap();
        assert!(at.is_some_and(|t| t > 0));
    }
    fn blob_record(store: &Store) -> (String, Batch) {
        let sha = format!("{:x}", Sha256::digest(b"blob-data"));
        let blob = store.put_blob(&sha, b"blob-data").unwrap();
        let mut record = batch(1);
        record.records[0].kind = RecordKind::Event;
        record.records[0].data = None;
        record.records[0].content_type = "application/octet-stream".into();
        record.records[0].blob = Some(blob);
        (sha, record)
    }

    #[test]
    fn selected_orphan_cannot_be_deleted_after_an_accepted_reference() {
        let dir = tempdir().unwrap();
        let store = Store::memory(&dir.path().join("blobs")).unwrap();
        let (sha, record) = blob_record(&store);
        store
            .db
            .lock()
            .unwrap()
            .execute("UPDATE blobs SET created_at_ms=0", [])
            .unwrap();
        let candidates = store.orphan_blobs(1).unwrap();
        assert_eq!(candidates.as_slice(), std::slice::from_ref(&sha));
        assert_eq!(
            store.ingest(&policy(), &record, 1).unwrap().records[0].status,
            ReceiptStatus::Accepted
        );
        store.delete_orphan_blobs(&candidates, 1).unwrap();
        assert_eq!(store.get_blob(&sha).unwrap(), Some(b"blob-data".to_vec()));
    }

    #[test]
    fn a_reuploaded_orphan_keeps_its_new_grace_period() {
        let dir = tempdir().unwrap();
        let store = Store::memory(&dir.path().join("blobs")).unwrap();
        let (sha, _) = blob_record(&store);
        store
            .db
            .lock()
            .unwrap()
            .execute("UPDATE blobs SET created_at_ms=0", [])
            .unwrap();
        let candidates = store.orphan_blobs(1).unwrap();
        store.put_blob(&sha, b"blob-data").unwrap();
        store.delete_orphan_blobs(&candidates, 1).unwrap();
        assert_eq!(store.get_blob(&sha).unwrap(), Some(b"blob-data".to_vec()));
        assert!(store.orphan_blobs(1).unwrap().is_empty());
    }

    #[test]
    fn gc_io_failure_restarts_safely_after_a_partially_deleted_chunk() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db.sqlite");
        let blobs = dir.path().join("blobs");
        let store = Store::open(&db, &blobs).unwrap();
        let (sha, record) = blob_record(&store);
        let other = format!("{:x}", Sha256::digest(b"other"));
        store.put_blob(&other, b"other").unwrap();
        store
            .db
            .lock()
            .unwrap()
            .execute("UPDATE blobs SET created_at_ms=0", [])
            .unwrap();
        // İkinci delete bir dosya yerine dizin görerek hata üretir.
        let bad = store.blob_path(&other);
        fs::remove_file(&bad).unwrap();
        fs::create_dir(&bad).unwrap();
        assert!(
            store
                .delete_orphan_blobs(&[sha.clone(), other.clone()], 1)
                .is_err()
        );
        assert!(store.get_blob(&sha).unwrap().is_none());
        assert!(matches!(
            store.ingest(&policy(), &record, 1),
            Err(StoreError::MissingBlob)
        ));
        fs::remove_dir(&bad).unwrap();
        drop(store);
        let store = Store::open(&db, &blobs).unwrap();
        store
            .delete_orphan_blobs(&store.orphan_blobs(1).unwrap(), 1)
            .unwrap();
        assert!(store.orphan_blobs(1).unwrap().is_empty());
        store.put_blob(&sha, b"blob-data").unwrap();
        assert_eq!(
            store.ingest(&policy(), &record, 1).unwrap().records[0].status,
            ReceiptStatus::Accepted
        );
    }

    #[test]
    fn backup_fails_for_missing_or_corrupt_referenced_blobs_without_a_manifest() {
        let dir = tempdir().unwrap();
        let store = Store::memory(&dir.path().join("blobs")).unwrap();
        let (sha, record) = blob_record(&store);
        store.ingest(&policy(), &record, 1).unwrap();
        fs::remove_file(store.blob_path(&sha)).unwrap();
        let missing = dir.path().join("missing");
        assert!(store.backup(&missing).is_err());
        assert!(!missing.join("manifest.json").exists());
        assert!(Store::verify_backup(&missing).is_err());
        store.put_blob(&sha, b"blob-data").unwrap();
        fs::write(store.blob_path(&sha), b"bad--data").unwrap();
        let corrupt = dir.path().join("corrupt");
        assert!(store.backup(&corrupt).is_err());
        assert!(!corrupt.join("manifest.json").exists());
    }

    #[test]
    fn backup_pins_snapshot_blobs_until_their_verified_copy_is_complete() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;
        let dir = tempdir().unwrap();
        let store = Arc::new(Store::memory(&dir.path().join("blobs")).unwrap());
        let (sha, record) = blob_record(&store);
        store.ingest(&policy(), &record, 1).unwrap();
        store
            .db
            .lock()
            .unwrap()
            .execute("UPDATE blobs SET created_at_ms=0", [])
            .unwrap();
        let path = dir.path().join("snapshot");
        let (at_snapshot, snapshot_ready) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let copied = store.clone();
        let target = path.clone();
        let backup = std::thread::spawn(move || {
            copied.backup_with(&target, || {
                assert!(matches!(
                    copied.db.try_lock(),
                    Err(std::sync::TryLockError::WouldBlock)
                ));
                at_snapshot.send(()).unwrap();
                wait.recv().unwrap();
            })
        });
        snapshot_ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let gc_store = store.clone();
        let (done, completed) = mpsc::channel();
        let gc = std::thread::spawn(move || {
            let result = gc_store.prune(v4::RETENTION_MS + 2);
            done.send(()).unwrap();
            result
        });
        let blocked = completed.recv_timeout(Duration::from_millis(50)).is_err();
        release.send(()).unwrap();
        backup.join().unwrap().unwrap();
        gc.join().unwrap().unwrap();
        assert!(blocked, "GC kopyalama tamamlanmadan ilerlememeli");
        assert_eq!(store.get_blob(&sha).unwrap(), None);
        Store::verify_backup(&path).unwrap();
        let restored = Store::open(&path.join("db.sqlite"), &path.join("blobs")).unwrap();
        assert_eq!(
            restored.get_blob(&sha).unwrap(),
            Some(b"blob-data".to_vec())
        );
        assert_eq!(
            restored
                .records(None, 0, None, None, None, 10)
                .unwrap()
                .records[0]
                .record,
            record.records[0]
        );
    }

    #[test]
    fn restore_verification_rejects_tampered_database_and_blob_contents() {
        let dir = tempdir().unwrap();
        let store = Store::memory(&dir.path().join("blobs")).unwrap();
        let (sha, record) = blob_record(&store);
        store.ingest(&policy(), &record, 1).unwrap();
        let path = dir.path().join("snapshot");
        store.backup(&path).unwrap();
        Store::verify_backup(&path).unwrap();
        let blob = path.join("blobs").join(&sha[..2]).join(&sha);
        fs::write(&blob, b"bad--data").unwrap();
        assert!(Store::verify_backup(&path).is_err());
        fs::write(blob, b"blob-data").unwrap();
        let db = path.join("db.sqlite");
        let original = fs::read(&db).unwrap();
        let mut bad = original.clone();
        bad[0] ^= 1;
        fs::write(&db, bad).unwrap();
        assert!(Store::verify_backup(&path).is_err());
        fs::write(db, original).unwrap();
        Store::verify_backup(&path).unwrap();
        assert!(
            store.backup(&path).is_err(),
            "mevcut yedek üzerine yazılmaz"
        );
        Store::verify_backup(&path).unwrap();
    }
}
