// Testler Claude tarafından yazılmıştır.

use geri_adapter_sdk::{Draft, IdCollision, SequenceError, Store, renumber, shift_sequence};
use geri_proto::v4::Record;
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;

const SCHEMA_VERSION: i32 = 2;

pub use geri_adapter_sdk::DEFAULT_SEEN_WINDOW;

const WAL_AUTOCHECKPOINT_PAGES: i64 = 64;
const WAL_SIZE_LIMIT_BYTES: i64 = 256 * 1024;

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS source_seq(
        source_id TEXT PRIMARY KEY,
        seq INTEGER NOT NULL,
        synced INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS outbox(
        source_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        record_id TEXT NOT NULL,
        raw_json TEXT NOT NULL,
        automatic INTEGER NOT NULL DEFAULT 0,
        UNIQUE(source_id, record_id));
    CREATE TABLE IF NOT EXISTS outbox_blobs(
        sha256 TEXT PRIMARY KEY,
        bytes BLOB NOT NULL);
    CREATE TABLE IF NOT EXISTS seen(
        source_id TEXT NOT NULL,
        record_id TEXT NOT NULL,
        seq INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(source_id, record_id));";

const INDEXES: &str = "
    CREATE INDEX IF NOT EXISTS seen_by_seq ON seen(source_id, seq);
    CREATE UNIQUE INDEX IF NOT EXISTS outbox_seq ON outbox(source_id, seq) WHERE seq > 0;";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored record is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("outbox schema v{0} is newer than this SDK (v{SCHEMA_VERSION})")]
    NewerSchema(i32),
    #[error("cannot create the outbox directory: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    IdCollision(#[from] IdCollision),
    #[error(transparent)]
    Sequence(#[from] SequenceError),
}

#[derive(Debug)]
pub struct SqliteStore {
    db: Connection,
    seen_window: u64,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        Self::init(Connection::open(path)?)
    }

    pub fn memory() -> Result<Self, Error> {
        Self::init(Connection::open_in_memory()?)
    }

    pub fn with_seen_window(mut self, records: u64) -> Self {
        self.seen_window = records.max(1);
        self
    }

    fn init(mut db: Connection) -> Result<Self, Error> {
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        db.pragma_update(None, "wal_autocheckpoint", WAL_AUTOCHECKPOINT_PAGES)?;
        db.pragma_update(None, "journal_size_limit", WAL_SIZE_LIMIT_BYTES)?;
        let version: i32 = db.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > SCHEMA_VERSION {
            return Err(Error::NewerSchema(version));
        }
        let tx = db.transaction()?;
        tx.execute_batch(SCHEMA)?;
        if version < 2 {
            migrate_seen(&tx)?;
        }
        migrate_synced(&tx)?;
        migrate_unordered(&tx)?;
        migrate_automatic(&tx)?;
        tx.execute_batch(INDEXES)?;
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(Self {
            db,
            seen_window: DEFAULT_SEEN_WINDOW,
        })
    }
}

fn migrate_automatic(tx: &rusqlite::Transaction<'_>) -> Result<(), Error> {
    let has = tx
        .prepare("SELECT 1 FROM pragma_table_info('outbox') WHERE name='automatic'")?
        .exists([])?;
    if !has {
        tx.execute_batch("ALTER TABLE outbox ADD COLUMN automatic INTEGER NOT NULL DEFAULT 0;")?;
        tx.execute_batch("UPDATE outbox SET automatic=1 WHERE seq > 0
            AND record_id = source_id || '-' || CAST(seq AS TEXT)
            AND NOT EXISTS (SELECT 1 FROM seen WHERE seen.source_id=outbox.source_id AND seen.record_id=outbox.record_id);")?;
    }
    Ok(())
}

fn migrate_seen(tx: &rusqlite::Transaction<'_>) -> Result<(), Error> {
    let has_seq = tx
        .prepare("SELECT 1 FROM pragma_table_info('seen') WHERE name='seq'")?
        .exists([])?;
    if !has_seq {
        tx.execute_batch("ALTER TABLE seen ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;")?;
    }
    tx.execute(
        "DELETE FROM seen WHERE record_id = source_id || '-' ||
            CAST(CAST(substr(record_id, length(source_id) + 2) AS INTEGER) AS TEXT)",
        [],
    )?;
    tx.execute(
        "UPDATE seen SET seq = COALESCE(
            (SELECT seq FROM source_seq WHERE source_seq.source_id = seen.source_id), 0)",
        [],
    )?;
    Ok(())
}

fn migrate_synced(tx: &rusqlite::Transaction<'_>) -> Result<(), Error> {
    let has = tx
        .prepare("SELECT 1 FROM pragma_table_info('source_seq') WHERE name='synced'")?
        .exists([])?;
    if !has {
        tx.execute_batch(
            "ALTER TABLE source_seq ADD COLUMN synced INTEGER NOT NULL DEFAULT 0;
             UPDATE source_seq SET synced=1;",
        )?;
    }
    Ok(())
}

fn migrate_unordered(tx: &rusqlite::Transaction<'_>) -> Result<(), Error> {
    let old = tx
        .prepare("SELECT 1 FROM pragma_index_list('outbox') WHERE origin='pk'")?
        .exists([])?;
    if old {
        tx.execute_batch("ALTER TABLE outbox RENAME TO outbox_old;")?;
        tx.execute_batch(SCHEMA)?;
        tx.execute_batch(
            "INSERT INTO outbox(rowid, source_id, seq, record_id, raw_json, automatic)
                SELECT rowid, source_id, seq, record_id, raw_json,
                    seq > 0 AND record_id = source_id || '-' || CAST(seq AS TEXT)
                    AND NOT EXISTS (SELECT 1 FROM seen WHERE seen.source_id=outbox_old.source_id AND seen.record_id=outbox_old.record_id)
                FROM outbox_old;
             DROP TABLE outbox_old;",
        )?;
    }
    Ok(())
}

impl Store for SqliteStore {
    type Error = Error;

    fn append(&mut self, draft: Draft, blob: Option<&[u8]>) -> Result<Option<Record>, Error> {
        let tx = self.db.transaction()?;
        let last: u64 = tx
            .query_row(
                "SELECT seq FROM source_seq WHERE source_id=?1",
                [&draft.source_id],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if let Some(id) = &draft.record_id {
            let seen = tx
                .query_row(
                    "SELECT 1 FROM seen WHERE source_id=?1 AND record_id=?2 AND seq + ?3 > ?4
                     UNION ALL
                     SELECT 1 FROM outbox WHERE source_id=?1 AND record_id=?2",
                    params![draft.source_id, id, self.seen_window, last],
                    |_| Ok(()),
                )
                .optional()?;
            if seen.is_some() {
                return Ok(None);
            }
        }
        let remember = draft.record_id.is_some() && !draft.unordered;
        let automatic = draft.record_id.is_none();
        let record = draft.into_record(last)?;
        if automatic
            && tx
                .prepare(
                    "SELECT 1 FROM outbox WHERE source_id=?1 AND record_id=?2
            UNION ALL SELECT 1 FROM seen WHERE source_id=?1 AND record_id=?2 AND seq + ?3 > ?4",
                )?
                .exists(params![
                    record.source_id,
                    record.record_id,
                    self.seen_window,
                    last
                ])?
        {
            return Err(IdCollision(record.record_id).into());
        }
        tx.execute(
            "INSERT INTO outbox(source_id, seq, record_id, raw_json, automatic) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![
                record.source_id,
                record.sequence,
                record.record_id,
                serde_json::to_string(&record)?,
                automatic
            ],
        )?;
        if record.sequence > 0 {
            tx.execute(
                "INSERT INTO source_seq(source_id, seq) VALUES(?1, ?2)
                 ON CONFLICT(source_id) DO UPDATE SET seq=excluded.seq",
                params![record.source_id, record.sequence],
            )?;
        }
        if remember {
            tx.execute(
                "INSERT OR REPLACE INTO seen(source_id, record_id, seq) VALUES(?1, ?2, ?3)",
                params![record.source_id, record.record_id, record.sequence],
            )?;
            if record.sequence > self.seen_window {
                tx.execute(
                    "DELETE FROM seen WHERE source_id=?1 AND seq <= ?2",
                    params![record.source_id, record.sequence - self.seen_window],
                )?;
            }
        }
        if let (Some(r), Some(bytes)) = (&record.blob, blob) {
            tx.execute(
                "INSERT OR IGNORE INTO outbox_blobs(sha256, bytes) VALUES(?1, ?2)",
                params![r.sha256, bytes],
            )?;
        }
        tx.commit()?;
        Ok(Some(record))
    }

    fn peek(&mut self, max: usize) -> Result<Vec<Record>, Error> {
        let mut stmt = self
            .db
            .prepare_cached("SELECT raw_json FROM outbox ORDER BY rowid LIMIT ?1")?;
        let rows = stmt.query_map([max as i64], |r| r.get::<_, String>(0))?;
        rows.map(|raw| Ok(serde_json::from_str(&raw?)?)).collect()
    }

    fn remove(&mut self, source_id: &str, record_id: &str) -> Result<(), Error> {
        let tx = self.db.transaction()?;
        let blob: Option<Option<String>> = tx
            .query_row(
                "DELETE FROM outbox WHERE source_id=?1 AND record_id=?2
                 RETURNING json_extract(raw_json, '$.blob.sha256')",
                params![source_id, record_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(Some(sha)) = blob {
            tx.execute(
                "DELETE FROM outbox_blobs WHERE sha256=?1 AND NOT EXISTS (
                    SELECT 1 FROM outbox WHERE json_extract(raw_json, '$.blob.sha256')=?1)",
                [&sha],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn pending(&mut self) -> Result<usize, Error> {
        Ok(self
            .db
            .query_row("SELECT COUNT(*) FROM outbox", [], |r| r.get::<_, i64>(0))?
            as usize)
    }

    fn blob(&mut self, sha256: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self
            .db
            .query_row(
                "SELECT bytes FROM outbox_blobs WHERE sha256=?1",
                [sha256],
                |r| r.get(0),
            )
            .optional()?)
    }

    fn sync(&mut self, source_id: &str, next: Option<u64>) -> Result<Option<(u64, u64)>, Error> {
        if next.is_some_and(|n| n > geri_proto::v4::MAX_SEQUENCE) {
            return Err(SequenceError::OutOfRange.into());
        }
        let tx = self.db.transaction()?;
        let row: Option<(u64, bool)> = tx
            .query_row(
                "SELECT seq, synced FROM source_seq WHERE source_id=?1",
                [source_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if row.is_some_and(|(_, synced)| synced) {
            return Ok(None);
        }
        let last = row.map_or(0, |(seq, _)| seq);
        let first: u64 = tx.query_row(
            "SELECT COALESCE(MIN(seq), ?2 + 1) FROM outbox WHERE source_id=?1 AND seq > 0",
            params![source_id, last],
            |r| r.get(0),
        )?;
        let moved = match next.filter(|n| *n > 0 && *n != first) {
            None => None,
            Some(next) => {
                if next > geri_proto::v4::MAX_SEQUENCE || first > geri_proto::v4::MAX_SEQUENCE {
                    return Err(SequenceError::OutOfRange.into());
                }
                let delta = i64::try_from(i128::from(next) - i128::from(first))
                    .map_err(|_| SequenceError::ResyncRange)?;
                let shifted_last = shift_sequence(last, delta)?;
                let seen_sequences: Vec<u64> = tx
                    .prepare("SELECT seq FROM seen WHERE source_id=?1")?
                    .query_map([source_id], |r| r.get(0))?
                    .collect::<Result<_, _>>()?;
                for sequence in seen_sequences {
                    if sequence > 0 {
                        shift_sequence(sequence, delta)?;
                    }
                }
                let records: Vec<(i64, Record, bool)> = tx
                    .prepare("SELECT rowid, raw_json, automatic FROM outbox WHERE source_id=?1 AND seq > 0")?
                    .query_map([source_id], |r| Ok((r.get(0)?, r.get::<_, String>(1)?, r.get(2)?)))?
                    .map(|row| {
                        let (rowid, raw, automatic) = row?;
                        Ok((rowid, serde_json::from_str(&raw)?, automatic))
                    })
                    .collect::<Result<_, Error>>()?;
                tx.execute(
                    "DELETE FROM outbox WHERE source_id=?1 AND seq > 0",
                    [source_id],
                )?;
                for (rowid, record, automatic) in records {
                    let record = renumber(record, delta, automatic)?;
                    tx.execute(
                        "INSERT INTO outbox(rowid, source_id, seq, record_id, raw_json, automatic)
                         VALUES(?5, ?1, ?2, ?3, ?4, ?6)",
                        params![
                            record.source_id,
                            record.sequence,
                            record.record_id,
                            serde_json::to_string(&record)?,
                            rowid,
                            automatic
                        ],
                    )?;
                }
                tx.execute(
                    "UPDATE seen SET seq = seq + ?2 WHERE source_id=?1",
                    params![source_id, delta],
                )?;
                Some((first, next, shifted_last))
            }
        };
        tx.execute(
            "INSERT INTO source_seq(source_id, seq, synced) VALUES(?1, ?2, 1)
             ON CONFLICT(source_id) DO UPDATE SET seq=excluded.seq, synced=1",
            params![source_id, moved.map_or(last, |(_, _, last)| last)],
        )?;
        tx.commit()?;
        Ok(moved.map(|(from, to, _)| (from, to)))
    }
}

#[cfg(test)]
mod tests;
