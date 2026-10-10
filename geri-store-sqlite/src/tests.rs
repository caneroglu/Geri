use super::*;
use geri_adapter_sdk::MemStore;
use geri_proto::v4::{BlobRef, RecordKind};
use serde_json::json;
use tempfile::tempdir;

fn draft(source: &str, record_id: Option<&str>) -> Draft {
    Draft {
        source_id: source.into(),
        entity_id: "e".into(),
        record_id: record_id.map(Into::into),
        kind: RecordKind::Event,
        event_time_ms: Some(7),
        content_type: "application/json".into(),
        data: Some(json!({"x": 1, "none": null})),
        indexed: None,
        blob: None,
        unordered: false,
    }
}

fn unordered(source: &str, record_id: &str) -> Draft {
    Draft {
        unordered: true,
        ..draft(source, Some(record_id))
    }
}

fn blob_draft(record_id: &str, sha: &str) -> Draft {
    Draft {
        data: None,
        content_type: "application/xml".into(),
        indexed: Some(json!({"title": "t"})),
        blob: Some(BlobRef {
            sha256: sha.into(),
            size: 3,
        }),
        ..draft("feed", Some(record_id))
    }
}

fn contract<S: Store>(mut s: S)
where
    S::Error: core::fmt::Debug,
{
    let a1 = s.append(draft("b", None), None).unwrap().unwrap();
    let a2 = s.append(draft("b", Some("x")), None).unwrap().unwrap();
    let c1 = s.append(draft("a", None), None).unwrap().unwrap();
    assert_eq!((a1.sequence, a1.record_id.as_str()), (1, "b-1"));
    assert_eq!((a2.sequence, a2.record_id.as_str()), (2, "x"));
    assert_eq!(c1.sequence, 1, "sequences are per source");
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    assert_eq!(s.pending().unwrap(), 3);

    let peeked = s.peek(10).unwrap();
    assert_eq!(peeked, vec![a1.clone(), a2.clone(), c1.clone()]);
    assert_eq!(s.peek(1).unwrap(), vec![a1]);

    s.remove("b", "b-1").unwrap();
    s.remove("b", "b-1").unwrap();
    s.remove("b", "nope").unwrap();
    assert_eq!(s.pending().unwrap(), 2);
    s.remove("b", "x").unwrap();
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    assert_eq!(
        s.append(draft("b", None), None).unwrap().unwrap().sequence,
        3
    );

    let b1 = s
        .append(blob_draft("r1", "abc"), Some(b"xyz"))
        .unwrap()
        .unwrap();
    let b2 = s
        .append(blob_draft("r2", "abc"), Some(b"xyz"))
        .unwrap()
        .unwrap();
    assert_eq!(s.blob("abc").unwrap().as_deref(), Some(&b"xyz"[..]));
    s.remove("feed", &b1.record_id).unwrap();
    assert_eq!(s.blob("abc").unwrap().as_deref(), Some(&b"xyz"[..]));
    s.remove("feed", &b2.record_id).unwrap();
    assert_eq!(s.blob("abc").unwrap(), None);

    let before = s.append(draft("u", None), None).unwrap().unwrap();
    let u1 = s.append(unordered("u", "m1"), None).unwrap().unwrap();
    let u2 = s.append(unordered("u", "m2"), None).unwrap().unwrap();
    let after = s.append(draft("u", None), None).unwrap().unwrap();
    assert_eq!((u1.sequence, u1.record_id.as_str()), (0, "m1"));
    assert_eq!((before.sequence, after.sequence), (1, 2));
    assert_eq!(after.record_id, "u-2");
    assert_eq!(s.append(unordered("u", "m1"), None).unwrap(), None);
    assert_eq!(s.append(draft("u", Some("m1")), None).unwrap(), None);
    let peeked = s.peek(100).unwrap();
    assert_eq!(
        peeked[peeked.len() - 4..],
        [before.clone(), u1.clone(), u2.clone(), after.clone()]
    );
    s.remove("u", "m1").unwrap();
    assert!(s.peek(100).unwrap().contains(&u2));
    assert_eq!(
        s.append(unordered("u", "m1"), None)
            .unwrap()
            .unwrap()
            .sequence,
        0
    );
    s.remove("u", "u-1").unwrap();
    let named = s.append(draft("u", Some("n")), None).unwrap().unwrap();
    s.remove("u", &named.record_id).unwrap();
    assert_eq!(s.append(unordered("u", "n"), None).unwrap(), None);
}

#[test]
fn sqlite_keeps_the_store_contract() {
    contract(SqliteStore::memory().unwrap());
}

#[test]
fn mem_store_keeps_the_same_contract() {
    contract(MemStore::new());
}

#[test]
fn everything_survives_a_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("deep/dir/outbox.sqlite");
    let mut s = SqliteStore::open(&path).unwrap();
    let r = s.append(draft("feed", Some("e-1")), None).unwrap().unwrap();
    s.append(blob_draft("e-2", "abc"), Some(b"xyz")).unwrap();
    drop(s);
    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(s.pending().unwrap(), 2);
    assert_eq!(s.peek(1).unwrap(), vec![r]);
    assert_eq!(s.append(draft("feed", Some("e-1")), None).unwrap(), None);
    assert_eq!(
        s.append(draft("feed", None), None)
            .unwrap()
            .unwrap()
            .sequence,
        3
    );
    assert_eq!(s.blob("abc").unwrap().as_deref(), Some(&b"xyz"[..]));
}

const OLD_SCHEMA: &str = "
    CREATE TABLE source_seq(source_id TEXT PRIMARY KEY, seq INTEGER NOT NULL);
    CREATE TABLE outbox(source_id TEXT NOT NULL, seq INTEGER NOT NULL, record_id TEXT NOT NULL,
        raw_json TEXT NOT NULL, PRIMARY KEY(source_id, seq), UNIQUE(source_id, record_id));
    CREATE TABLE outbox_blobs(sha256 TEXT PRIMARY KEY, bytes BLOB NOT NULL);
    CREATE TABLE seen(source_id TEXT NOT NULL, record_id TEXT NOT NULL,
        PRIMARY KEY(source_id, record_id));";

#[test]
fn outboxes_of_the_old_sdk_are_migrated() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("old.sqlite");
    let old = Connection::open(&path).unwrap();
    old.execute_batch(OLD_SCHEMA).unwrap();
    let record = draft("router", None).into_record(40).unwrap();
    old.execute(
        "INSERT INTO outbox VALUES('router', 41, 'router-41', ?1)",
        [serde_json::to_string(&record).unwrap()],
    )
    .unwrap();
    old.execute_batch(
        "INSERT INTO source_seq VALUES('router', 41);
         INSERT INTO seen VALUES('router', 'router-40'), ('router', 'router-41'),
                                ('feed', 'entry-x'), ('feed', 'feed-007');",
    )
    .unwrap();
    drop(old);

    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(s.peek(10).unwrap(), vec![record]);
    assert_eq!(
        s.append(draft("router", None), None)
            .unwrap()
            .unwrap()
            .sequence,
        42
    );
    let seen: Vec<String> =
        s.db.prepare("SELECT record_id FROM seen ORDER BY record_id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    assert_eq!(seen, ["entry-x", "feed-007"]);
    assert_eq!(
        s.append(draft("feed", Some("entry-x")), None).unwrap(),
        None
    );
    let version: i32 =
        s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
    assert_eq!(version, SCHEMA_VERSION);
    assert_eq!(s.sync("router", Some(1)).unwrap(), None);
}

fn sync_contract<S: Store>(mut s: S)
where
    S::Error: core::fmt::Debug,
{
    s.append(draft("b", None), None).unwrap().unwrap();
    s.append(draft("b", Some("x")), None).unwrap().unwrap();
    assert_eq!(s.sync("b", Some(5001)).unwrap(), Some((1, 5001)));
    let moved: Vec<_> = s
        .peek(10)
        .unwrap()
        .into_iter()
        .map(|r| (r.sequence, r.record_id))
        .collect();
    assert_eq!(moved, [(5001, "b-5001".into()), (5002, "x".into())]);
    let next = s.append(draft("b", None), None).unwrap().unwrap();
    assert_eq!((next.sequence, next.record_id.as_str()), (5003, "b-5003"));
    assert_eq!(s.append(draft("b", Some("x")), None).unwrap(), None);
    assert_eq!(s.sync("b", Some(1)).unwrap(), None);
    assert_eq!(s.peek(1).unwrap()[0].sequence, 5001);

    assert_eq!(s.sync("c", Some(10)).unwrap(), Some((1, 10)));
    assert_eq!(
        s.append(draft("c", None), None).unwrap().unwrap().sequence,
        10
    );

    s.append(draft("d", None), None).unwrap().unwrap();
    assert_eq!(s.sync("d", None).unwrap(), None);
    assert_eq!(s.sync("d", Some(99)).unwrap(), None);
    assert_eq!(
        s.append(draft("d", None), None).unwrap().unwrap().sequence,
        2
    );

    s.append(draft("e", None), None).unwrap().unwrap();
    assert_eq!(s.sync("e", Some(1)).unwrap(), None);

    s.append(unordered("v", "k1"), None).unwrap().unwrap();
    s.append(draft("v", None), None).unwrap().unwrap();
    s.append(unordered("v", "k2"), None).unwrap().unwrap();
    assert_eq!(s.sync("v", Some(7)).unwrap(), Some((1, 7)));
    let of = |s: &mut S, source: &str| -> Vec<(u64, String)> {
        s.peek(100)
            .unwrap()
            .into_iter()
            .filter(|r| r.source_id == source)
            .map(|r| (r.sequence, r.record_id))
            .collect()
    };
    assert_eq!(
        of(&mut s, "v"),
        [(0, "k1".into()), (7, "v-7".into()), (0, "k2".into())]
    );
    s.append(unordered("w", "z"), None).unwrap().unwrap();
    assert_eq!(s.sync("w", Some(9)).unwrap(), Some((1, 9)));
    assert_eq!(
        s.append(draft("w", None), None).unwrap().unwrap().sequence,
        9
    );

    for _ in 0..4 {
        s.append(draft("g", None), None).unwrap().unwrap();
    }
    s.remove("g", "g-1").unwrap();
    s.remove("g", "g-2").unwrap();
    assert_eq!(s.sync("g", Some(2)).unwrap(), Some((3, 2)));
    assert_eq!(of(&mut s, "g"), [(2, "g-2".into()), (3, "g-3".into())]);
    s.remove("g", "g-3").unwrap();
    s.remove("g", "g-2").unwrap();
    assert!(of(&mut s, "g").is_empty());
}

#[test]
fn sqlite_renumbers_only_what_the_hub_never_saw() {
    sync_contract(SqliteStore::memory().unwrap());
}

#[test]
fn mem_store_renumbers_only_what_the_hub_never_saw() {
    sync_contract(MemStore::new());
}

#[test]
fn todays_router_outbox_keeps_its_numbers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v2.sqlite");
    let old = Connection::open(&path).unwrap();
    old.execute_batch(OLD_SCHEMA).unwrap();
    old.execute_batch(
        "ALTER TABLE seen ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;
         INSERT INTO source_seq VALUES('router', 5000);
         PRAGMA user_version = 2;",
    )
    .unwrap();
    drop(old);
    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(s.sync("router", Some(1)).unwrap(), None);
    assert_eq!(
        s.append(draft("router", None), None)
            .unwrap()
            .unwrap()
            .sequence,
        5001
    );
    let version: i32 =
        s.db.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
    assert_eq!(version, 2);
}

#[test]
fn an_outbox_keyed_by_sequence_is_rebuilt_in_write_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("pk.sqlite");
    let old = Connection::open(&path).unwrap();
    old.execute_batch(OLD_SCHEMA).unwrap();
    old.execute_batch(
        "ALTER TABLE seen ADD COLUMN seq INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE source_seq ADD COLUMN synced INTEGER NOT NULL DEFAULT 0;
         PRAGMA user_version = 2;",
    )
    .unwrap();
    let written: Vec<Record> = [("b", 1), ("a", 1), ("b", 2)]
        .into_iter()
        .map(|(source, last)| {
            let r = draft(source, None).into_record(last - 1).unwrap();
            old.execute(
                "INSERT INTO outbox VALUES(?1, ?2, ?3, ?4)",
                params![
                    r.source_id,
                    r.sequence,
                    r.record_id,
                    serde_json::to_string(&r).unwrap()
                ],
            )
            .unwrap();
            old.execute(
                "INSERT OR REPLACE INTO source_seq(source_id, seq, synced) VALUES(?1, ?2, 1)",
                params![r.source_id, r.sequence],
            )
            .unwrap();
            r
        })
        .collect();
    drop(old);

    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(s.peek(10).unwrap(), written);
    let pk: bool =
        s.db.prepare("SELECT 1 FROM pragma_index_list('outbox') WHERE origin='pk'")
            .unwrap()
            .exists([])
            .unwrap();
    assert!(!pk);
    let u1 = s.append(unordered("b", "m1"), None).unwrap().unwrap();
    let u2 = s.append(unordered("b", "m2"), None).unwrap().unwrap();
    assert_eq!(
        s.append(draft("b", None), None).unwrap().unwrap().sequence,
        3
    );
    assert!(
        s.db.execute("INSERT INTO outbox VALUES('b', 1, 'dup', '{}')", [])
            .is_err()
    );
    drop(s);
    let mut s = SqliteStore::open(&path).unwrap();
    let peeked = s.peek(10).unwrap();
    assert_eq!(peeked[..3], written[..]);
    assert_eq!(peeked[3..5], [u1, u2]);
}

#[test]
fn synced_survives_a_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("o.sqlite");
    let mut s = SqliteStore::open(&path).unwrap();
    s.append(draft("router", None), None).unwrap().unwrap();
    s.sync("router", None).unwrap();
    drop(s);
    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(s.sync("router", Some(50)).unwrap(), None);
    assert_eq!(s.peek(1).unwrap()[0].sequence, 1);
}

fn seen_window<S: Store>(mut s: S)
where
    S::Error: core::fmt::Debug,
{
    for id in ["a", "b", "c", "d"] {
        let r = s.append(draft("feed", Some(id)), None).unwrap().unwrap();
        s.remove("feed", &r.record_id).unwrap();
    }
    let again = s.append(draft("feed", Some("a")), None).unwrap().unwrap();
    s.remove("feed", &again.record_id).unwrap();
    assert_eq!(s.append(draft("feed", Some("c")), None).unwrap(), None);
    for _ in 0..3 {
        s.append(draft("feed", None), None).unwrap().unwrap();
    }
    assert!(s.append(draft("feed", Some("c")), None).unwrap().is_some());
    let pending = s.append(draft("feed", Some("p")), None).unwrap().unwrap();
    for _ in 0..5 {
        s.append(draft("feed", None), None).unwrap().unwrap();
    }
    assert_eq!(s.append(draft("feed", Some("p")), None).unwrap(), None);
    assert!(s.peek(100).unwrap().contains(&pending));
}

#[test]
fn sqlite_forgets_external_ids_outside_the_window() {
    seen_window(SqliteStore::memory().unwrap().with_seen_window(3));
}

#[test]
fn mem_store_forgets_external_ids_outside_the_window() {
    seen_window(MemStore::new().with_seen_window(3));
}

#[test]
fn automatic_ids_take_no_room_in_seen() {
    let mut s = SqliteStore::memory().unwrap();
    for _ in 0..50 {
        s.append(draft("router", None), None).unwrap();
    }
    let rows: i64 =
        s.db.query_row("SELECT COUNT(*) FROM seen", [], |r| r.get(0))
            .unwrap();
    assert_eq!(rows, 0);
}

#[test]
fn the_wal_file_stays_small() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("outbox.sqlite");
    let mut s = SqliteStore::open(&path).unwrap();
    for i in 0..3_000 {
        let mut d = draft("router", Some(&format!("e-{i}")));
        d.data = Some(json!({"i": i, "pad": "x".repeat(200)}));
        let r = s.append(d, None).unwrap().unwrap();
        if i % 2 == 0 {
            s.remove("router", &r.record_id).unwrap();
        }
    }
    let wal = std::fs::metadata(dir.path().join("outbox.sqlite-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert!(wal <= 2 * WAL_SIZE_LIMIT_BYTES as u64, "wal is {wal} bytes");
    drop(s);
    let _s = SqliteStore::open(&path).unwrap();
    let wal = std::fs::metadata(dir.path().join("outbox.sqlite-wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    assert_eq!(wal, 0);
}

#[test]
fn a_newer_schema_is_refused_instead_of_misread() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("future.sqlite");
    Connection::open(&path)
        .unwrap()
        .pragma_update(None, "user_version", 99)
        .unwrap();
    assert!(matches!(
        SqliteStore::open(&path),
        Err(Error::NewerSchema(99))
    ));
}

#[test]
fn a_session_runs_on_it() {
    use geri_adapter_sdk::{Config, Session};
    let decl = geri_proto::v4::SourceDeclaration {
        source_id: "router".into(),
        label: "r".into(),
        kind: "k".into(),
        entities: vec![],
    };
    let mut s = Session::new(Config::new(vec![decl]), SqliteStore::memory().unwrap()).unwrap();
    s.state("router", "cpu", json!(1)).unwrap();
    s.state("router", "cpu", json!(2)).unwrap();
    assert_eq!(s.store_mut().pending().unwrap(), 2);
}

#[test]
#[ignore = "needs a copy of a real outbox"]
fn migrates_a_real_outbox_copy() {
    let path = std::env::var("GERI_TEST_OUTBOX").expect("GERI_TEST_OUTBOX is not set");
    let mut s = SqliteStore::open(&path).unwrap();
    let count =
        |s: &SqliteStore, sql: &str| -> i64 { s.db.query_row(sql, [], |r| r.get(0)).unwrap() };
    println!(
        "sonra: user_version {} | seen {} | outbox {}",
        count(&s, "PRAGMA user_version"),
        count(&s, "SELECT COUNT(*) FROM seen"),
        count(&s, "SELECT COUNT(*) FROM outbox")
    );
    let before: i64 = count(&s, "SELECT seq FROM source_seq WHERE source_id='router'");
    let next = s.append(draft("router", None), None).unwrap().unwrap();
    assert_eq!(next.sequence as i64, before + 1);
    println!(
        "sıradaki kayıt: {} (sequence {})",
        next.record_id, next.sequence
    );
}

fn identity_contract<S: Store>(mut s: S)
where
    S::Error: core::fmt::Debug,
{
    let explicit = s
        .append(draft("router", Some("router-2")), None)
        .unwrap()
        .unwrap();
    assert!(s.append(draft("router", None), None).is_err());
    assert_eq!(
        s.peek(10).unwrap().as_slice(),
        std::slice::from_ref(&explicit)
    );
    let next = s
        .append(draft("router", Some("another-id")), None)
        .unwrap()
        .unwrap();
    assert_eq!(next.sequence, 2);
    s.remove("router", &explicit.record_id).unwrap();
    s.remove("router", &next.record_id).unwrap();
    assert_eq!(s.pending().unwrap(), 0);
    assert_eq!(
        s.append(draft("router", Some("router-2")), None).unwrap(),
        None
    );

    s.append(draft("new", Some("new-1")), None).unwrap();
    s.append(draft("new", Some("new-02")), None).unwrap();
    s.append(draft("new", None), None).unwrap();
    s.sync("new", Some(41)).unwrap();
    let records = s.peek(10).unwrap();
    assert_eq!(
        records
            .iter()
            .map(|r| (&*r.record_id, r.sequence))
            .collect::<Vec<_>>(),
        [("new-1", 41), ("new-02", 42), ("new-43", 43)]
    );
}

#[test]
fn sqlite_preserves_explicit_identity_and_rejects_collisions() {
    identity_contract(SqliteStore::memory().unwrap());
}
#[test]
fn memory_preserves_explicit_identity_and_rejects_collisions() {
    identity_contract(MemStore::new());
}
#[test]
fn null_and_explicit_identity_survive_a_sqlite_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("null.sqlite");
    let mut s = SqliteStore::open(&path).unwrap();
    let record = s
        .append(
            Draft {
                data: Some(json!(null)),
                indexed: Some(json!(null)),
                ..draft("router", Some("router-1"))
            },
            None,
        )
        .unwrap()
        .unwrap();
    drop(s);
    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(s.peek(1).unwrap(), [record]);
    s.sync("router", Some(41)).unwrap();
    assert_eq!(s.peek(1).unwrap()[0].record_id, "router-1");
}
#[test]
fn old_v2_metadata_distinguishes_explicit_and_generated_ids() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v2.sqlite");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(&SCHEMA.replace("automatic INTEGER NOT NULL DEFAULT 0,", ""))
        .unwrap();
    for (n, automatic) in [(1, false), (2, true)] {
        let r = draft("router", Some(&format!("router-{n}")))
            .into_record(n - 1)
            .unwrap();
        db.execute(
            "INSERT INTO outbox(source_id,seq,record_id,raw_json) VALUES(?1,?2,?3,?4)",
            params![
                r.source_id,
                n,
                r.record_id,
                serde_json::to_string(&r).unwrap()
            ],
        )
        .unwrap();
        if !automatic {
            db.execute("INSERT INTO seen VALUES('router','router-1',1)", [])
                .unwrap();
        }
    }
    db.execute_batch("INSERT INTO source_seq VALUES('router',2,0); PRAGMA user_version=2;")
        .unwrap();
    drop(db);
    let mut s = SqliteStore::open(path).unwrap();
    s.sync("router", Some(41)).unwrap();
    let rs = s.peek(10).unwrap();
    assert_eq!(
        (&*rs[0].record_id, &*rs[1].record_id),
        ("router-1", "router-42")
    );
}

fn sync_collision_contract<S: Store>(mut s: S)
where
    S::Error: core::fmt::Debug,
{
    s.append(draft("router", Some("router-42")), None).unwrap();
    s.append(draft("router", None), None).unwrap();
    let before = s.peek(10).unwrap();
    assert!(s.sync("router", Some(41)).is_err());
    assert_eq!(s.peek(10).unwrap(), before);
    s.sync("router", Some(100)).unwrap();
    assert_eq!(s.peek(10).unwrap()[1].record_id, "router-101");
}
#[test]
fn sqlite_rolls_back_an_identity_collision_during_sync() {
    sync_collision_contract(SqliteStore::memory().unwrap());
}
#[test]
fn memory_rolls_back_an_identity_collision_during_sync() {
    sync_collision_contract(MemStore::new());
}

#[test]
fn invalid_legacy_outbox_survives_reopen_and_is_reported_without_a_gap() {
    use geri_adapter_sdk::{Config, Error as SdkError, Event, Session};
    use geri_proto::v4::{
        Limits, SourceDeclaration, VERSION, Welcome,
        msg::{Frame, ToHub, ToSource},
    };
    let dir = tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let old = {
        let mut store = SqliteStore::open(&path).unwrap();
        let mut d = draft("router", None);
        d.data = None;
        store.append(d, None).unwrap().unwrap()
    };
    let decl = SourceDeclaration {
        source_id: "router".into(),
        label: "r".into(),
        kind: "k".into(),
        entities: vec![],
    };
    let mut session =
        Session::new(Config::new(vec![decl]), SqliteStore::open(&path).unwrap()).unwrap();
    let next = session.state("router", "cpu", json!(2)).unwrap().unwrap();
    assert_eq!((old.sequence, next.sequence), (1, 2));
    session.handle_connected(0, false);
    let hello = session.poll_transmit(0).unwrap();
    assert!(matches!(hello.msg, ToHub::Hello { .. }));
    session.handle_frame(
        0,
        Frame {
            id: hello.id,
            msg: ToSource::Welcome(Welcome {
                v: VERSION,
                agent_id: "a".into(),
                limits: Limits {
                    max_batch_records: 128,
                    max_inline_bytes: 65536,
                    max_body_bytes: 1 << 20,
                    max_blob_bytes: 1 << 24,
                    max_webhook_bytes: 65536,
                },
            }),
        },
    );
    let declare = session.poll_transmit(0).unwrap();
    session.handle_frame(
        0,
        Frame {
            id: declare.id,
            msg: ToSource::Ok {
                watched: None,
                next_sequence: Some(1),
            },
        },
    );
    assert!(session.poll_transmit(0).is_none());
    let events: Vec<_> = std::iter::from_fn(|| session.poll_event()).collect();
    assert!(
        events.contains(&Event::Error(SdkError::InvalidOutboxRecord {
            source_id: "router".into(),
            record_id: "router-1".into(),
            reason: geri_proto::v4::validate::Invalid::PayloadCount,
        }))
    );
    drop(session);
    let mut store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.peek(128).unwrap(), [old, next]);
    let after = store.append(draft("router", None), None).unwrap().unwrap();
    assert_eq!(after.sequence, 3);
}
