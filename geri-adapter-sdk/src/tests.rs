use super::*;
use alloc::{string::String, vec, vec::Vec};
use geri_proto::v4::{
    ApiError, Command, Limits, Receipt, ReceiptStatus, RecordKind, RecordReceipt,
    SourceDeclaration, VERSION, Welcome,
    msg::{Frame, ToHub, ToSource},
};
use serde_json::{Map, Value, json};

fn decl(id: &str) -> SourceDeclaration {
    SourceDeclaration {
        source_id: id.into(),
        label: id.into(),
        kind: "test".into(),
        entities: vec![],
    }
}

fn controlled(id: &str) -> SourceDeclaration {
    let mut wan = geri_proto::v4::Entity::new(id, "wan");
    wan.control = Some(geri_proto::Control::new(
        geri_proto::Shape::Trigger,
        geri_proto::Danger::Safe,
    ));
    SourceDeclaration {
        entities: vec![wan],
        ..decl(id)
    }
}

fn welcome(v: u16) -> ToSource {
    ToSource::Welcome(Welcome {
        v,
        agent_id: "agent".into(),
        limits: Limits {
            max_batch_records: 128,
            max_inline_bytes: 65536,
            max_body_bytes: 1 << 20,
            max_blob_bytes: 1 << 24,
            max_webhook_bytes: 65536,
        },
    })
}

fn accepted(source: &str, record_id: &str, cursor: u64) -> RecordReceipt {
    RecordReceipt {
        source_id: source.into(),
        record_id: record_id.into(),
        status: ReceiptStatus::Accepted,
        cursor: Some(cursor),
        expected_sequence: None,
    }
}

fn command(id: &str) -> Command {
    Command {
        command_id: id.into(),
        source_id: "router".into(),
        entity_id: "wan".into(),
        action: "press".into(),
        args: Value::Null,
        issued_by: "can".into(),
        issued_at_ms: 1,
    }
}

fn values(v: i64) -> Map<String, Value> {
    json!({"cpu": v}).as_object().unwrap().clone()
}

struct Hub {
    s: Session<MemStore>,
    now: u64,
    sources: usize,
}

impl Hub {
    fn new(sources: &[&str]) -> Self {
        Self::with_store(sources, MemStore::new())
    }

    fn controlled(sources: &[&str]) -> Self {
        Self::build(
            sources.iter().map(|s| controlled(s)).collect(),
            MemStore::new(),
        )
    }

    fn with_store(sources: &[&str], store: MemStore) -> Self {
        Self::build(sources.iter().map(|s| decl(s)).collect(), store)
    }

    fn build(decls: Vec<SourceDeclaration>, store: MemStore) -> Self {
        let sources = decls.len();
        let config = Config::new(decls);
        Self {
            s: Session::new(config, store).unwrap(),
            now: 1_000,
            sources,
        }
    }

    fn at(&mut self, now: u64) {
        self.now = now;
        self.s.handle_timeout(now);
    }

    fn next(&mut self) -> Frame<ToHub> {
        self.s
            .poll_transmit(self.now)
            .expect("session should send something")
    }

    fn idle(&mut self) -> bool {
        self.s.poll_transmit(self.now).is_none()
    }

    fn answer(&mut self, to: &Frame<ToHub>, msg: ToSource) {
        self.s.handle_frame(self.now, Frame { id: to.id, msg });
    }

    fn signal(&mut self, msg: ToSource) {
        self.s.handle_frame(self.now, Frame { id: None, msg });
    }

    fn events(&mut self) -> Vec<Event> {
        core::iter::from_fn(|| self.s.poll_event()).collect()
    }

    fn connect(&mut self, pushes: bool) {
        self.connect_with(pushes, None);
    }

    fn connect_with(&mut self, pushes: bool, next_sequence: Option<u64>) {
        self.s.handle_connected(self.now, pushes);
        let hello = self.next();
        assert_eq!(
            hello.msg,
            ToHub::Hello {
                v: VERSION,
                auth: None
            }
        );
        self.answer(&hello, welcome(VERSION));
        assert!(self.events().contains(&Event::Connected { push: pushes }));
        for _ in 0..self.sources {
            let frame = self.next();
            assert!(matches!(frame.msg, ToHub::Declare(_)), "{:?}", frame.msg);
            self.answer(
                &frame,
                ToSource::Ok {
                    watched: None,
                    next_sequence,
                },
            );
        }
    }

    fn ready(&mut self, pushes: bool) {
        self.connect(pushes);
        let claim = self.next();
        assert!(matches!(claim.msg, ToHub::Claim { .. }), "{:?}", claim.msg);
        self.answer(&claim, ToSource::Idle);
    }
}

#[test]
fn handshake_then_commands_then_outbox() {
    let mut h = Hub::controlled(&["router"]);
    h.s.state("router", "cpu", json!(12)).unwrap();
    h.s.state("router", "cpu", json!(13)).unwrap();
    assert!(h.s.wants_connect(h.now));
    h.connect(false);

    let claim = h.next();
    assert_eq!(
        claim.msg,
        ToHub::Claim {
            source_id: "router".into()
        }
    );
    h.answer(&claim, ToSource::Idle);

    let batch = h.next();
    let ToHub::Batch(b) = &batch.msg else {
        panic!("{:?}", batch.msg)
    };
    let seqs: Vec<u64> = b.records.iter().map(|r| r.sequence).collect();
    assert_eq!(seqs, [1, 2]);
    h.answer(
        &batch,
        ToSource::Receipt(Receipt {
            v: VERSION,
            records: vec![
                accepted("router", "router-1", 10),
                accepted("router", "router-2", 11),
            ],
        }),
    );
    assert_eq!(h.s.store_mut().pending().unwrap(), 0);
    assert!(h.idle());
    assert_eq!(h.s.poll_timeout(), Some(h.now + 10_000));
    h.at(h.now + 10_000);
    assert!(matches!(h.next().msg, ToHub::Claim { .. }));
}

#[test]
fn a_ring_jumps_ahead_of_a_long_outbox() {
    let mut h = Hub::controlled(&["router"]);
    for i in 0..300 {
        h.s.state("router", "cpu", json!(i)).unwrap();
    }
    h.ready(true);
    let batch = h.next();
    assert!(matches!(batch.msg, ToHub::Batch(_)));
    h.signal(ToSource::Ring {
        source_id: "router".into(),
    });
    let ToHub::Batch(b) = &batch.msg else {
        unreachable!()
    };
    let records = b
        .records
        .iter()
        .map(|r| accepted("router", &r.record_id, r.sequence))
        .collect();
    h.answer(
        &batch,
        ToSource::Receipt(Receipt {
            v: VERSION,
            records,
        }),
    );
    assert!(h.s.store_mut().pending().unwrap() > 0);
    assert!(matches!(h.next().msg, ToHub::Claim { .. }));
}

#[test]
fn sources_without_controls_are_never_claimed() {
    let mut h = Hub::new(&["feed"]);
    h.connect(false);
    assert!(h.idle());
    h.signal(ToSource::Ring {
        source_id: "feed".into(),
    });
    assert!(h.idle());
    h.at(h.now + 60_000);
    assert!(h.idle());
    h.s.declare(controlled("feed")).unwrap();
    let declare = h.next();
    h.answer(
        &declare,
        ToSource::Ok {
            watched: None,
            next_sequence: None,
        },
    );
    h.s.handle_disconnected(h.now);
    h.at(h.now + 60_000);
    h.connect(false);
    assert!(matches!(h.next().msg, ToHub::Claim { .. }));
}

#[test]
fn records_are_sequenced_deduplicated_and_survive_restart() {
    let mut h = Hub::new(&["feed"]);
    let first =
        h.s.event("feed", "entry", Some("e-1"), None, json!({"t": 1}));
    assert_eq!(first.unwrap().unwrap().sequence, 1);
    assert_eq!(
        h.s.event("feed", "entry", Some("e-1"), None, json!({"t": 1})),
        Ok(None)
    );
    let store = h.s.into_store();
    let mut h = Hub::with_store(&["feed"], store);
    assert_eq!(
        h.s.event("feed", "entry", Some("e-1"), None, json!({"t": 1})),
        Ok(None)
    );
    let next = h.s.state("feed", "count", json!(1)).unwrap().unwrap();
    assert_eq!((next.sequence, next.record_id.as_str()), (2, "feed-2"));
    assert_eq!(h.s.store_mut().pending().unwrap(), 2);
}

#[test]
fn a_gap_stops_the_outbox_at_that_record_and_backs_off() {
    let mut h = Hub::controlled(&["router"]);
    h.s.state("router", "cpu", json!(1)).unwrap();
    h.s.state("router", "cpu", json!(2)).unwrap();
    h.ready(false);
    let batch = h.next();
    let gap = RecordReceipt {
        source_id: "router".into(),
        record_id: "router-2".into(),
        status: ReceiptStatus::Gap,
        cursor: None,
        expected_sequence: Some(1),
    };
    h.answer(
        &batch,
        ToSource::Receipt(Receipt {
            v: VERSION,
            records: vec![accepted("router", "router-1", 5), gap],
        }),
    );
    assert!(h.events().contains(&Event::Error(Error::Gap {
        source_id: "router".into(),
        expected: 1
    })));
    assert_eq!(h.s.store_mut().pending().unwrap(), 1);
    assert!(h.idle());
    h.at(h.now + 1_000);
    let ToHub::Batch(b) = h.next().msg else {
        panic!()
    };
    assert_eq!(b.records[0].sequence, 2);
}

fn batch_seqs(frame: &Frame<ToHub>) -> Vec<(u64, String)> {
    let ToHub::Batch(b) = &frame.msg else {
        panic!("{:?}", frame.msg)
    };
    b.records
        .iter()
        .map(|r| (r.sequence, r.record_id.clone()))
        .collect()
}

#[test]
fn a_fresh_outbox_starts_where_the_hub_left_off() {
    let mut h = Hub::new(&["router"]);
    h.s.state("router", "cpu", json!(1)).unwrap();
    h.connect_with(false, Some(5001));
    assert!(h.events().contains(&Event::Renumbered {
        source_id: "router".into(),
        from: 1,
        to: 5001
    }));
    let batch = h.next();
    assert_eq!(batch_seqs(&batch), [(5001, "router-5001".into())]);
    let next = h.s.state("router", "cpu", json!(2)).unwrap().unwrap();
    assert_eq!(next.sequence, 5002);
}

#[test]
fn unordered_records_ride_along_without_a_number() {
    let mut h = Hub::new(&["router"]);
    let u =
        h.s.unordered("router", "cpu", "m1", RecordKind::State, Some(5), json!(1))
            .unwrap()
            .unwrap();
    assert_eq!((u.sequence, u.record_id.as_str()), (0, "m1"));
    h.s.state("router", "cpu", json!(2)).unwrap();
    assert_eq!(
        h.s.unordered("router", "cpu", "m1", RecordKind::State, Some(6), json!(3)),
        Ok(None)
    );
    h.connect_with(false, Some(5001));
    assert!(h.events().contains(&Event::Renumbered {
        source_id: "router".into(),
        from: 1,
        to: 5001
    }));
    let batch = h.next();
    assert_eq!(
        batch_seqs(&batch),
        [(0, "m1".into()), (5001, "router-5001".into())]
    );
    h.answer(
        &batch,
        ToSource::Receipt(Receipt {
            v: VERSION,
            records: vec![
                RecordReceipt {
                    status: ReceiptStatus::Duplicate,
                    ..accepted("router", "m1", 7)
                },
                accepted("router", "router-5001", 8),
            ],
        }),
    );
    assert_eq!(h.s.store_mut().pending().unwrap(), 0);
    assert_eq!(
        h.s.state("router", "cpu", json!(4))
            .unwrap()
            .unwrap()
            .sequence,
        5002
    );
}

#[test]
fn an_unordered_record_needs_an_id() {
    let mut h = Hub::new(&["router"]);
    let draft = Draft {
        source_id: "router".into(),
        entity_id: "cpu".into(),
        record_id: None,
        kind: RecordKind::State,
        event_time_ms: None,
        content_type: "application/json".into(),
        data: Some(json!(1)),
        indexed: None,
        blob: None,
        unordered: true,
    };
    assert_eq!(h.s.record(draft.clone()), Err(Error::MissingRecordId));
    assert_eq!(
        h.s.unordered("router", "cpu", "a/b", RecordKind::State, None, json!(1)),
        Err(Error::InvalidId)
    );
    assert_eq!(h.s.store_mut().pending().unwrap(), 0);
    assert!(
        h.s.record(Draft {
            unordered: false,
            ..draft
        })
        .unwrap()
        .is_some()
    );
}

#[test]
fn numbers_the_hub_has_seen_are_never_moved() {
    let mut h = Hub::new(&["router"]);
    h.s.state("router", "cpu", json!(1)).unwrap();
    h.connect(false);
    assert_eq!(batch_seqs(&h.next()), [(1, "router-1".into())]);
    h.s.handle_disconnected(h.now);
    h.at(h.now + 60_000);
    h.connect_with(false, Some(2));
    assert!(
        !h.events()
            .iter()
            .any(|e| matches!(e, Event::Renumbered { .. }))
    );
    assert_eq!(batch_seqs(&h.next()), [(1, "router-1".into())]);
}

#[test]
fn http_commands_are_drained_and_results_go_first() {
    let mut h = Hub::controlled(&["router"]);
    h.connect(false);
    let claim = h.next();
    h.answer(&claim, ToSource::Command(command("c1")));
    let Some(Event::Command(c)) = h.events().pop() else {
        panic!()
    };
    let again = h.next();
    assert!(matches!(again.msg, ToHub::Claim { .. }));
    h.answer(&again, ToSource::Idle);
    h.s.result(&c, true, json!(null));
    let result = h.next();
    assert_eq!(
        result.msg,
        ToHub::Result {
            source_id: "router".into(),
            result: geri_proto::v4::CommandResult {
                command_id: "c1".into(),
                ok: true,
                result: json!(null),
            },
        }
    );
    h.answer(
        &result,
        ToSource::Ok {
            watched: None,
            next_sequence: None,
        },
    );
    assert!(h.idle());
}

#[test]
fn push_mode_waits_for_ring_and_pace() {
    let mut h = Hub::controlled(&["router"]);
    h.s.live("router", values(1)).unwrap();
    h.ready(true);
    h.at(h.now + 60_000);
    assert!(h.idle());
    assert_eq!(h.s.poll_timeout(), None);

    h.signal(ToSource::Ring {
        source_id: "router".into(),
    });
    let claim = h.next();
    assert!(matches!(claim.msg, ToHub::Claim { .. }));
    h.answer(&claim, ToSource::Idle);

    h.signal(ToSource::Pace {
        source_id: "router".into(),
        ms: 1_000,
    });
    assert!(h.events().contains(&Event::Watched {
        source_id: "router".into(),
        watched: true
    }));
    let live = h.next();
    assert_eq!(
        live.msg,
        ToHub::Live {
            source_id: "router".into(),
            values: values(1)
        }
    );
    h.answer(
        &live,
        ToSource::Ok {
            watched: None,
            next_sequence: None,
        },
    );
    assert!(h.idle());
    assert_eq!(h.s.poll_timeout(), Some(h.now + 1_000));
    h.s.live("router", values(2)).unwrap();
    h.at(h.now + 1_000);
    let live = h.next();
    assert!(matches!(&live.msg, ToHub::Live { values: v, .. } if *v == values(2)));
    h.answer(
        &live,
        ToSource::Ok {
            watched: None,
            next_sequence: None,
        },
    );

    h.signal(ToSource::Pace {
        source_id: "router".into(),
        ms: 0,
    });
    assert!(h.events().contains(&Event::Watched {
        source_id: "router".into(),
        watched: false
    }));
    h.at(h.now + 60_000);
    assert!(h.idle());
}

#[test]
fn http_live_speeds_up_while_watched() {
    let mut h = Hub::controlled(&["router"]);
    h.s.live("router", values(1)).unwrap();
    h.ready(false);
    let live = h.next();
    assert!(matches!(live.msg, ToHub::Live { .. }));
    h.answer(
        &live,
        ToSource::Ok {
            watched: Some(false),
            next_sequence: None,
        },
    );
    assert_eq!(h.s.poll_timeout(), Some(h.now + 10_000));
    h.at(h.now + 10_000);
    let claim = h.next();
    assert!(matches!(claim.msg, ToHub::Claim { .. }));
    h.answer(&claim, ToSource::Idle);
    let live = h.next();
    assert!(matches!(live.msg, ToHub::Live { .. }));
    h.answer(
        &live,
        ToSource::Ok {
            watched: Some(true),
            next_sequence: None,
        },
    );
    assert!(h.events().contains(&Event::Watched {
        source_id: "router".into(),
        watched: true
    }));
    assert_eq!(h.s.poll_timeout(), Some(h.now + 2_000));
}

#[test]
fn a_timeout_reconnects_redeclares_and_resends_the_result() {
    let mut h = Hub::controlled(&["router"]);
    h.ready(false);
    h.s.result(&command("c9"), false, json!("boom"));
    let result = h.next();
    assert!(matches!(result.msg, ToHub::Result { .. }));
    h.at(h.now + 30_000);
    assert!(h.events().contains(&Event::Error(Error::Timeout)));
    assert!(!h.s.wants_connect(h.now));
    h.at(h.now + 1_000);
    assert!(h.s.wants_connect(h.now));
    h.connect(false);
    let again = h.next();
    assert_eq!(again.msg, result.msg);
}

#[test]
fn stale_answers_are_ignored_and_wrong_ones_reset_the_connection() {
    let mut h = Hub::controlled(&["router"]);
    h.connect(false);
    let claim = h.next();
    h.s.handle_frame(
        h.now,
        Frame {
            id: Some(claim.id.unwrap() + 100),
            msg: ToSource::Idle,
        },
    );
    assert!(h.idle(), "still waiting for the real answer");
    h.answer(
        &claim,
        ToSource::Receipt(Receipt {
            v: VERSION,
            records: vec![],
        }),
    );
    assert!(matches!(
        h.events().as_slice(),
        [Event::Error(Error::Protocol(_))]
    ));
    assert!(h.idle());
    h.at(h.now + 1_000);
    assert!(h.s.wants_connect(h.now));
}

#[test]
fn missing_blobs_are_requested_then_the_batch_is_retried() {
    let mut h = Hub::new(&["feed"]);
    let record =
        h.s.blob(
            "feed",
            "entry",
            Some("e-1"),
            geri_proto::v4::RecordKind::Event,
            None,
            "application/xml",
            b"<entry/>",
            json!({"title": "x"}),
        )
        .unwrap()
        .unwrap();
    let blob = record.blob.clone().unwrap();
    h.connect(false);
    let batch = h.next();
    h.answer(
        &batch,
        ToSource::Error {
            status: 422,
            error: ApiError {
                code: "missing_blob".into(),
                message: "upload".into(),
                earliest_cursor: None,
                counter: None,
            },
        },
    );
    assert!(h.events().contains(&Event::UploadBlobs(vec![blob.clone()])));
    assert_eq!(
        h.s.store_mut().blob(&blob.sha256).unwrap().as_deref(),
        Some(&b"<entry/>"[..])
    );
    h.s.blobs_uploaded(h.now);
    assert!(matches!(h.next().msg, ToHub::Batch(_)));
}

#[test]
fn a_hub_of_another_version_is_not_used() {
    let mut h = Hub::new(&["router"]);
    h.s.handle_connected(h.now, false);
    let hello = h.next();
    h.answer(&hello, welcome(5));
    assert_eq!(h.events(), [Event::Error(Error::Version(5))]);
    assert!(h.idle());
    h.at(h.now + 1_000);
    assert_eq!(
        h.next().msg,
        ToHub::Hello {
            v: VERSION,
            auth: None
        }
    );
}

#[test]
fn sources_declared_later_are_sent_while_connected() {
    let mut h = Hub::controlled(&["router"]);
    h.ready(false);
    h.s.declare(decl("feed")).unwrap();
    let declare = h.next();
    assert!(matches!(&declare.msg, ToHub::Declare(d) if d.source_id == "feed"));
}

#[test]
fn bad_input_is_refused_before_the_outbox() {
    let mut h = Hub::new(&["router"]);
    assert_eq!(
        h.s.state("nope", "cpu", json!(1)),
        Err(Error::UnknownSource("nope".into()))
    );
    assert_eq!(h.s.state("router", "c/pu", json!(1)), Err(Error::InvalidId));
    let big = Value::String("x".repeat(70_000));
    assert_eq!(h.s.state("router", "cpu", big), Err(Error::InvalidPayload));
    assert_eq!(
        h.s.cloudevent("router", json!({"specversion": "0.3"})),
        Err(Error::InvalidCloudEvent)
    );
    let event = json!({"specversion": "1.0", "source": "/w", "id": "42", "type": "x"});
    let r = h.s.cloudevent("router", event.clone()).unwrap().unwrap();
    assert_eq!(original_cloudevent(&r), Some(&event));
    assert_eq!(h.s.cloudevent("router", event), Ok(None));
    assert_eq!(h.s.store_mut().pending().unwrap(), 1);
    let long = String::from("s").repeat(44);
    h.s.declare(decl(&long)).unwrap();
    assert_eq!(h.s.state(&long, "cpu", json!(1)), Err(Error::InvalidId));
}

#[test]
fn idle_reply_preserves_a_ring_received_during_claim() {
    let mut h = Hub::controlled(&["router"]);
    h.connect(true);
    let claim = h.next();
    h.signal(ToSource::Ring {
        source_id: "router".into(),
    });
    h.answer(&claim, ToSource::Idle);
    assert!(matches!(h.next().msg, ToHub::Claim { .. }));
}

#[test]
fn stale_declaration_reply_preserves_the_new_schema() {
    let mut h = Hub::new(&["router"]);
    h.s.handle_connected(h.now, true);
    let hello = h.next();
    h.answer(&hello, welcome(VERSION));
    let original = h.next();
    let mut updated = decl("router");
    updated.label = "updated".into();
    h.s.declare(updated.clone()).unwrap();
    h.answer(
        &original,
        ToSource::Ok {
            watched: None,
            next_sequence: Some(1),
        },
    );
    assert_eq!(h.next().msg, ToHub::Declare(updated));
}

#[test]
fn large_batches_are_split_without_losing_any_records() {
    for payload in [json!("x".repeat(65530)), json!(vec![0.5; 10000])] {
        let mut h = Hub::new(&["router"]);
        for _ in 0..20 {
            h.s.state("router", "cpu", payload.clone()).unwrap();
        }
        h.connect(false);
        let mut accepted_count = 0;
        while h.s.store_mut().pending().unwrap() > 0 {
            let frame = h.next();
            let ToHub::Batch(batch) = &frame.msg else {
                panic!("expected batch")
            };
            let limit = h.s.limits().unwrap().max_body_bytes;
            assert!(serde_json::to_vec(&frame).unwrap().len() <= limit);
            assert!(geri_proto::cbor::to_vec(&frame).unwrap().len() <= limit);
            let receipts = batch
                .records
                .iter()
                .map(|r| {
                    accepted_count += 1;
                    accepted(&r.source_id, &r.record_id, accepted_count)
                })
                .collect();
            h.answer(
                &frame,
                ToSource::Receipt(Receipt {
                    v: VERSION,
                    records: receipts,
                }),
            );
        }
        assert_eq!(accepted_count, 20);
    }
}

#[test]
fn declarations_back_off_before_sending_unsynced_records() {
    let mut h = Hub::new(&["router"]);
    h.s.state("router", "cpu", json!(1)).unwrap();
    h.s.handle_connected(h.now, false);
    let hello = h.next();
    h.answer(&hello, welcome(VERSION));
    let declaration = h.next();
    h.answer(
        &declaration,
        ToSource::Error {
            status: 503,
            error: ApiError {
                code: "busy".into(),
                message: "later".into(),
                earliest_cursor: None,
                counter: None,
            },
        },
    );
    assert!(h.idle());
    assert!(h.s.poll_timeout().unwrap() > h.now);
    h.at(h.now + 1000);
    let declaration = h.next();
    h.answer(
        &declaration,
        ToSource::Ok {
            watched: None,
            next_sequence: Some(41),
        },
    );
    assert_eq!(batch_seqs(&h.next()), [(41, "router-41".into())]);
}

#[test]
fn automatic_ids_cannot_be_given_from_outside() {
    let mut h = Hub::new(&["feed"]);
    assert_eq!(
        h.s.event("feed", "entry", Some("feed-1"), None, json!(1)),
        Err(Error::ReservedRecordId)
    );
    assert_eq!(
        h.s.event("feed", "entry", Some("feed-007"), None, json!(1)),
        Err(Error::ReservedRecordId)
    );
    for id in ["feed-", "feed-1a", "feedx-1", "feed1", "feed--1"] {
        assert!(
            h.s.event("feed", "entry", Some(id), None, json!(1)).is_ok(),
            "{id}"
        );
    }
    let next = h.s.state("feed", "n", json!(1)).unwrap().unwrap();
    assert_eq!(next.record_id, alloc::format!("feed-{}", next.sequence));
}

fn too_large(to: &Frame<ToHub>) -> Frame<ToSource> {
    Frame {
        id: to.id,
        msg: ToSource::Error {
            status: 413,
            error: ApiError {
                code: "too_large".into(),
                message: "body too large".into(),
                earliest_cursor: None,
                counter: None,
            },
        },
    }
}

#[test]
fn a_413_halves_the_batch_until_it_fits() {
    let mut h = Hub::new(&["router"]);
    for i in 0..8 {
        h.s.state("router", "cpu", json!(i)).unwrap();
    }
    h.connect(false);
    let mut sizes = Vec::new();
    loop {
        let frame = h.next();
        let ToHub::Batch(b) = &frame.msg else {
            panic!("{:?}", frame.msg)
        };
        sizes.push(b.records.len());
        if b.records.len() > 2 {
            h.s.handle_frame(h.now, too_large(&frame));
            continue;
        }
        let records = b
            .records
            .iter()
            .map(|r| accepted("router", &r.record_id, r.sequence))
            .collect();
        h.answer(
            &frame,
            ToSource::Receipt(Receipt {
                v: VERSION,
                records,
            }),
        );
        break;
    }
    assert_eq!(sizes, [8, 4, 2]);
    let ToHub::Batch(b) = h.next().msg else {
        panic!()
    };
    assert_eq!(b.records.len(), 6);
}

#[test]
fn a_single_record_over_the_hub_limit_is_reported_and_waits() {
    let mut h = Hub::new(&["router"]);
    h.s.state("router", "cpu", json!(1)).unwrap();
    h.connect(false);
    let frame = h.next();
    h.s.handle_frame(h.now, too_large(&frame));
    assert!(h.events().contains(&Event::Error(Error::RecordTooLarge {
        source_id: "router".into(),
        record_id: "router-1".into(),
    })));
    assert_eq!(h.s.store_mut().pending().unwrap(), 1);
    assert!(h.idle());
    h.at(h.now + 1_000);
    assert!(matches!(h.next().msg, ToHub::Batch(_)));
}

#[test]
fn unread_errors_are_capped() {
    let mut h = Hub::new(&["router"]);
    for _ in 0..200 {
        h.s.handle_connected(h.now, false);
        let hello = h.next();
        h.answer(&hello, welcome(VERSION + 1));
    }
    let errors = h
        .events()
        .iter()
        .filter(|e| matches!(e, Event::Error(_)))
        .count();
    assert_eq!(errors, session::MAX_QUEUED_ERRORS);
}

// F01: doğrulama depolama sınırından önce yapılmalı.
fn draft() -> Draft {
    Draft {
        source_id: "router".into(),
        entity_id: "cpu".into(),
        record_id: None,
        kind: RecordKind::State,
        event_time_ms: None,
        content_type: "application/json".into(),
        data: Some(json!(null)),
        indexed: None,
        blob: None,
        unordered: false,
    }
}

#[test]
fn generated_sequences_stop_at_the_exact_cross_store_limit() {
    let max = geri_proto::v4::MAX_SEQUENCE;
    assert_eq!(draft().into_record(max - 1).unwrap().sequence, max);
    assert_eq!(
        draft().into_record(max),
        Err(store::SequenceError::Exhausted)
    );
    let mut unordered = draft();
    unordered.unordered = true;
    assert_eq!(unordered.into_record(u64::MAX).unwrap().sequence, 0);
}

#[test]
fn resync_rejects_out_of_range_values_without_changing_the_outbox() {
    let mut store = MemStore::default();
    store.append(draft(), None).unwrap();
    let before = store.peek(10).unwrap();
    assert!(matches!(
        store.sync("router", Some(geri_proto::v4::MAX_SEQUENCE + 1)),
        Err(MemStoreError::Sequence(SequenceError::OutOfRange))
    ));
    assert_eq!(store.peek(10).unwrap(), before);
    assert!(matches!(
        renumber(before[0].clone(), -1, true),
        Err(SequenceError::ResyncRange)
    ));
}

fn blob_ref() -> geri_proto::v4::BlobRef {
    geri_proto::v4::BlobRef {
        sha256: "ab".repeat(32),
        size: 1,
    }
}

#[test]
fn invalid_drafts_do_not_consume_sequence_or_poison_the_next_batch() {
    let mut cases = Vec::new();
    let mut d = draft();
    d.data = None;
    cases.push(d);
    let mut d = draft();
    d.blob = Some(blob_ref());
    cases.push(d);
    for content_type in ["", "image/ş", "text/plain"] {
        let mut d = draft();
        d.content_type = content_type.into();
        cases.push(d);
    }
    let mut d = draft();
    d.content_type = "x".repeat(129);
    cases.push(d);
    let mut d = draft();
    d.data = Some(json!("x".repeat(65535)));
    cases.push(d);
    let mut d = draft();
    d.indexed = Some(json!("x".repeat(16383)));
    cases.push(d);
    for (hash, size) in [
        ("ab".repeat(32), 0),
        ("AB".repeat(32), 1),
        ("g".repeat(64), 1),
        ("a".repeat(63), 1),
    ] {
        let mut d = draft();
        d.data = None;
        d.blob = Some(geri_proto::v4::BlobRef { sha256: hash, size });
        cases.push(d);
    }
    for invalid in cases {
        let mut h = Hub::new(&["router"]);
        assert_eq!(h.s.record(invalid), Err(Error::InvalidPayload));
        assert_eq!(h.s.store_mut().pending().unwrap(), 0);
        let valid = h.s.state("router", "cpu", json!(null)).unwrap().unwrap();
        assert_eq!((valid.sequence, valid.record_id.as_str()), (1, "router-1"));
        h.connect(false);
        let ToHub::Batch(batch) = h.next().msg else {
            panic!("batch bekleniyor")
        };
        assert_eq!(batch.records, [valid]);
        assert_eq!(geri_proto::v4::validate::record(&batch.records[0]), Ok(()));
    }
}

#[test]
fn blob_convenience_validates_before_storing_bytes_or_allocating_sequence() {
    let mut h = Hub::new(&["router"]);
    for content_type in ["image/ş", "", "application/json\nş"] {
        assert_eq!(
            h.s.blob(
                "router",
                "cpu",
                None,
                RecordKind::State,
                None,
                content_type,
                b"image",
                json!({})
            ),
            Err(Error::InvalidPayload)
        );
    }
    assert_eq!(
        h.s.blob(
            "router",
            "cpu",
            None,
            RecordKind::State,
            None,
            "image/png",
            b"image",
            json!("x".repeat(16383))
        ),
        Err(Error::InvalidPayload)
    );
    assert_eq!(h.s.store_mut().pending().unwrap(), 0);
    use sha2::Digest;
    let hash = hex::encode(sha2::Sha256::digest(b"image"));
    assert_eq!(h.s.store_mut().blob(&hash).unwrap(), None);
    let valid =
        h.s.blob(
            "router",
            "cpu",
            None,
            RecordKind::State,
            None,
            "image/png",
            b"image",
            json!({}),
        )
        .unwrap()
        .unwrap();
    assert_eq!(valid.sequence, 1);
    assert_eq!(geri_proto::v4::validate::record(&valid), Ok(()));
    assert_eq!(
        h.s.store_mut().blob(&hash).unwrap(),
        Some(b"image".to_vec())
    );
}

#[test]
fn record_size_limits_count_serialized_bytes_at_the_boundary() {
    let mut h = Hub::new(&["router"]);
    let mut d = draft();
    d.data = Some(json!("x".repeat(65534)));
    d.indexed = Some(json!("x".repeat(16382)));
    assert_eq!(h.s.record(d).unwrap().unwrap().sequence, 1);
    // UTF-8 ve JSON kaçışlarının wire boyutu sınırda da hesaba katılır.
    let mut invalid = draft();
    invalid.data = Some(json!("ş".repeat(32768)));
    assert_eq!(h.s.record(invalid), Err(Error::InvalidPayload));
    let mut invalid = draft();
    invalid.indexed = Some(json!("\n".repeat(8192)));
    assert_eq!(h.s.record(invalid), Err(Error::InvalidPayload));
    assert_eq!(
        h.s.state("router", "cpu", json!(1))
            .unwrap()
            .unwrap()
            .sequence,
        2
    );
}

fn invalid_declarations() -> Vec<SourceDeclaration> {
    let mut cases = Vec::new();
    let mut d = decl("bad/id");
    cases.push(d);
    d = decl("router");
    d.label = "x".repeat(129);
    cases.push(d);
    d = decl("router");
    d.kind = "x".repeat(65);
    cases.push(d);
    d = decl("router");
    d.entities = (0..129)
        .map(|i| geri_proto::v4::Entity::new("router", alloc::format!("e{i}")))
        .collect();
    cases.push(d);
    for which in 0..8 {
        d = controlled("router");
        let entity = &mut d.entities[0];
        match which {
            0 => entity.source_id = "other".into(),
            1 => entity.entity_id = "bad/id".into(),
            2 => entity.label = "x".repeat(129),
            3 => entity.kind = "x".repeat(65),
            4 => entity.group = Some(String::new()),
            5 => entity.group = Some("x".repeat(129)),
            6 => {
                entity.control = Some(geri_proto::Control::new(
                    geri_proto::Shape::Range {
                        min: 2.0,
                        max: 1.0,
                        step: None,
                        unit: None,
                    },
                    geri_proto::Danger::Safe,
                ))
            }
            7 => d.entities.push(d.entities[0].clone()),
            _ => unreachable!(),
        }
        cases.push(d);
    }
    cases
}

#[test]
fn invalid_source_declarations_are_rejected_at_construction_and_update() {
    for invalid in invalid_declarations() {
        let mut h = Hub::new(&["router"]);
        let expected_source = invalid.source_id.clone();
        assert!(
            matches!(h.s.declare(invalid.clone()), Err(Error::InvalidSource { source_id, .. }) if source_id == expected_source)
        );
        assert!(matches!(
            Session::new(Config::new(vec![invalid]), MemStore::new()),
            Err(Error::InvalidSource { .. })
        ));
        h.s.handle_connected(h.now, false);
        let hello = h.next();
        h.answer(&hello, welcome(VERSION));
        assert_eq!(h.next().msg, ToHub::Declare(decl("router")));
    }
    let config = Config::new(vec![decl("router"), decl("router")]);
    assert_eq!(
        config.validate(),
        Err(Error::DuplicateSource("router".into()))
    );
    assert!(matches!(
        Session::new(config, MemStore::new()),
        Err(Error::DuplicateSource(_))
    ));
}

#[test]
fn invalid_legacy_outbox_is_reported_and_retained_without_skipping_sequence() {
    let mut store = MemStore::new();
    let mut invalid = draft();
    invalid.data = None;
    let old = store.append(invalid, None).unwrap().unwrap();
    let mut h = Hub::with_store(&["router"], store);
    let next = h.s.state("router", "cpu", json!(2)).unwrap().unwrap();
    assert_eq!((old.sequence, next.sequence), (1, 2));
    h.connect(false);
    assert!(h.idle());
    assert_eq!(
        h.events(),
        [Event::Error(Error::InvalidOutboxRecord {
            source_id: "router".into(),
            record_id: "router-1".into(),
            reason: geri_proto::v4::validate::Invalid::PayloadCount,
        })]
    );
    assert_eq!(
        h.s.store_mut().peek(128).unwrap(),
        [old.clone(), next.clone()]
    );
    assert_eq!(h.s.poll_timeout(), Some(h.now + 1000));
    // Aynı anı poll etmek sınırsız hata kuyruğu yaratmaz.
    for _ in 0..10 {
        assert!(h.idle());
    }
    assert!(h.events().is_empty());
    h.at(h.now + 1000);
    assert!(h.idle());
    assert!(matches!(
        h.events().as_slice(),
        [Event::Error(Error::InvalidOutboxRecord { .. })]
    ));
    assert_eq!(h.s.store_mut().peek(128).unwrap(), [old, next]);
    assert_eq!(
        h.s.state("router", "cpu", json!(3))
            .unwrap()
            .unwrap()
            .sequence,
        3
    );
}

fn ok() -> ToSource {
    ToSource::Ok {
        watched: None,
        next_sequence: None,
    }
}

/// Sıradaki live zamanı cevaptan sonra kurulduğu için yavaş hub'da da sıra döner.
#[test]
fn live_takes_turns_when_answers_are_slower_than_pace() {
    let mut h = Hub::new(&["a", "b"]);
    h.s.live("a", values(1)).unwrap();
    h.s.live("b", values(2)).unwrap();
    h.connect(true);
    for source_id in ["a", "b"] {
        h.signal(ToSource::Pace {
            source_id: source_id.into(),
            ms: 1_000,
        });
    }
    let mut order = Vec::new();
    for _ in 0..4 {
        let live = h.next();
        let ToHub::Live { source_id, .. } = &live.msg else {
            panic!("{:?}", live.msg)
        };
        order.push(source_id.clone());
        h.now += 1_500;
        h.answer(&live, ok());
        h.at(h.now);
    }
    assert_eq!(order, ["a", "b", "a", "b"]);
}

#[test]
fn a_busy_source_does_not_starve_another_ones_claim() {
    let mut h = Hub::controlled(&["router", "other"]);
    h.connect(true);
    for _ in 0..2 {
        let claim = h.next();
        assert!(matches!(claim.msg, ToHub::Claim { .. }), "{:?}", claim.msg);
        h.answer(&claim, ToSource::Idle);
    }
    for source_id in ["router", "other"] {
        h.signal(ToSource::Ring {
            source_id: source_id.into(),
        });
    }
    let claim = h.next();
    assert_eq!(
        claim.msg,
        ToHub::Claim {
            source_id: "router".into()
        }
    );
    // Komut geldi: router hemen yeniden sormak istiyor ama sıra other'da.
    h.answer(&claim, ToSource::Command(command("c1")));
    let claim = h.next();
    assert_eq!(
        claim.msg,
        ToHub::Claim {
            source_id: "other".into()
        }
    );
    h.answer(&claim, ToSource::Idle);
    assert_eq!(
        h.next().msg,
        ToHub::Claim {
            source_id: "router".into()
        }
    );
}
