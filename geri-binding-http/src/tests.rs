use super::*;
use alloc::vec;
use geri_proto::v4::{
    ApiError, Batch, Command, CommandResult, Limits, Receipt, ReceiptStatus, Record, RecordKind,
    RecordReceipt, SourceDeclaration, Welcome,
};
use serde_json::json;

fn declare() -> ToHub {
    ToHub::Declare(SourceDeclaration {
        source_id: "router".into(),
        label: "Router".into(),
        kind: "openwrt".into(),
        entities: vec![],
    })
}

fn batch() -> ToHub {
    ToHub::Batch(Batch {
        v: VERSION,
        records: vec![Record {
            source_id: "router".into(),
            entity_id: "cpu".into(),
            record_id: "router-1".into(),
            sequence: 1,
            kind: RecordKind::State,
            event_time_ms: Some(123),
            content_type: "application/json".into(),
            data: Some(json!({"pct": 12.5, "note": null})),
            blob: None,
            indexed: None,
        }],
    })
}

fn live() -> ToHub {
    ToHub::Live {
        source_id: "router".into(),
        values: json!({"cpu": {"t": "gauge", "v": 37}})
            .as_object()
            .unwrap()
            .clone(),
    }
}

fn claim() -> ToHub {
    ToHub::Claim {
        source_id: "router".into(),
    }
}

fn result() -> ToHub {
    ToHub::Result {
        source_id: "router".into(),
        result: CommandResult {
            command_id: "c1".into(),
            ok: true,
            result: json!(null),
        },
    }
}

fn all_requests() -> Vec<ToHub> {
    vec![
        ToHub::Hello {
            v: VERSION,
            auth: None,
        },
        declare(),
        batch(),
        live(),
        claim(),
        result(),
    ]
}

#[test]
fn requests_use_todays_v4_paths() {
    let expected = [
        (Method::Get, "/v4/welcome", false),
        (Method::Put, "/v4/sources/router", true),
        (Method::Post, "/v4/batch", true),
        (Method::Put, "/v4/live/router", true),
        (Method::Get, "/v4/commands/claim?source=router", false),
        (Method::Post, "/v4/commands/result?source=router", true),
    ];
    for (msg, (method, path, has_body)) in all_requests().iter().zip(expected) {
        let req = client::request(msg, Codec::Json).unwrap();
        assert_eq!((req.method, req.path.as_str()), (method, path));
        assert_eq!(!req.body.is_empty(), has_body, "{path}");
    }
}

#[test]
fn every_request_survives_client_to_hub_in_both_codecs() {
    for codec in [Codec::Json, Codec::Cbor] {
        for msg in all_requests() {
            let req = client::request(&msg, codec).unwrap();
            let got = hub::request(
                req.method,
                &req.path,
                Some(req.codec.content_type()),
                &req.body,
            )
            .unwrap();
            assert_eq!(got, msg, "{codec:?} {}", req.path);
        }
    }
}

#[test]
fn every_answer_survives_hub_to_client_in_both_codecs() {
    let command = Command {
        command_id: "c1".into(),
        source_id: "router".into(),
        entity_id: "wan".into(),
        action: "press".into(),
        args: json!(null),
        issued_by: "can".into(),
        issued_at_ms: 1,
    };
    let pairs = vec![
        (
            ToHub::Hello {
                v: VERSION,
                auth: None,
            },
            ToSource::Welcome(Welcome {
                v: VERSION,
                agent_id: "router".into(),
                limits: Limits {
                    max_batch_records: 128,
                    max_inline_bytes: 65536,
                    max_body_bytes: 1 << 20,
                    max_blob_bytes: 1 << 24,
                    max_webhook_bytes: 65536,
                },
            }),
        ),
        (
            declare(),
            ToSource::Ok {
                watched: None,
                next_sequence: Some(5001),
            },
        ),
        (
            batch(),
            ToSource::Receipt(Receipt {
                v: VERSION,
                records: vec![RecordReceipt {
                    source_id: "router".into(),
                    record_id: "router-1".into(),
                    status: ReceiptStatus::Accepted,
                    cursor: Some(7),
                    expected_sequence: None,
                }],
            }),
        ),
        (
            live(),
            ToSource::Ok {
                watched: Some(true),
                next_sequence: None,
            },
        ),
        (
            live(),
            ToSource::Ok {
                watched: Some(false),
                next_sequence: None,
            },
        ),
        (claim(), ToSource::Command(command)),
        (claim(), ToSource::Idle),
        (
            result(),
            ToSource::Ok {
                watched: None,
                next_sequence: None,
            },
        ),
        (
            batch(),
            ToSource::Error {
                status: 422,
                error: ApiError {
                    code: "missing_blob".into(),
                    message: "upload it".into(),
                    earliest_cursor: None,
                    counter: None,
                },
            },
        ),
    ];
    for codec in [Codec::Json, Codec::Cbor] {
        for (sent, answer) in &pairs {
            let res = hub::response(answer, codec).unwrap();
            let content_type = (!res.body.is_empty()).then(|| res.codec.content_type());
            let got = client::response(sent, res.status, content_type, &res.body).unwrap();
            assert_eq!(&got, answer, "{codec:?}");
        }
    }
}

#[test]
fn statuses_match_todays_hub() {
    let status = |msg| hub::response(&msg, Codec::Json).unwrap().status;
    assert_eq!(status(ToSource::Idle), 204);
    assert_eq!(
        status(ToSource::Ok {
            watched: None,
            next_sequence: None
        }),
        204
    );
    assert_eq!(
        status(ToSource::Ok {
            watched: Some(false),
            next_sequence: None,
        }),
        200
    );
    let body = hub::response(
        &ToSource::Ok {
            watched: Some(true),
            next_sequence: None,
        },
        Codec::Json,
    )
    .unwrap();
    assert_eq!(body.body, br#"{"watched":true}"#);
}

#[test]
fn declare_reply_carries_the_next_sequence() {
    let res = hub::response(
        &ToSource::Ok {
            watched: None,
            next_sequence: Some(5001),
        },
        Codec::Json,
    )
    .unwrap();
    assert_eq!(
        (res.status, res.body.as_slice()),
        (200, &br#"{"next_sequence":5001}"#[..])
    );
    assert_eq!(
        client::response(&declare(), 204, None, b"").unwrap(),
        ToSource::Ok {
            watched: None,
            next_sequence: None,
        }
    );
}

#[test]
fn an_error_without_a_readable_body_keeps_its_status() {
    let got = client::response(&batch(), 507, Some("text/plain"), b"disk on fire").unwrap();
    assert_eq!(
        got,
        ToSource::Error {
            status: 507,
            error: ApiError {
                code: "unknown".into(),
                message: "disk on fire".into(),
                earliest_cursor: None,
                counter: None,
            },
        }
    );
}

#[test]
fn identifiers_are_checked_in_both_directions() {
    let bad = ToHub::Claim {
        source_id: "../etc".into(),
    };
    assert_eq!(client::request(&bad, Codec::Json), Err(Error::InvalidId));
    for path in [
        "/v4/commands/claim",
        "/v4/commands/claim?source=",
        "/v4/commands/claim?source=a%2Fb",
        "/v4/commands/claim?other=router",
    ] {
        assert_eq!(
            hub::request(Method::Get, path, None, b""),
            Err(Error::InvalidId),
            "{path}"
        );
    }
    let query = hub::request(
        Method::Get,
        "/v4/commands/claim?x=1&source=router",
        None,
        b"",
    );
    assert_eq!(query, Ok(claim()));
}

#[test]
fn declaration_path_and_body_must_agree() {
    let req = client::request(&declare(), Codec::Json).unwrap();
    assert_eq!(
        hub::request(Method::Put, "/v4/sources/other", None, &req.body),
        Err(Error::PathMismatch)
    );
}

#[test]
fn other_requests_are_left_to_the_hub_router() {
    for (method, path) in [
        (Method::Put, "/v4/sources/router/visibility"),
        (Method::Get, "/v4/sources"),
        (Method::Get, "/v4/records?source=router"),
        (Method::Post, "/v4/welcome"),
        (Method::Put, "/v4/blobs/abc"),
        (Method::Post, "/v4/commands"),
    ] {
        assert_eq!(
            hub::request(method, path, None, b"{}"),
            Err(Error::NotAMessage),
            "{path}"
        );
    }
}

#[test]
fn keepalive_and_signals_have_no_http_form() {
    assert_eq!(
        client::request(&ToHub::Ping, Codec::Json),
        Err(Error::NoHttpForm)
    );
    for msg in [
        ToSource::Pong,
        ToSource::Ring {
            source_id: "router".into(),
        },
        ToSource::Pace {
            source_id: "router".into(),
            ms: 1000,
        },
    ] {
        assert_eq!(hub::response(&msg, Codec::Json), Err(Error::NoHttpForm));
    }
}

#[test]
fn cbor_bodies_keep_json_null() {
    let req = client::request(&result(), Codec::Cbor).unwrap();
    assert!(req.body.contains(&0xf6), "{:02x?}", req.body);
}

#[test]
fn content_type_parameters_and_absence_are_tolerated() {
    assert_eq!(Codec::from_content_type(None), Codec::Json);
    assert_eq!(
        Codec::from_content_type(Some("application/json; charset=utf-8")),
        Codec::Json
    );
    assert_eq!(
        Codec::from_content_type(Some("application/cbor")),
        Codec::Cbor
    );
    assert_eq!(Method::parse("DELETE"), None);
    assert_eq!(Method::parse(Method::Put.as_str()), Some(Method::Put));
}

#[test]
fn signals_ride_on_any_answer_as_a_header() {
    let signals = vec![
        ToSource::Ring {
            source_id: "router".into(),
        },
        ToSource::Pace {
            source_id: "router".into(),
            ms: 1000,
        },
    ];
    let header = hub::signals(&signals).unwrap();
    assert_eq!(
        header,
        r#"[{"t":"ring","source_id":"router"},{"t":"pace","source_id":"router","ms":1000}]"#
    );
    assert_eq!(client::signals(Some(&header)), signals);
    assert_eq!(hub::signals(&[]), None);
}

#[test]
fn broken_or_foreign_signal_headers_are_ignored() {
    assert!(client::signals(None).is_empty());
    assert!(client::signals(Some("ring")).is_empty());
    assert!(client::signals(Some(r#"{"t":"ring","source_id":"router"}"#)).is_empty());
    let mixed = r#"[{"t":"idle"},{"t":"resync_maybe_later"},{"t":"ring","source_id":"a"}]"#;
    assert_eq!(
        client::signals(Some(mixed)),
        vec![ToSource::Ring {
            source_id: "a".into()
        }]
    );
}

#[test]
fn long_poll_is_a_get_with_a_capped_wait() {
    let req = client::wait(10_000);
    assert_eq!(
        (req.method, req.path.as_str()),
        (Method::Get, "/v4/signals?wait=10000")
    );
    assert_eq!(client::wait(u32::MAX).path, "/v4/signals?wait=25000");
    assert_eq!(hub::wait(Method::Get, &req.path), Some(10_000));
    assert_eq!(
        hub::wait(Method::Get, "/v4/signals?wait=99999999"),
        Some(MAX_WAIT_MS)
    );
    assert_eq!(hub::wait(Method::Get, "/v4/signals"), Some(MAX_WAIT_MS));
    assert_eq!(
        hub::wait(Method::Get, "/v4/signals?wait=x"),
        Some(MAX_WAIT_MS)
    );
    assert_eq!(hub::wait(Method::Post, "/v4/signals"), None);
    assert_eq!(hub::wait(Method::Get, "/v4/signals/x"), None);
    assert_eq!(
        hub::request(Method::Get, "/v4/signals?wait=1", None, b""),
        Err(Error::NotAMessage)
    );
}
