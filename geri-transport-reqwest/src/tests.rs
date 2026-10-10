use super::*;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use geri_adapter_sdk::{Config, Event, MemStore, Session, Store};
use geri_binding_http::hub;
use geri_proto::v4::{
    ApiError, Batch, Command, CommandResult, Limits, Receipt, ReceiptStatus, RecordKind,
    RecordReceipt, SourceDeclaration, VERSION, Welcome,
};
use serde_json::json;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

const TOKEN: &str = "secret-token-123456";
const SEED: [u8; 32] = [7; 32];

#[derive(Default)]
struct Mock {
    blobs: HashSet<String>,
    batches: Vec<Batch>,
    commands: VecDeque<Command>,
    results: Vec<(String, CommandResult)>,
    live: usize,
    piggyback: bool,
    long_poll: bool,
    waits: usize,
    window: geri_sign::Window,
    stale: usize,
}

type Shared = Arc<Mutex<Mock>>;

async fn handle(
    State(mock): State<Shared>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let accept = Codec::from_content_type(header("accept"));
    let answer = |msg: ToSource| {
        let res = hub::response(&msg, accept).unwrap();
        let status = StatusCode::from_u16(res.status).unwrap();
        (
            status,
            [(header::CONTENT_TYPE, accept.content_type())],
            res.body,
        )
            .into_response()
    };
    let target = uri.path_and_query().unwrap().as_str();
    let authorized = match header("authorization").and_then(geri_sign::Authorization::parse) {
        Some(signed) => {
            let key = geri_sign::public_key(&SEED);
            if signed.agent != "router" || !signed.verify(&key, method.as_str(), target, &body) {
                false
            } else {
                let mut mock = mock.lock().unwrap();
                if !mock.window.accept(signed.n) {
                    mock.stale += 1;
                    let counter = mock.window.max.to_string();
                    return (
                        StatusCode::UNAUTHORIZED,
                        [(geri_sign::COUNTER_HEADER, counter)],
                    )
                        .into_response();
                }
                true
            }
        }
        None => {
            header("authorization") == Some(&format!("Bearer {TOKEN}"))
                && header("x-geri-agent") == Some("router")
        }
    };
    if !authorized {
        return answer(ToSource::Error {
            status: 401,
            error: ApiError {
                code: "unauthorized".into(),
                message: "who are you".into(),
                earliest_cursor: None,
                counter: None,
            },
        });
    }
    if let Some(ms) = hub::wait(Method::Get, uri.path_and_query().unwrap().as_str()) {
        return long_poll(mock, ms).await;
    }
    let mut mock = mock.lock().unwrap();
    if let Some(sha) = uri.path().strip_prefix("/v4/blobs/") {
        mock.blobs.insert(sha.into());
        return StatusCode::NO_CONTENT.into_response();
    }
    let path = uri.path_and_query().map_or("/", |p| p.as_str());
    let method = Method::parse(method.as_str()).unwrap();
    let msg = hub::request(method, path, header("content-type"), &body).unwrap();
    let reply = match msg {
        ToHub::Hello { .. } => ToSource::Welcome(Welcome {
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
        ToHub::Declare(_) => ToSource::Ok {
            watched: None,
            next_sequence: None,
        },
        ToHub::Batch(batch) => {
            let missing = batch
                .records
                .iter()
                .filter_map(|r| r.blob.as_ref())
                .any(|b| !mock.blobs.contains(&b.sha256));
            if missing {
                ToSource::Error {
                    status: 422,
                    error: ApiError {
                        code: "missing_blob".into(),
                        message: "upload first".into(),
                        earliest_cursor: None,
                        counter: None,
                    },
                }
            } else {
                let records = batch
                    .records
                    .iter()
                    .enumerate()
                    .map(|(i, r)| RecordReceipt {
                        source_id: r.source_id.clone(),
                        record_id: r.record_id.clone(),
                        status: ReceiptStatus::Accepted,
                        cursor: Some(100 + i as u64),
                        expected_sequence: None,
                    })
                    .collect();
                mock.batches.push(batch);
                ToSource::Receipt(Receipt {
                    v: VERSION,
                    records,
                })
            }
        }
        ToHub::Live { .. } => {
            mock.live += 1;
            ToSource::Ok {
                watched: Some(true),
                next_sequence: None,
            }
        }
        ToHub::Claim { .. } => match mock.commands.pop_front() {
            Some(command) => ToSource::Command(command),
            None => ToSource::Idle,
        },
        ToHub::Result { source_id, result } => {
            mock.results.push((source_id, result));
            ToSource::Ok {
                watched: None,
                next_sequence: None,
            }
        }
        ToHub::Ping | ToHub::Unknown => unreachable!(),
    };
    let mut res = answer(reply);
    if mock.piggyback && !mock.commands.is_empty() {
        let ring = [ToSource::Ring {
            source_id: "router".into(),
        }];
        let value = hub::signals(&ring).unwrap().parse().unwrap();
        res.headers_mut().insert(SIGNALS, value);
    }
    res
}

async fn long_poll(mock: Shared, ms: u32) -> Response {
    {
        let mut mock = mock.lock().unwrap();
        mock.waits += 1;
        if !mock.long_poll {
            return StatusCode::NOT_FOUND.into_response();
        }
    }
    let until = tokio::time::Instant::now() + std::time::Duration::from_millis(ms.into());
    let mut res = StatusCode::NO_CONTENT.into_response();
    while tokio::time::Instant::now() < until {
        if !mock.lock().unwrap().commands.is_empty() {
            let ring = [ToSource::Ring {
                source_id: "router".into(),
            }];
            let value = hub::signals(&ring).unwrap().parse().unwrap();
            res.headers_mut().insert(SIGNALS, value);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    res
}

async fn serve(mock: Shared) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(handle).with_state(mock);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

fn session() -> Session<MemStore> {
    let decl = SourceDeclaration {
        source_id: "router".into(),
        label: "Router".into(),
        kind: "openwrt".into(),
        entities: vec![
            geri_proto::v4::Entity::new("router", "wan")
                .control(geri_proto::Shape::Trigger, geri_proto::Danger::Safe),
        ],
    };
    Session::new(Config::new(vec![decl]), MemStore::new()).unwrap()
}

async fn pump(s: &mut Session<MemStore>, t: &mut HttpTransport, now: u64) -> Vec<Event> {
    while let Some(frame) = s.poll_transmit(now) {
        t.send(frame).await.unwrap();
        while let Some(answer) = t.answers.pop_front() {
            s.handle_frame(now, answer);
        }
    }
    std::iter::from_fn(|| s.poll_event()).collect()
}

#[tokio::test]
async fn a_session_talks_to_a_hub_over_http_in_both_codecs() {
    for (codec, signed) in [
        (Codec::Json, false),
        (Codec::Cbor, false),
        (Codec::Json, true),
        (Codec::Cbor, true),
    ] {
        let mock = Shared::default();
        mock.lock().unwrap().window = geri_sign::Window {
            max: 1000,
            seen: u64::MAX,
        };
        mock.lock().unwrap().commands.push_back(Command {
            command_id: "c1".into(),
            source_id: "router".into(),
            entity_id: "wan".into(),
            action: "press".into(),
            args: json!(null),
            issued_by: "can".into(),
            issued_at_ms: 1,
        });
        let mut t =
            HttpTransport::new(serve(mock.clone()).await + "/", TOKEN, "router").with_codec(codec);
        if signed {
            let signer = geri_sign::Signer::new("router", SEED).unwrap();
            t = t.with_signer(Arc::new(signer));
        }
        let mut s = session();
        s.state("router", "cpu", json!({"pct": 12, "note": null}))
            .unwrap();
        let blob = s
            .blob(
                "router",
                "log",
                Some("log-1"),
                RecordKind::Event,
                None,
                "text/plain",
                b"hello",
                json!({}),
            )
            .unwrap()
            .unwrap()
            .blob
            .unwrap();
        s.live("router", json!({"cpu": 37}).as_object().unwrap().clone())
            .unwrap();

        assert!(!t.pushes());
        t.connect().await.unwrap();
        s.handle_connected(0, t.pushes());
        let events = pump(&mut s, &mut t, 0).await;
        assert_eq!(events[0], Event::Connected { push: false }, "{codec:?}");
        assert!(events.contains(&Event::UploadBlobs(vec![blob.clone()])));
        let command = events
            .iter()
            .find_map(|e| match e {
                Event::Command(c) => Some(c.clone()),
                _ => None,
            })
            .unwrap();
        assert!(events.contains(&Event::Watched {
            source_id: "router".into(),
            watched: true
        }));

        let bytes = s.store_mut().blob(&blob.sha256).unwrap().unwrap();
        t.put_blob(&blob.sha256, bytes).await.unwrap();
        s.blobs_uploaded(0);
        s.result(&command, true, json!({"done": true}));
        assert!(pump(&mut s, &mut t, 0).await.is_empty());

        assert_eq!(s.store_mut().pending().unwrap(), 0);
        let mock = mock.lock().unwrap();
        assert_eq!(mock.batches.len(), 1);
        assert_eq!(
            mock.batches[0].records[0].data,
            Some(json!({"pct": 12, "note": null}))
        );
        assert_eq!(mock.results[0].0, "router");
        assert_eq!(mock.results[0].1.command_id, "c1");
        assert_eq!(mock.live, 1);
        assert_eq!(mock.stale, usize::from(signed), "{codec:?}");
        assert!(!signed || mock.window.max > 1000);
    }
}

#[tokio::test]
async fn a_piggybacked_ring_claims_without_waiting_for_the_poll() {
    let mock = Shared::default();
    mock.lock().unwrap().piggyback = true;
    let mut t = HttpTransport::new(serve(mock.clone()).await, TOKEN, "router");
    let mut s = session();
    s.live("router", json!({"cpu": 1}).as_object().unwrap().clone())
        .unwrap();
    t.connect().await.unwrap();
    s.handle_connected(0, t.pushes());
    pump(&mut s, &mut t, 0).await;

    mock.lock().unwrap().commands.push_back(Command {
        command_id: "c1".into(),
        source_id: "router".into(),
        entity_id: "wan".into(),
        action: "press".into(),
        args: json!(null),
        issued_by: "can".into(),
        issued_at_ms: 1,
    });
    let events = pump(&mut s, &mut t, 2_000).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Command(c) if c.command_id == "c1")),
        "{events:?}"
    );
    assert!(mock.lock().unwrap().commands.is_empty());
    assert!(pump(&mut s, &mut t, 2_000).await.is_empty());
}

fn command() -> Command {
    Command {
        command_id: "c1".into(),
        source_id: "router".into(),
        entity_id: "wan".into(),
        action: "press".into(),
        args: json!(null),
        issued_by: "can".into(),
        issued_at_ms: 1,
    }
}

#[tokio::test]
async fn a_long_poll_brings_a_command_without_waiting_for_the_poll() {
    let mock = Shared::default();
    mock.lock().unwrap().long_poll = true;
    let mut t = HttpTransport::new(serve(mock.clone()).await, TOKEN, "router");
    let mut s = session();
    t.connect().await.unwrap();
    s.handle_connected(0, t.pushes());
    pump(&mut s, &mut t, 0).await;

    let started = std::time::Instant::now();
    let later = mock.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        later.lock().unwrap().commands.push_back(command());
    });
    let frame = tokio::time::timeout(std::time::Duration::from_secs(3), t.recv())
        .await
        .expect("long-poll komutu getirmedi")
        .unwrap();
    assert_eq!(
        frame,
        Frame {
            id: None,
            msg: ToSource::Ring {
                source_id: "router".into()
            }
        }
    );
    s.handle_frame(0, frame);
    let events = pump(&mut s, &mut t, 0).await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Command(c) if c.command_id == "c1"))
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn an_old_hub_without_long_poll_is_asked_once() {
    let mock = Shared::default();
    let mut t = HttpTransport::new(serve(mock.clone()).await, TOKEN, "router");
    t.connect().await.unwrap();
    for _ in 0..3 {
        let waited = tokio::time::timeout(std::time::Duration::from_millis(300), t.recv()).await;
        assert!(waited.is_err(), "404'ten sonra recv sessiz kalmalı");
    }
    assert_eq!(mock.lock().unwrap().waits, 1);
    t.connect().await.unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_millis(300), t.recv()).await;
    assert_eq!(mock.lock().unwrap().waits, 2);
}

#[tokio::test]
async fn a_wrong_token_is_a_hub_answer_not_a_broken_connection() {
    let mut t = HttpTransport::new(serve(Shared::default()).await, "wrong", "router");
    let mut s = session();
    s.handle_connected(0, false);
    let events = pump(&mut s, &mut t, 0).await;
    assert!(
        matches!(&events[..], [Event::Error(geri_adapter_sdk::Error::Hub { status: 401, code, .. })] if code == "unauthorized"),
        "{events:?}"
    );
}

#[tokio::test]
async fn an_unreachable_hub_is_a_transport_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut t = HttpTransport::new(url, TOKEN, "router");
    let mut s = session();
    s.handle_connected(0, false);
    let frame = s.poll_transmit(0).unwrap();
    assert!(matches!(t.send(frame).await, Err(Error::Http(_))));
}

#[tokio::test]
async fn keepalive_has_no_http_form() {
    let mut t = HttpTransport::new("http://127.0.0.1:1", TOKEN, "router");
    let ping = Frame {
        id: Some(1),
        msg: ToHub::Ping,
    };
    assert!(matches!(
        t.send(ping).await,
        Err(Error::Binding(geri_binding_http::Error::NoHttpForm))
    ));
}

#[tokio::test]
#[ignore = "needs a running geri-hubd"]
async fn against_a_real_hubd() {
    let var = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let (url, token, operator) = (
        var("GERI_TEST_HUB"),
        var("GERI_TEST_TOKEN"),
        var("GERI_TEST_OPERATOR"),
    );
    let decl = SourceDeclaration {
        source_id: "router".into(),
        label: "Router".into(),
        kind: "openwrt".into(),
        entities: vec![geri_proto::v4::Entity {
            group: None,
            source_id: "router".into(),
            entity_id: "wan".into(),
            label: "WAN".into(),
            kind: "switch".into(),
            control: Some(geri_proto::Control {
                shape: geri_proto::Shape::Trigger,
                danger: Default::default(),
            }),
        }],
    };
    let mut s = Session::new(Config::new(vec![decl]), MemStore::new()).unwrap();
    let mut t = HttpTransport::new(&url, &token, "router").with_codec(Codec::Cbor);
    for i in 0..3 {
        s.state("router", "cpu", json!({"pct": i, "note": null}))
            .unwrap();
    }
    s.handle_connected(0, false);
    let events = pump(&mut s, &mut t, 0).await;
    assert_eq!(events, [Event::Connected { push: false }], "{events:?}");
    assert_eq!(s.store_mut().pending().unwrap(), 0);

    let issued = reqwest::Client::new()
        .post(format!("{url}/v4/commands"))
        .bearer_auth(&operator)
        .header("X-Geri-Agent", "operator")
        .header("content-type", "application/json")
        .body(r#"{"source_id":"router","entity_id":"wan","action":"press","args":null}"#)
        .send()
        .await
        .unwrap();
    assert!(
        issued.status().is_success(),
        "{}",
        issued.text().await.unwrap()
    );
    let events = pump(&mut s, &mut t, 10_000).await;
    let Some(Event::Command(command)) = events.first() else {
        panic!("{events:?}")
    };
    s.result(command, true, json!(null));
    assert_eq!(pump(&mut s, &mut t, 10_000).await, []);
}
