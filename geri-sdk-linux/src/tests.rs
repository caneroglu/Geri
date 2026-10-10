use super::*;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use geri_binding_http::{Method, hub};
use geri_proto::v4::{
    Limits, Receipt, ReceiptStatus, RecordReceipt, VERSION, Welcome,
    msg::{Frame, ToHub, ToSource},
};
use serde_json::json;
use std::collections::VecDeque;
use tokio::time::timeout;

#[derive(Default)]
struct Mock {
    ws_attempts: usize,
    records: Vec<Record>,
    commands: VecDeque<Command>,
    results: Vec<String>,
    live: Vec<Map<String, Value>>,
    down: bool,
}

type Shared = Arc<Mutex<Mock>>;

async fn handle(
    State(mock): State<Shared>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut mock = mock.lock().unwrap();
    if mock.down {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let path = uri.path_and_query().map_or("/", |p| p.as_str());
    if path == "/v4/ws" {
        mock.ws_attempts += 1;
    }
    let Ok(msg) = hub::request(
        Method::parse(method.as_str()).unwrap(),
        path,
        content_type,
        &body,
    ) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let answer = match msg {
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
        ToHub::Declare(_) | ToHub::Result { .. } => {
            if let ToHub::Result { result, .. } = msg {
                mock.results.push(result.command_id);
            }
            ToSource::Ok {
                watched: None,
                next_sequence: None,
            }
        }
        ToHub::Batch(batch) => {
            let records = batch
                .records
                .iter()
                .map(|r| RecordReceipt {
                    source_id: r.source_id.clone(),
                    record_id: r.record_id.clone(),
                    status: ReceiptStatus::Accepted,
                    cursor: Some(1),
                    expected_sequence: None,
                })
                .collect();
            mock.records.extend(batch.records);
            ToSource::Receipt(Receipt {
                v: VERSION,
                records,
            })
        }
        ToHub::Live { values, .. } => {
            mock.live.push(values);
            ToSource::Ok {
                watched: Some(true),
                next_sequence: None,
            }
        }
        ToHub::Claim { .. } => match mock.commands.pop_front() {
            Some(c) => ToSource::Command(c),
            None => ToSource::Idle,
        },
        ToHub::Ping | ToHub::Unknown => unreachable!(),
    };
    let res = hub::response(&answer, Codec::Json).unwrap();
    (StatusCode::from_u16(res.status).unwrap(), res.body).into_response()
}

async fn serve(mock: Shared) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(handle).with_state(mock);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

fn router() -> SourceDeclaration {
    SourceDeclaration {
        source_id: "router".into(),
        label: "Router".into(),
        kind: "openwrt".into(),
        entities: vec![Entity {
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
    }
}

async fn until(mock: &Shared, what: &str, f: impl Fn(&Mock) -> bool) {
    timeout(Duration::from_secs(5), async {
        while !f(&mock.lock().unwrap()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn next_command(events: &mut mpsc::Receiver<Event>) -> Command {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Event::Command(c)) = events.recv().await {
                return c;
            }
        }
    })
    .await
    .expect("no command arrived")
}

#[tokio::test]
async fn records_commands_and_live_flow_in_the_background() {
    let mock = Shared::default();
    let url = serve(mock.clone()).await;
    let (geri, mut events) = builder(url, "token", "router")
        .source(router())
        .configure(|c| {
            c.claim_every_ms = 50;
            c.live_watched_ms = 50;
            c.live_idle_ms = 50;
        })
        .start()
        .unwrap();

    geri.state("router", "cpu", json!({"pct": 1})).unwrap();
    geri.live("router", json!({"cpu": 1}).as_object().unwrap().clone())
        .unwrap();
    until(&mock, "the record", |m| m.records.len() == 1).await;
    until(&mock, "live values", |m| !m.live.is_empty()).await;
    assert_eq!(geri.pending().unwrap(), 0);

    mock.lock().unwrap().commands.push_back(Command {
        command_id: "c1".into(),
        source_id: "router".into(),
        entity_id: "wan".into(),
        action: "press".into(),
        args: Value::Null,
        issued_by: "can".into(),
        issued_at_ms: 1,
    });
    let command = next_command(&mut events).await;
    geri.result(&command, true, Value::Null);
    until(&mock, "the result", |m| m.results == ["c1"]).await;

    assert_eq!(
        geri.state("nope", "cpu", json!(1)),
        Err(Error::UnknownSource("nope".into()))
    );
}

#[tokio::test]
async fn records_wait_in_the_outbox_while_the_hub_is_down() {
    let mock = Shared::default();
    mock.lock().unwrap().down = true;
    let url = serve(mock.clone()).await;
    let dir = std::env::temp_dir().join(format!("geri-sdk-linux-{}", std::process::id()));
    let path = dir.join("outbox.sqlite");
    let (geri, _events) = builder(url, "token", "router")
        .source(router())
        .outbox(&path)
        .configure(|c| {
            c.backoff_min_ms = 20;
            c.backoff_max_ms = 50;
        })
        .start()
        .unwrap();
    for i in 0..3 {
        geri.state("router", "cpu", json!(i)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(geri.pending().unwrap(), 3);
    mock.lock().unwrap().down = false;
    until(&mock, "all records", |m| m.records.len() == 3).await;
    let sequences: Vec<u64> = mock
        .lock()
        .unwrap()
        .records
        .iter()
        .map(|r| r.sequence)
        .collect();
    assert_eq!(sequences, [1, 2, 3]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn start_needs_a_runtime() {
    assert!(matches!(
        builder("http://x", "t", "a").start(),
        Err(StartError::NoRuntime)
    ));
}

async fn wait_for(events: &mut mpsc::Receiver<Event>, what: &str, f: fn(&Event) -> bool) -> Event {
    let found = timeout(Duration::from_secs(5), async {
        loop {
            match events.recv().await.unwrap() {
                e if f(&e) => return e,
                Event::Error(e) => panic!("{e}"),
                _ => {}
            }
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

#[tokio::test]
#[ignore = "needs a running geri-hubd"]
async fn against_a_real_hubd_over_websocket() {
    use futures_util::StreamExt;
    let var = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let (url, token, operator) = (
        var("GERI_TEST_HUB"),
        var("GERI_TEST_TOKEN"),
        var("GERI_TEST_OPERATOR"),
    );
    let decl = SourceDeclaration {
        entities: vec![Entity {
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
        ..router()
    };
    let (geri, mut events) = builder(&url, &token, "router")
        .source(decl)
        .link(Link::WebSocket)
        .codec(Codec::Cbor)
        .start()
        .unwrap();
    geri.state("router", "wan", json!({"up": true})).unwrap();
    wait_for(&mut events, "connection", |e| {
        *e == Event::Connected { push: true }
    })
    .await;

    let http = reqwest::Client::new();
    let viewer = http
        .get(format!("{url}/v4/live/router"))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap();
    assert!(viewer.status().is_success());
    wait_for(&mut events, "watched", |e| {
        matches!(e, Event::Watched { watched: true, .. })
    })
    .await;
    geri.live("router", json!({"cpu": 77}).as_object().unwrap().clone())
        .unwrap();
    let mut stream = viewer.bytes_stream();
    let chunk = timeout(Duration::from_secs(5), async {
        loop {
            let bytes = stream.next().await.unwrap().unwrap();
            let text = String::from_utf8_lossy(&bytes).to_string();
            if text.contains("event: live") {
                return text;
            }
        }
    })
    .await
    .expect("viewer got no live values");
    assert!(chunk.contains(r#""cpu":77"#), "{chunk}");
    drop(stream);
    wait_for(&mut events, "unwatched", |e| {
        matches!(e, Event::Watched { watched: false, .. })
    })
    .await;

    let issued_at = Instant::now();
    let issued = http
        .post(format!("{url}/v4/commands"))
        .bearer_auth(&operator)
        .header("content-type", "application/json")
        .body(r#"{"source_id":"router","entity_id":"wan","action":"press","args":null}"#)
        .send()
        .await
        .unwrap();
    assert!(issued.status().is_success());
    let Event::Command(command) =
        wait_for(&mut events, "command", |e| matches!(e, Event::Command(_))).await
    else {
        unreachable!()
    };
    let latency = issued_at.elapsed();
    println!("command latency over websocket: {latency:?}");
    assert!(latency < Duration::from_millis(500), "{latency:?}");
    geri.result(&command, true, Value::Null);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let audit: Value = http
        .get(format!("{url}/v4/commands"))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap_or_default();
    assert_eq!(audit[0]["status"], "done", "{audit}");
    assert_eq!(geri.pending().unwrap(), 0);
}

#[tokio::test]
async fn auto_retries_websocket_while_http_is_healthy() {
    let mock = Shared::default();
    let url = serve(mock.clone()).await;
    let ws = WsTransport::new(&url, "token", "router").unwrap();
    let http = HttpTransport::new(&url, "token", "router");
    let transport =
        AutoTransport::new(ws, http, Link::Auto).with_ws_retry(Duration::from_millis(100));
    let session =
        Session::new(Config::new(vec![router()]), SqliteStore::memory().unwrap()).unwrap();
    let geri = Geri {
        session: Arc::new(Mutex::new(session)),
        wake: Arc::new(Notify::new()),
        epoch: watch::channel(0).0,
        driver_lock: Arc::new(tokio::sync::Mutex::new(())),
        started: Instant::now(),
    };
    let (events, mut rx) = mpsc::channel(EVENT_CAP);
    tokio::spawn(run(geri, transport, events));
    until(&mock, "websocket retries", |m| m.ws_attempts >= 3).await;
    let mut upgrades = 0;
    while let Ok(e) = rx.try_recv() {
        if matches!(&e, Event::Error(Error::Transport(m)) if m.contains("websocket")) {
            upgrades += 1;
        }
    }
    let attempts = mock.lock().unwrap().ws_attempts;
    assert!(
        upgrades <= attempts,
        "{upgrades} upgrades for {attempts} attempts"
    );
}

#[tokio::test]
async fn a_replaced_connection_falls_back_to_http_instead_of_fighting() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("http://{}", listener.local_addr().unwrap());
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: geri_transport_ws::CLOSE_REPLACED.into(),
                    reason: "replaced by a newer connection".into(),
                };
                let _ = ws.close(Some(frame)).await;
            });
        }
    });
    let mock = Shared::default();
    let http_url = serve(mock.clone()).await;
    let transport = AutoTransport::new(
        WsTransport::new(&ws_url, "token", "router").unwrap(),
        HttpTransport::new(&http_url, "token", "router"),
        Link::Auto,
    );
    let session =
        Session::new(Config::new(vec![router()]), SqliteStore::memory().unwrap()).unwrap();
    let geri = Geri {
        session: Arc::new(Mutex::new(session)),
        wake: Arc::new(Notify::new()),
        epoch: watch::channel(0).0,
        driver_lock: Arc::new(tokio::sync::Mutex::new(())),
        started: Instant::now(),
    };
    let (events, mut rx) = mpsc::channel(EVENT_CAP);
    tokio::spawn(run(geri.clone(), transport, events));
    geri.state("router", "cpu", json!(1)).unwrap();
    until(&mock, "the record over http", |m| m.records.len() == 1).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    let mut replaced = false;
    while let Ok(e) = rx.try_recv() {
        replaced |= matches!(&e, Event::Error(Error::Transport(m)) if m.contains("replaced"));
    }
    assert!(replaced, "the reason should reach the log");
}

#[tokio::test]
#[ignore = "needs a running geri-hubd"]
async fn against_a_real_hubd_over_http_long_poll() {
    let var = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let (url, token, operator) = (
        var("GERI_TEST_HUB"),
        var("GERI_TEST_TOKEN"),
        var("GERI_TEST_OPERATOR"),
    );
    let decl = SourceDeclaration {
        entities: vec![Entity {
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
        ..router()
    };
    let (geri, mut events) = builder(&url, &token, "router")
        .source(decl)
        .link(Link::Http)
        .start()
        .unwrap();
    wait_for(&mut events, "connection", |e| {
        *e == Event::Connected { push: false }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let issued_at = Instant::now();
    let issued = reqwest::Client::new()
        .post(format!("{url}/v4/commands"))
        .bearer_auth(&operator)
        .header("content-type", "application/json")
        .body(r#"{"source_id":"router","entity_id":"wan","action":"press","args":null}"#)
        .send()
        .await
        .unwrap();
    assert!(issued.status().is_success());
    let Event::Command(command) =
        wait_for(&mut events, "command", |e| matches!(e, Event::Command(_))).await
    else {
        unreachable!()
    };
    let latency = issued_at.elapsed();
    println!("command latency over http long-poll: {latency:?}");
    assert!(latency < Duration::from_secs(1), "{latency:?}");
    geri.result(&command, true, Value::Null);
}

#[tokio::test]
#[ignore = "needs a running geri-hubd"]
async fn against_a_real_hubd_with_unordered_records() {
    let var = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let (url, token, operator) = (
        var("GERI_TEST_HUB"),
        var("GERI_TEST_TOKEN"),
        var("GERI_TEST_OPERATOR"),
    );
    let (geri, mut events) = builder(&url, &token, "router")
        .source(router())
        .start()
        .unwrap();
    wait_for(&mut events, "connection", |e| {
        matches!(e, Event::Connected { .. })
    })
    .await;
    let kind = RecordKind::State;
    geri.unordered(
        "router",
        "cpu",
        "u-late",
        kind,
        Some(2_000),
        json!({"pct": 70}),
    )
    .unwrap();
    geri.unordered(
        "router",
        "cpu",
        "u-early",
        kind,
        Some(1_000),
        json!({"pct": 50}),
    )
    .unwrap();
    geri.state("router", "mem", json!({"pct": 1})).unwrap();
    timeout(Duration::from_secs(5), async {
        while geri.pending().unwrap() > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("outbox did not drain");
    geri.unordered(
        "router",
        "cpu",
        "u-late",
        kind,
        Some(2_000),
        json!({"pct": 70}),
    )
    .unwrap();
    timeout(Duration::from_secs(5), async {
        while geri.pending().unwrap() > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("duplicate did not drain");

    let state: Value = reqwest::Client::new()
        .get(format!("{url}/v4/state/router"))
        .bearer_auth(&operator)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let cpu = state
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["entity_id"] == "cpu")
        .unwrap_or_else(|| panic!("{state}"));
    assert_eq!(cpu["record_id"], "u-late", "{state}");
    assert_eq!(cpu["sequence"], 0);
}

#[test]
#[ignore = "needs a running geri-hubd"]
fn against_a_real_hubd_with_a_key() {
    let var = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let (url, key, operator) = (
        var("GERI_TEST_HUB"),
        var("GERI_TEST_KEY"),
        var("GERI_TEST_OPERATOR"),
    );
    let decl = SourceDeclaration {
        entities: vec![Entity {
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
        ..router()
    };
    for link in [Link::WebSocket, Link::Http, Link::WebSocket] {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (geri, mut events) = builder(&url, "", "router")
                .key(&key)
                .source(decl.clone())
                .link(link)
                .start()
                .unwrap();
            wait_for(&mut events, "connection", |e| {
                matches!(e, Event::Connected { .. })
            })
            .await;
            geri.state("router", "wan", json!({"up": true})).unwrap();
            timeout(Duration::from_secs(5), async {
                while geri.pending().unwrap() > 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("outbox did not drain");
            tokio::time::sleep(Duration::from_millis(300)).await;
            let issued = reqwest::Client::new()
                .post(format!("{url}/v4/commands"))
                .bearer_auth(&operator)
                .header("content-type", "application/json")
                .body(r#"{"source_id":"router","entity_id":"wan","action":"press","args":null}"#)
                .send()
                .await
                .unwrap();
            assert!(issued.status().is_success());
            geri.state("router", "wan", json!({"up": false})).unwrap();
            let Event::Command(command) =
                wait_for(&mut events, "command", |e| matches!(e, Event::Command(_))).await
            else {
                unreachable!()
            };
            geri.result(&command, true, Value::Null);
            tokio::time::sleep(Duration::from_millis(200)).await;
            println!("{link:?}: signed and answered");
        });
    }
}

#[test]
fn a_bad_key_file_stops_the_start() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let dir = std::env::temp_dir().join(format!("geri-key-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let missing = builder("http://127.0.0.1:9", "", "router")
        .key(dir.join("none"))
        .start();
    assert!(matches!(missing, Err(StartError::KeyFile { .. })));
    std::fs::write(dir.join("bad"), "zz").unwrap();
    let bad = builder("http://127.0.0.1:9", "", "router")
        .key(dir.join("bad"))
        .start();
    assert!(matches!(bad, Err(StartError::Key(geri_sign::Error::Key))));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
#[ignore = "needs a running geri-hubd with tcp listeners"]
fn against_a_real_hubd_over_tcp_and_quic() {
    let var = |name| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let (url, operator, key) = (
        var("GERI_TEST_HUB"),
        var("GERI_TEST_OPERATOR"),
        var("GERI_TEST_KEY"),
    );
    let noise = format!("tcp://{}", var("GERI_TEST_TCP"));
    let quic = format!("quic://{}", var("GERI_TEST_QUIC"));
    let plain = format!("tcp://{}", var("GERI_TEST_TCP_PLAIN"));
    let decl = SourceDeclaration {
        entities: vec![Entity {
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
        ..router()
    };
    let rounds = [
        (
            "noise",
            noise.as_str(),
            String::new(),
            Some(var("GERI_TEST_HUB_KEY")),
            true,
        ),
        (
            "plain + signature",
            plain.as_str(),
            String::new(),
            None,
            true,
        ),
        (
            "plain + token",
            plain.as_str(),
            var("GERI_TEST_TOKEN"),
            None,
            false,
        ),
        (
            "quic + signature",
            quic.as_str(),
            String::new(),
            Some(var("GERI_TEST_HUB_KEY")),
            true,
        ),
        (
            "quic + token",
            quic.as_str(),
            var("GERI_TEST_TOKEN"),
            Some(var("GERI_TEST_HUB_KEY")),
            false,
        ),
        (
            "noise again",
            noise.as_str(),
            String::new(),
            Some(var("GERI_TEST_HUB_KEY")),
            true,
        ),
    ];
    for (name, addr, token, hub_key, signed) in rounds {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut b = builder(addr, token, "router")
                .source(decl.clone())
                .codec(Codec::Cbor);
            if signed {
                b = b.key(&key);
            }
            if let Some(hub_key) = hub_key {
                b = b.hub_key(hub_key);
            }
            let (geri, mut events) = b.start().unwrap();
            wait_for(&mut events, "connection", |e| {
                *e == Event::Connected { push: true }
            })
            .await;
            geri.state("router", "wan", json!({"up": true})).unwrap();
            timeout(Duration::from_secs(5), async {
                while geri.pending().unwrap() > 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("outbox did not drain");
            let issued_at = Instant::now();
            let issued = reqwest::Client::new()
                .post(format!("{url}/v4/commands"))
                .bearer_auth(&operator)
                .header("content-type", "application/json")
                .body(r#"{"source_id":"router","entity_id":"wan","action":"press","args":null}"#)
                .send()
                .await
                .unwrap();
            assert!(issued.status().is_success());
            let Event::Command(command) =
                wait_for(&mut events, "command", |e| matches!(e, Event::Command(_))).await
            else {
                unreachable!()
            };
            let latency = issued_at.elapsed();
            geri.result(&command, true, Value::Null);
            tokio::time::sleep(Duration::from_millis(200)).await;
            println!("{name}: command latency {latency:?}");
            assert!(latency < Duration::from_millis(500), "{latency:?}");
        });
    }
}

#[test]
fn tcp_settings_are_checked_at_start() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let hub = "ab".repeat(32);
    let only_tcp = builder("http://127.0.0.1:9", "t", "router")
        .hub_key(&hub)
        .start();
    assert!(matches!(only_tcp, Err(StartError::HubKeyWithoutTcp)));
    let no_key = builder("tcp://127.0.0.1:9", "", "router")
        .hub_key(&hub)
        .start();
    assert!(matches!(no_key, Err(StartError::NoiseNeedsKey)));
    let bad = builder("tcp://127.0.0.1:9", "", "router")
        .hub_key("zz")
        .start();
    assert!(matches!(bad, Err(StartError::HubKey)));
    assert!(
        builder("tcp://127.0.0.1:9", "token-123456789012", "router")
            .start()
            .is_ok()
    );
}

#[tokio::test]
async fn invalid_config_is_refused_before_outbox_or_driver_creation() {
    let mut source = router();
    source.entities[0].source_id = "another".into();
    let path = std::env::temp_dir().join(format!(
        "geri-f01-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let result = builder("http://127.0.0.1:1", "t", "a")
        .source(source.clone())
        .outbox(path.join("outbox.sqlite"))
        .start();
    assert!(matches!(
        result,
        Err(StartError::Source(Error::InvalidSource {
            reason: geri_proto::v4::validate::Invalid::EntitySource,
            ..
        }))
    ));
    assert!(
        !path.exists(),
        "geçersiz config outbox dizini oluşturmamalı"
    );
    assert!(matches!(
        Geri::new(Config::new(vec![source]), geri_adapter_sdk::MemStore::new()),
        Err(Error::InvalidSource { .. })
    ));
    let result = builder("http://127.0.0.1:1", "t", "a")
        .source(router())
        .source(router())
        .start();
    assert!(
        matches!(result, Err(StartError::Source(Error::DuplicateSource(id))) if id == "router")
    );
    let result = builder("http://127.0.0.1:1", "t", "a")
        .configure(|c| c.sources = vec![router(), router()])
        .start();
    assert!(matches!(
        result,
        Err(StartError::Source(Error::DuplicateSource(_)))
    ));
}

// ---- F06–F08 ----

/// F06: ölü hub karşısında döngü park eder; olay seli ve backoff itmesi yok.
/// Hub dönünce backoff bitiminde reconnect denenir, outbox boşluksuz ilerler.
#[tokio::test]
async fn a_dead_hub_parks_instead_of_spinning() {
    let mock = Shared::default();
    mock.lock().unwrap().down = true;
    let url = serve(mock.clone()).await;
    let (geri, mut rx) = builder(url, "token", "router")
        .source(router())
        .link(Link::Http)
        .configure(|c| {
            c.backoff_min_ms = 50;
            c.backoff_max_ms = 200;
        })
        .start()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut parked = 0;
    while rx.try_recv().is_ok() {
        parked += 1;
    }
    assert!(
        parked <= 8,
        "{parked} events while hub is down (busy loop?)"
    );
    geri.state("router", "cpu", json!({"pct": 1})).unwrap();
    mock.lock().unwrap().down = false;
    until(&mock, "record after recovery", |m| !m.records.is_empty()).await;
    geri.shutdown();
}

/// Betikli sahte transport: istek tipine göre cevap verir, ilk komut sonucunun
/// gönderimini düşürüp bağlantıyı koparır. Sürücü sonucu yeniden göndermeli,
/// eylem tek komut olayı olarak kalmalı (F07).
#[derive(Debug, Default)]
struct ScriptTransport {
    sent: Vec<ToHub>,
    last_id: Option<u32>,
    connects: usize,
    command_sent: bool,
    result_failed: bool,
}

impl Transport for ScriptTransport {
    type Error = String;

    fn pushes(&self) -> bool {
        true
    }

    async fn connect(&mut self) -> Result<(), String> {
        self.connects += 1;
        Ok(())
    }

    async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), String> {
        self.last_id = frame.id;
        if matches!(frame.msg, ToHub::Result { .. }) && !self.result_failed {
            self.result_failed = true;
            self.sent.push(frame.msg);
            return Err("result lost on the wire".into());
        }
        self.sent.push(frame.msg);
        Ok(())
    }

    async fn recv(&mut self) -> Result<Frame<ToSource>, String> {
        if self.result_failed && self.connects < 2 {
            return Err("connection reset".into());
        }
        let msg = match self.sent.last() {
            Some(ToHub::Hello { .. }) => ToSource::Welcome(Welcome {
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
            Some(ToHub::Declare(_)) => ToSource::Ok {
                watched: None,
                next_sequence: None,
            },
            Some(ToHub::Claim { .. }) => {
                if self.command_sent {
                    ToSource::Idle
                } else {
                    self.command_sent = true;
                    ToSource::Command(Command {
                        command_id: "cmd-1".into(),
                        source_id: "router".into(),
                        entity_id: "wan".into(),
                        action: "press".into(),
                        args: Value::Null,
                        issued_by: "can".into(),
                        issued_at_ms: 1,
                    })
                }
            }
            Some(ToHub::Result { .. }) => ToSource::Ok {
                watched: None,
                next_sequence: None,
            },
            _ => ToSource::Idle,
        };
        Ok(Frame {
            id: self.last_id,
            msg,
        })
    }
}

impl UploadBlob for ScriptTransport {
    type Error = String;

    async fn upload_blob(&self, _sha256: &str, _bytes: Vec<u8>) -> Result<(), String> {
        Err("no blob support".into())
    }
}

#[tokio::test]
async fn a_lost_result_is_retried_without_rerunning_the_action() {
    let mut config = Config::new(vec![router()]);
    config.backoff_min_ms = 20;
    config.backoff_max_ms = 100;
    let geri = Geri::new(config, SqliteStore::memory().unwrap()).unwrap();
    let (tx, mut rx) = mpsc::channel(EVENT_CAP);
    let shared = Arc::new(tokio::sync::Mutex::new(ScriptTransport::default()));
    struct SharedScript(Arc<tokio::sync::Mutex<ScriptTransport>>);
    impl std::fmt::Debug for SharedScript {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("SharedScript")
        }
    }
    impl Transport for SharedScript {
        type Error = String;
        fn pushes(&self) -> bool {
            true
        }
        async fn connect(&mut self) -> Result<(), String> {
            self.0.lock().await.connect().await
        }
        async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), String> {
            self.0.lock().await.send(frame).await
        }
        async fn recv(&mut self) -> Result<Frame<ToSource>, String> {
            self.0.lock().await.recv().await
        }
    }
    impl UploadBlob for SharedScript {
        type Error = String;
        async fn upload_blob(&self, _sha256: &str, _bytes: Vec<u8>) -> Result<(), String> {
            Err("no blob support".into())
        }
    }
    let cfg = DriverConfig {
        connect_timeout: Duration::from_secs(5),
        io_timeout: Duration::from_secs(5),
        upload_timeout: Duration::from_secs(5),
    };
    tokio::spawn(run_with(
        cfg,
        geri.clone(),
        SharedScript(shared.clone()),
        tx,
    ));
    let command = next_command(&mut rx).await;
    assert_eq!(command.command_id, "cmd-1");
    geri.result(&command, true, Value::Null);
    timeout(Duration::from_secs(5), async {
        loop {
            {
                let s = shared.lock().await;
                let retries: Vec<_> = s
                    .sent
                    .iter()
                    .filter_map(|m| match m {
                        ToHub::Result { result, .. } => Some(result.command_id.clone()),
                        _ => None,
                    })
                    .collect();
                if retries.len() >= 2 && s.connects >= 2 {
                    assert!(retries.iter().all(|id| id == "cmd-1"));
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("result was not retried after reconnect");
    // Eylem tek komut olayı olarak kalır: yeniden çalıştırma yok.
    let mut commands = 1;
    while let Ok(e) = rx.try_recv() {
        if matches!(e, Event::Command(_)) {
            commands += 1;
        }
    }
    assert_eq!(commands, 1, "action must not run twice");
    geri.shutdown();
}

/// F08: shutdown sürücü görevini bitirir; alıcı düşmüşse görev zaten durur.
#[tokio::test]
async fn shutdown_stops_the_driver_task() {
    #[derive(Debug)]
    struct Pend {
        up: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Transport for Pend {
        type Error = String;
        fn pushes(&self) -> bool {
            false
        }
        async fn connect(&mut self) -> Result<(), String> {
            self.up.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn send(&mut self, _frame: Frame<ToHub>) -> Result<(), String> {
            Ok(())
        }
        async fn recv(&mut self) -> Result<Frame<ToSource>, String> {
            std::future::pending().await
        }
    }
    impl UploadBlob for Pend {
        type Error = String;
        async fn upload_blob(&self, _sha256: &str, _bytes: Vec<u8>) -> Result<(), String> {
            Ok(())
        }
    }
    let geri = Geri::new(Config::new(vec![router()]), SqliteStore::memory().unwrap()).unwrap();
    let up = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (tx, _rx) = mpsc::channel(EVENT_CAP);
    let task = tokio::spawn(run(geri.clone(), Pend { up: up.clone() }, tx));
    timeout(Duration::from_secs(5), async {
        while !up.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("driver task did not start");
    geri.shutdown();
    timeout(Duration::from_secs(5), task)
        .await
        .expect("driver task survived shutdown")
        .unwrap();
}

/// F08: taşma deposu komutları sırayla kayıpsız tutar, gerisini tekleştirir.
#[test]
fn overflow_keeps_commands_and_coalesces_the_rest() {
    use geri_proto::v4::BlobRef;
    let (tx, mut rx) = mpsc::channel::<Event>(1);
    let mut ov = Overflow::default();
    push(&tx, &mut ov, Event::Connected { push: true });
    assert!(ov.is_empty());
    let cmd = |id: &str| {
        Event::Command(Command {
            command_id: id.into(),
            source_id: "r".into(),
            entity_id: "w".into(),
            action: "press".into(),
            args: Value::Null,
            issued_by: "t".into(),
            issued_at_ms: 1,
        })
    };
    push(&tx, &mut ov, cmd("a"));
    push(
        &tx,
        &mut ov,
        Event::UploadBlobs(vec![BlobRef {
            sha256: "s".into(),
            size: 1,
        }]),
    );
    push(&tx, &mut ov, cmd("b"));
    push(
        &tx,
        &mut ov,
        Event::Watched {
            source_id: "r".into(),
            watched: true,
        },
    );
    push(
        &tx,
        &mut ov,
        Event::Watched {
            source_id: "r".into(),
            watched: false,
        },
    );
    push(&tx, &mut ov, Event::Error(Error::Transport("x".into())));
    push(&tx, &mut ov, Event::Error(Error::Transport("x".into())));
    assert!(!ov.is_empty());
    ov.flush(&tx);
    assert!(!ov.is_empty(), "channel is full, nothing may move");
    assert!(matches!(rx.try_recv(), Ok(Event::Connected { .. })));
    ov.flush(&tx);
    assert!(matches!(rx.try_recv(), Ok(Event::Command(c)) if c.command_id == "a"));
    ov.flush(&tx);
    assert!(matches!(rx.try_recv(), Ok(Event::UploadBlobs(_))));
    ov.flush(&tx);
    assert!(matches!(rx.try_recv(), Ok(Event::Command(c)) if c.command_id == "b"));
    ov.flush(&tx);
    assert!(matches!(
        rx.try_recv(),
        Ok(Event::Watched { watched: false, .. })
    ));
    ov.flush(&tx);
    assert!(matches!(rx.try_recv(), Ok(Event::Error(_))));
    assert!(ov.is_empty());
}

// Askıda kalan her I/O evresinde iptal ve bağlantı temizliği.
#[derive(Debug)]
struct Blocking {
    phase: u8,
    entered: Arc<Notify>,
    closed: Arc<std::sync::atomic::AtomicUsize>,
}
impl Blocking {
    async fn block(&self, phase: u8) {
        if self.phase == phase {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
    }
}
impl Transport for Blocking {
    type Error = String;
    fn pushes(&self) -> bool {
        true
    }
    fn disconnect(&mut self) {
        self.closed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    async fn connect(&mut self) -> Result<(), String> {
        self.block(0).await;
        Ok(())
    }
    async fn send(&mut self, _: Frame<ToHub>) -> Result<(), String> {
        self.block(1).await;
        Ok(())
    }
    async fn recv(&mut self) -> Result<Frame<ToSource>, String> {
        self.block(2).await;
        std::future::pending().await
    }
}
impl UploadBlob for Blocking {
    type Error = String;
    async fn upload_blob(&self, _: &str, _: Vec<u8>) -> Result<(), String> {
        self.block(3).await;
        Ok(())
    }
}
fn blocking_fixture(phase: u8) -> (Geri, Blocking) {
    let geri = Geri::new(Config::new(vec![router()]), SqliteStore::memory().unwrap()).unwrap();
    if phase == 3 {
        geri.blob(
            "router",
            "cpu",
            None,
            RecordKind::State,
            None,
            "application/octet-stream",
            b"test",
            Value::Null,
        )
        .unwrap();
        let blob = geri.lock().store_mut().peek(1).unwrap()[0]
            .blob
            .clone()
            .unwrap();
        geri.lock().return_event(Event::UploadBlobs(vec![blob]));
    }
    (
        geri,
        Blocking {
            phase,
            entered: Arc::new(Notify::new()),
            closed: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        },
    )
}
#[tokio::test]
async fn shutdown_and_receiver_close_cancel_every_io_phase() {
    for phase in 0..4 {
        for close_receiver in [false, true] {
            let (geri, transport) = blocking_fixture(phase);
            let entered = transport.entered.clone();
            let closed = transport.closed.clone();
            let (tx, rx) = mpsc::channel(EVENT_CAP);
            let task = tokio::spawn(run(geri.clone(), transport, tx));
            timeout(Duration::from_secs(1), entered.notified())
                .await
                .unwrap();
            if close_receiver {
                drop(rx);
            } else {
                geri.shutdown();
            }
            timeout(Duration::from_millis(300), task)
                .await
                .expect("I/O iptali bekledi")
                .unwrap();
            assert!(closed.load(std::sync::atomic::Ordering::SeqCst) > 0);
        }
    }
}
#[tokio::test]
async fn replacing_a_driver_cancels_and_joins_the_previous_io() {
    let (geri, first) = blocking_fixture(1);
    let entered = first.entered.clone();
    let closed = first.closed.clone();
    let (tx, _rx) = mpsc::channel(EVENT_CAP);
    let task = tokio::spawn(run(geri.clone(), first, tx));
    timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let (_, second) = blocking_fixture(0);
    let (tx2, _rx2) = mpsc::channel(EVENT_CAP);
    let task2 = tokio::spawn(run(geri.clone(), second, tx2));
    timeout(Duration::from_millis(300), task)
        .await
        .unwrap()
        .unwrap();
    assert!(closed.load(std::sync::atomic::Ordering::SeqCst) > 0);
    geri.shutdown_and_wait().await;
    timeout(Duration::from_millis(300), task2)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn stalled_connect_send_and_upload_have_deadlines() {
    for phase in [0, 1, 3] {
        let (geri, transport) = blocking_fixture(phase);
        let entered = transport.entered.clone();
        let closed = transport.closed.clone();
        let (tx, mut rx) = mpsc::channel(EVENT_CAP);
        let cfg = DriverConfig {
            connect_timeout: Duration::from_millis(40),
            io_timeout: Duration::from_millis(40),
            upload_timeout: Duration::from_millis(40),
        };
        let task = tokio::spawn(run_with(cfg, geri.clone(), transport, tx));
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .unwrap();
        timeout(Duration::from_millis(300), async {
            while !matches!(rx.recv().await, Some(Event::Error(_))) {}
        })
        .await
        .expect("deadline uygulanmadı");
        if phase != 3 {
            assert!(closed.load(std::sync::atomic::Ordering::SeqCst) > 0);
        }
        if phase == 3 {
            assert_eq!(geri.pending().unwrap(), 1);
        }
        geri.shutdown();
        timeout(Duration::from_millis(300), task)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn a_full_event_channel_stops_claims_and_resumes_without_losing_commands() {
    let mock = Shared::default();
    for i in 0..10 {
        mock.lock().unwrap().commands.push_back(Command {
            command_id: format!("c{i}"),
            source_id: "router".into(),
            entity_id: "wan".into(),
            action: "press".into(),
            args: Value::Null,
            issued_by: "test".into(),
            issued_at_ms: 1,
        });
    }
    let url = serve(mock.clone()).await;
    let geri = Geri::new(Config::new(vec![router()]), SqliteStore::memory().unwrap()).unwrap();
    let (tx, mut rx) = mpsc::channel(2);
    let transport = HttpTransport::new(url, "token", "router");
    let task = tokio::spawn(run(geri.clone(), transport, tx));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let left = mock.lock().unwrap().commands.len();
    assert!(left >= 8, "dolu kanalda claim devam etti: {left}");
    // Kanal doluyken kalıcı kayıt ve receipt yine ilerler.
    geri.state("router", "cpu", json!(7)).unwrap();
    until(&mock, "outbox while events are full", |m| {
        !m.records.is_empty()
    })
    .await;
    let ids = timeout(Duration::from_secs(2), async {
        let mut ids = Vec::new();
        while ids.len() < 10 {
            if let Some(Event::Command(command)) = rx.recv().await {
                ids.push(command.command_id);
            }
        }
        ids
    })
    .await
    .expect("kapasite açılınca claim uyanmadı");
    assert_eq!(ids, (0..10).map(|i| format!("c{i}")).collect::<Vec<_>>());
    geri.shutdown();
    task.await.unwrap();
}

#[tokio::test]
async fn the_session_deadline_interrupts_a_longer_send_timeout() {
    let (_, transport) = blocking_fixture(1);
    let entered = transport.entered.clone();
    let closed = transport.closed.clone();
    // Config özel fixture: Session timeout'u I/O timeout'undan önce gelir.
    let geri = Geri::new(
        Config {
            request_timeout_ms: 40,
            ..Config::new(vec![router()])
        },
        SqliteStore::memory().unwrap(),
    )
    .unwrap();
    let (tx, mut rx) = mpsc::channel(EVENT_CAP);
    let task = tokio::spawn(run(geri.clone(), transport, tx));
    timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    let error = timeout(Duration::from_millis(300), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(error, Event::Error(Error::Transport(ref message)) if message.contains("session deadline"))
    );
    assert!(closed.load(std::sync::atomic::Ordering::SeqCst) > 0);
    geri.shutdown();
    task.await.unwrap();
}
