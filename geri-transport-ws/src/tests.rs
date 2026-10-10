use super::*;
use geri_adapter_sdk::{Config, Event, MemStore, Session};
use geri_proto::v4::{
    Command, Limits, SourceDeclaration, VERSION, Welcome,
    msg::{ToHub, ToSource},
};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

const TOKEN: &str = "router-token-123456";
const SEED: [u8; 32] = [7; 32];

#[derive(Clone, Copy, Default)]
struct Hub {
    silent: bool,
    counter: u64,
}

async fn hub(behaviour: Hub) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let window = std::sync::Mutex::new(geri_sign::Window {
        max: behaviour.counter,
        seen: u64::MAX,
    });
    tokio::spawn(async move {
        let mut ws = loop {
            let (tcp, _) = listener.accept().await.unwrap();
            #[allow(clippy::result_large_err)]
            let check = |req: &Request, res: Response| -> Result<Response, ErrorResponse> {
                let deny = |counter: Option<u64>| {
                    let mut denied = ErrorResponse::new(Some("unauthorized".into()));
                    *denied.status_mut() = tungstenite::http::StatusCode::UNAUTHORIZED;
                    if let Some(counter) = counter {
                        denied
                            .headers_mut()
                            .insert(geri_sign::COUNTER_HEADER, counter.into());
                    }
                    Err(denied)
                };
                let auth = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok());
                let target = req.uri().path_and_query().unwrap().as_str();
                if let Some(signed) = auth.and_then(geri_sign::Authorization::parse) {
                    let key = geri_sign::public_key(&SEED);
                    if !signed.verify(&key, "GET", target, b"") {
                        return deny(None);
                    }
                    let mut window = window.lock().unwrap();
                    if !window.accept(signed.n) {
                        return deny(Some(window.max));
                    }
                    Ok(res)
                } else if auth == Some(&format!("Bearer {TOKEN}")) && target == "/v4/ws" {
                    Ok(res)
                } else {
                    deny(None)
                }
            };
            if let Ok(ws) = tokio_tungstenite::accept_hdr_async(tcp, check).await {
                break ws;
            }
        };
        let mut commands = vec![Command {
            command_id: "c1".into(),
            source_id: "router".into(),
            entity_id: "wan".into(),
            action: "press".into(),
            args: json!(null),
            issued_by: "can".into(),
            issued_at_ms: 1,
        }];
        while let Some(Ok(msg)) = ws.next().await {
            let (codec, bytes) = match &msg {
                Message::Text(t) => (Codec::Json, t.as_bytes().to_vec()),
                Message::Binary(b) => (Codec::Cbor, b.to_vec()),
                _ => continue,
            };
            let frame: Frame<ToHub> = geri_binding_stream::decode(&bytes).unwrap();
            let mut replies = vec![];
            match frame.msg {
                ToHub::Ping if behaviour.silent => continue,
                ToHub::Ping => replies.push(Frame {
                    id: None,
                    msg: ToSource::Pong,
                }),
                ToHub::Hello { .. } => replies.push(Frame {
                    id: frame.id,
                    msg: ToSource::Welcome(Welcome {
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
                }),
                ToHub::Declare(_) => {
                    replies.push(Frame {
                        id: frame.id,
                        msg: ToSource::Ok {
                            watched: None,
                            next_sequence: None,
                        },
                    });
                    let source_id = "router".to_string();
                    replies.push(Frame {
                        id: None,
                        msg: ToSource::Pace {
                            source_id: source_id.clone(),
                            ms: 1000,
                        },
                    });
                }
                ToHub::Claim { .. } => replies.push(Frame {
                    id: frame.id,
                    msg: match commands.pop() {
                        Some(c) => ToSource::Command(c),
                        None => ToSource::Idle,
                    },
                }),
                _ => replies.push(Frame {
                    id: frame.id,
                    msg: ToSource::Ok {
                        watched: None,
                        next_sequence: None,
                    },
                }),
            }
            for reply in replies {
                let bytes = geri_binding_stream::encode(&reply, codec).unwrap();
                let out = match codec {
                    Codec::Json => Message::Text(String::from_utf8(bytes).unwrap().into()),
                    Codec::Cbor => Message::Binary(bytes.into()),
                };
                if ws.send(out).await.is_err() {
                    return;
                }
            }
        }
    });
    format!("http://{addr}")
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

async fn drive(s: &mut Session<MemStore>, t: &mut WsTransport, rounds: usize) -> Vec<Event> {
    let mut events = vec![];
    for _ in 0..rounds {
        while let Some(frame) = s.poll_transmit(0) {
            t.send(frame).await.unwrap();
        }
        events.extend(std::iter::from_fn(|| s.poll_event()));
        match tokio::time::timeout(Duration::from_millis(200), t.recv()).await {
            Ok(frame) => s.handle_frame(0, frame.unwrap()),
            Err(_) => break,
        }
    }
    events.extend(std::iter::from_fn(|| s.poll_event()));
    events
}

#[test]
fn hub_urls_become_websocket_urls() {
    let url = |base| WsTransport::new(base, "t", "a").map(|t| t.url().to_owned());
    assert_eq!(
        url("https://lab.example.com/").unwrap(),
        "wss://lab.example.com/v4/ws"
    );
    assert_eq!(
        url("http://127.0.0.1:8787").unwrap(),
        "ws://127.0.0.1:8787/v4/ws"
    );
    assert_eq!(url("wss://x/custom").unwrap(), "wss://x/custom");
    assert!(matches!(url("ftp://x"), Err(Error::Url)));
}

#[tokio::test]
async fn a_session_runs_over_websocket_with_pushed_signals() {
    for codec in [Codec::Json, Codec::Cbor] {
        let url = hub(Hub::default()).await;
        let mut t = WsTransport::new(&url, TOKEN, "router")
            .unwrap()
            .with_codec(codec);
        let mut s = session();
        s.live("router", json!({"cpu": 5}).as_object().unwrap().clone())
            .unwrap();
        assert!(t.pushes());
        t.connect().await.unwrap();
        s.handle_connected(0, t.pushes());
        let events = drive(&mut s, &mut t, 20).await;
        assert!(
            events.contains(&Event::Connected { push: true }),
            "{codec:?} {events:?}"
        );
        assert!(events.contains(&Event::Watched {
            source_id: "router".into(),
            watched: true
        }));
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Event::Command(c) if c.command_id == "c1")),
            "{events:?}"
        );
    }
}

#[tokio::test]
async fn a_signed_connect_learns_the_counter_once_and_talks() {
    let url = hub(Hub {
        counter: 1000,
        ..Hub::default()
    })
    .await;
    let signer = Arc::new(geri_sign::Signer::new("router", SEED).unwrap());
    let mut t = WsTransport::new(&url, "", "router")
        .unwrap()
        .with_signer(signer.clone());
    let mut s = session();
    t.connect().await.unwrap();
    s.handle_connected(0, t.pushes());
    let events = drive(&mut s, &mut t, 20).await;
    assert!(
        events.contains(&Event::Connected { push: true }),
        "{events:?}"
    );
    let next = signer.authorization("GET", "/", b"");
    assert_eq!(geri_sign::Authorization::parse(&next).unwrap().n, 1002);
}

#[tokio::test]
async fn a_wrong_key_is_refused_at_connect() {
    let url = hub(Hub::default()).await;
    let other = geri_sign::Signer::new("router", [8; 32]).unwrap();
    let mut t = WsTransport::new(&url, "", "router")
        .unwrap()
        .with_signer(Arc::new(other));
    assert!(matches!(t.connect().await, Err(Error::Ws(_))));
}

#[tokio::test]
async fn a_wrong_token_is_refused_at_connect() {
    let url = hub(Hub::default()).await;
    let mut t = WsTransport::new(&url, "wrong", "router").unwrap();
    assert!(matches!(t.connect().await, Err(Error::Ws(_))));
}

#[tokio::test]
async fn keepalive_keeps_a_live_hub_and_drops_a_silent_one() {
    let url = hub(Hub::default()).await;
    let mut t = WsTransport::new(&url, TOKEN, "router")
        .unwrap()
        .with_keepalive(Duration::from_millis(30));
    t.connect().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), t.recv())
            .await
            .is_err()
    );

    let url = hub(Hub {
        silent: true,
        ..Hub::default()
    })
    .await;
    let mut t = WsTransport::new(&url, TOKEN, "router")
        .unwrap()
        .with_keepalive(Duration::from_millis(30));
    t.connect().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), t.recv())
        .await
        .expect("dead connection should be noticed");
    assert!(matches!(result, Err(Error::Silent)));
    let hello = Frame {
        id: Some(1),
        msg: ToHub::Hello {
            v: VERSION,
            auth: None,
        },
    };
    assert!(matches!(t.send(hello).await, Err(Error::Closed)));
}

#[tokio::test]
async fn being_replaced_is_told_apart_from_a_plain_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let frame = tungstenite::protocol::CloseFrame {
            code: CLOSE_REPLACED.into(),
            reason: "replaced by a newer connection".into(),
        };
        ws.close(Some(frame)).await.unwrap();
    });
    let mut t = WsTransport::new(&format!("http://{addr}"), TOKEN, "router").unwrap();
    t.connect().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), t.recv())
        .await
        .unwrap();
    assert!(matches!(result, Err(Error::Replaced)), "{result:?}");
}
