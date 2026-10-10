use super::*;
use futures_util::{SinkExt, StreamExt};
use geri_proto::v4::{
    Command, Limits, Receipt, ReceiptStatus, RecordReceipt, SourceDeclaration, VERSION, Welcome,
};
use geri_sdk_linux::{Config, Event, Geri, NoBlobs, run};
use geri_transport_ws::WsTransport;
use serde_json::json;
use std::time::Duration;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, PartialEq)]
enum Seen {
    Connection,
    Record(u64),
    Result(String),
}

async fn hub(drop_after_receipt: bool) -> (String, mpsc::UnboundedReceiver<Seen>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut commands = vec![Command {
            command_id: "c1".into(),
            source_id: "bluepill".into(),
            entity_id: "led".into(),
            action: "press".into(),
            args: json!(null),
            issued_by: "can".into(),
            issued_at_ms: 1,
        }];
        let mut first = true;
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            tx.send(Seen::Connection).unwrap();
            let cut = std::mem::take(&mut first) && drop_after_receipt;
            while let Some(Ok(msg)) = ws.next().await {
                let bytes = match &msg {
                    Message::Text(t) => t.as_bytes().to_vec(),
                    Message::Binary(b) => b.to_vec(),
                    _ => continue,
                };
                let frame: Frame<ToHub> = geri_binding_stream::decode(&bytes).unwrap();
                let ok = ToSource::Ok {
                    watched: None,
                    next_sequence: None,
                };
                let mut replies = vec![];
                let mut close = false;
                match frame.msg {
                    ToHub::Ping => replies.push((None, ToSource::Pong)),
                    ToHub::Hello { .. } => replies.push((
                        frame.id,
                        ToSource::Welcome(Welcome {
                            v: VERSION,
                            agent_id: "bridge".into(),
                            limits: Limits {
                                max_batch_records: 128,
                                max_inline_bytes: 65536,
                                max_body_bytes: 1 << 20,
                                max_blob_bytes: 1 << 24,
                                max_webhook_bytes: 65536,
                            },
                        }),
                    )),
                    ToHub::Declare(d) => {
                        replies.push((frame.id, ok));
                        replies.push((
                            None,
                            ToSource::Ring {
                                source_id: d.source_id,
                            },
                        ));
                    }
                    ToHub::Batch(b) => {
                        let records = b
                            .records
                            .iter()
                            .map(|r| {
                                tx.send(Seen::Record(r.sequence)).unwrap();
                                RecordReceipt {
                                    source_id: r.source_id.clone(),
                                    record_id: r.record_id.clone(),
                                    status: ReceiptStatus::Accepted,
                                    cursor: Some(r.sequence),
                                    expected_sequence: None,
                                }
                            })
                            .collect();
                        replies.push((
                            frame.id,
                            ToSource::Receipt(Receipt {
                                v: VERSION,
                                records,
                            }),
                        ));
                        close = cut;
                    }
                    ToHub::Claim { .. } => replies.push((
                        frame.id,
                        match commands.pop() {
                            Some(c) => ToSource::Command(c),
                            None => ToSource::Idle,
                        },
                    )),
                    ToHub::Result { result, .. } => {
                        tx.send(Seen::Result(result.command_id)).unwrap();
                        replies.push((frame.id, ok));
                    }
                    _ => replies.push((frame.id, ok)),
                }
                for (id, msg) in replies {
                    let bytes =
                        geri_binding_stream::encode(&Frame { id, msg }, Codec::Json).unwrap();
                    let text = String::from_utf8(bytes).unwrap();
                    if ws.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                if close {
                    let _ = ws.close(None).await;
                    break;
                }
            }
        }
    });
    (format!("http://{addr}"), rx)
}

fn decl() -> SourceDeclaration {
    SourceDeclaration {
        source_id: "bluepill".into(),
        label: "Blue Pill".into(),
        kind: "stm32".into(),
        entities: vec![
            geri_proto::v4::Entity::new("bluepill", "wan")
                .control(geri_proto::Shape::Trigger, geri_proto::Danger::Safe),
        ],
    }
}

fn start(url: &str) -> (Geri<geri_adapter_sdk::MemStore>, mpsc::Receiver<Event>) {
    let (device, relay) = tokio::io::duplex(256);
    let hub = WsTransport::new(url, "bridge-token-123456", "bridge").unwrap();
    tokio::spawn(bridge::run(relay, hub, DEFAULT_MAX_FRAME, |_| {}));
    let mut config = Config::new(vec![decl()]);
    config.backoff_min_ms = 50;
    let geri = Geri::new(config, geri_adapter_sdk::MemStore::new()).unwrap();
    let (tx, rx) = mpsc::channel(geri_sdk_linux::EVENT_CAP);
    tokio::spawn(run(geri.clone(), NoBlobs(SerialTransport::new(device)), tx));
    (geri, rx)
}

#[allow(async_fn_in_trait)]
trait Recv<T> {
    async fn recv_next(&mut self) -> Option<T>;
}

impl<T> Recv<T> for mpsc::Receiver<T> {
    async fn recv_next(&mut self) -> Option<T> {
        self.recv().await
    }
}

impl<T> Recv<T> for mpsc::UnboundedReceiver<T> {
    async fn recv_next(&mut self) -> Option<T> {
        self.recv().await
    }
}

async fn next<T>(rx: &mut impl Recv<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv_next())
        .await
        .expect("timed out")
        .expect("closed")
}

#[tokio::test]
async fn a_source_on_a_serial_line_talks_to_the_hub_through_the_bridge() {
    let (url, mut hub_saw) = hub(false).await;
    let (geri, mut events) = start(&url);
    geri.state("bluepill", "temp", json!(21)).unwrap();

    assert_eq!(next(&mut hub_saw).await, Seen::Connection);
    assert_eq!(next(&mut events).await, Event::Connected { push: true });
    assert_eq!(next(&mut hub_saw).await, Seen::Record(1));
    let command = loop {
        if let Event::Command(c) = next(&mut events).await {
            break c;
        }
    };
    geri.result(&command, true, json!("on"));
    assert_eq!(next(&mut hub_saw).await, Seen::Result("c1".into()));
    assert_eq!(geri.pending().unwrap(), 0);
}

#[tokio::test]
async fn losing_the_hub_resets_the_source_which_starts_over() {
    let (url, mut hub_saw) = hub(true).await;
    let (geri, mut events) = start(&url);
    geri.state("bluepill", "temp", json!(21)).unwrap();
    assert_eq!(next(&mut hub_saw).await, Seen::Connection);
    assert_eq!(next(&mut hub_saw).await, Seen::Record(1));

    let reset = loop {
        if let Event::Error(e) = next(&mut events).await {
            break e;
        }
    };
    assert!(reset.to_string().contains("link reset"), "{reset}");
    assert_eq!(next(&mut hub_saw).await, Seen::Connection);
    geri.state("bluepill", "temp", json!(22)).unwrap();
    assert_eq!(next(&mut hub_saw).await, Seen::Record(2));
}

#[tokio::test]
async fn a_bridge_without_a_session_asks_the_source_to_start_over() {
    let (url, _hub_saw) = hub(false).await;
    let (device, relay) = tokio::io::duplex(256);
    let hub = WsTransport::new(&url, "bridge-token-123456", "bridge").unwrap();
    tokio::spawn(bridge::run(relay, hub, DEFAULT_MAX_FRAME, |_| {}));
    let mut source = SerialTransport::new(device);
    source
        .send(Frame {
            id: Some(9),
            msg: ToHub::Claim {
                source_id: "bluepill".into(),
            },
        })
        .await
        .unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(5), source.recv()).await;
    assert!(matches!(answer, Ok(Err(Error::Reset))), "{answer:?}");
}

#[tokio::test]
async fn an_unreachable_hub_is_reported_as_a_reset() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let (device, relay) = tokio::io::duplex(256);
    let hub = WsTransport::new(&url, "bridge-token-123456", "bridge").unwrap();
    let (tx, mut seen) = mpsc::unbounded_channel();
    tokio::spawn(bridge::run(relay, hub, DEFAULT_MAX_FRAME, move |e| {
        let _ = tx.send(matches!(e, bridge::Event::ConnectFailed(_)));
    }));
    let mut source = SerialTransport::new(device);
    source.connect().await.unwrap();
    let answer = tokio::time::timeout(Duration::from_secs(5), source.recv()).await;
    assert!(matches!(answer, Ok(Err(Error::Reset))), "{answer:?}");
    assert!(next(&mut seen).await);
}

#[tokio::test]
async fn line_noise_is_counted_and_skipped() {
    let (mut wire, device) = tokio::io::duplex(256);
    let mut source = SerialTransport::new(device);
    let mut bytes = vec![0x13, 0x37, 0x42, 0x00, 0xff, 0x00];
    let pong = geri_binding_serial::encode(
        &Frame {
            id: None,
            msg: ToSource::Pong,
        },
        Codec::Cbor,
    )
    .unwrap();
    geri_binding_serial::write_frame(&pong, &mut bytes);
    tokio::io::AsyncWriteExt::write_all(&mut wire, &bytes)
        .await
        .unwrap();
    let frame = source.recv().await.unwrap();
    assert_eq!(frame.msg, ToSource::Pong);
    assert_eq!(source.dropped(), 2);
}

#[tokio::test]
#[ignore = "needs a serial port, a running geri-serial-bridge and a hub"]
async fn against_a_real_hub_over_a_real_serial_port() {
    let path = std::env::var("GERI_TEST_SERIAL").expect("GERI_TEST_SERIAL");
    let source = std::env::var("GERI_TEST_SOURCE").unwrap_or_else(|_| "router".into());
    let transport = SerialTransport::open(&path, 115_200).unwrap();
    let decl = SourceDeclaration {
        source_id: source.clone(),
        ..decl()
    };
    let geri = Geri::new(Config::new(vec![decl]), geri_adapter_sdk::MemStore::new()).unwrap();
    let (tx, mut events) = mpsc::channel(geri_sdk_linux::EVENT_CAP);
    tokio::spawn(run(geri.clone(), NoBlobs(transport), tx));
    let started = std::time::Instant::now();
    geri.state(&source, "temp", json!(21)).unwrap();
    assert_eq!(next(&mut events).await, Event::Connected { push: true });
    for _ in 0..100 {
        if geri.pending().unwrap() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(geri.pending().unwrap(), 0);
    println!(
        "seri hat â†’ kÃ¶prÃ¼ â†’ hub: kayÄ±t {:?} iÃ§inde alÄ±ndÄ±",
        started.elapsed()
    );
}

#[tokio::test]
async fn reopen_swaps_dead_io_and_resyncs() {
    let (mut wire, device) = tokio::io::duplex(1024);
    let mut source = SerialTransport::new(device);
    // Havada yarım çerçeve bırak, portu öldür.
    tokio::io::AsyncWriteExt::write_all(&mut wire, &[0x13, 0x37])
        .await
        .unwrap();
    drop(wire);
    let answer = tokio::time::timeout(Duration::from_secs(5), source.recv()).await;
    assert!(
        matches!(answer, Ok(Err(Error::Closed | Error::Io(_)))),
        "{answer:?}"
    );
    // Port geri geldi: yeni tutamaç, reset el sıkışması, temiz çerçeve akışı.
    let (mut wire2, device2) = tokio::io::duplex(1024);
    source.reopen(device2);
    source.connect().await.unwrap();
    let mut mark = [0u8; 1];
    tokio::io::AsyncReadExt::read_exact(&mut wire2, &mut mark)
        .await
        .unwrap();
    assert_eq!(mark, [0]);
    let pong = geri_binding_serial::encode(
        &geri_proto::v4::msg::Frame {
            id: None,
            msg: ToSource::Pong,
        },
        Codec::Cbor,
    )
    .unwrap();
    let mut bytes = Vec::new();
    geri_binding_serial::write_frame(&pong, &mut bytes);
    tokio::io::AsyncWriteExt::write_all(&mut wire2, &bytes)
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), source.recv())
        .await
        .expect("yeniden açılan hat cevap vermedi")
        .unwrap();
    assert_eq!(frame.msg, ToSource::Pong);
    assert_eq!(source.dropped(), 0);
}

#[tokio::test]
async fn sdk_reopens_a_returning_port_and_drains_the_outbox_in_order() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    let (url, mut seen) = hub(false).await;
    let (dead, wire) = tokio::io::duplex(256);
    drop(wire);
    let ports = Arc::new(Mutex::new(std::collections::VecDeque::new()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let factory_ports = ports.clone();
    let factory_attempts = attempts.clone();
    let transport = SerialTransport::new(dead).with_reopener(move || {
        factory_attempts.fetch_add(1, Ordering::SeqCst);
        factory_ports
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(Error::Closed)
    });
    let mut cfg = Config::new(vec![decl()]);
    cfg.backoff_min_ms = 20;
    cfg.backoff_max_ms = 80;
    let geri = Geri::new(cfg, geri_adapter_sdk::MemStore::new()).unwrap();
    geri.state("bluepill", "temp", json!(21)).unwrap();
    let (tx, _rx) = mpsc::channel(geri_sdk_linux::EVENT_CAP);
    let task = tokio::spawn(run(geri.clone(), NoBlobs(transport), tx));
    tokio::time::timeout(Duration::from_secs(1), async {
        while attempts.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        attempts.load(Ordering::SeqCst) < 10,
        "offline port döngüsü park etmedi"
    );
    assert_eq!(geri.pending().unwrap(), 1);
    let (device, wire) = tokio::io::duplex(256);
    let hub_transport = WsTransport::new(&url, "bridge-token-123456", "bridge").unwrap();
    let bridge = tokio::spawn(bridge::run(wire, hub_transport, DEFAULT_MAX_FRAME, |_| {}));
    ports.lock().unwrap().push_back(device);
    loop {
        if let Seen::Record(seq) = next(&mut seen).await {
            assert_eq!(seq, 1);
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while geri.pending().unwrap() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    geri.state("bluepill", "temp", json!(22)).unwrap();
    loop {
        if let Seen::Record(seq) = next(&mut seen).await {
            assert_eq!(seq, 2);
            break;
        }
    }
    geri.shutdown();
    task.await.unwrap();
    bridge.abort();
}

#[tokio::test]
async fn a_partial_serial_write_times_out_then_recovers_on_a_fresh_io() {
    use std::sync::{Arc, Mutex};
    // Bir baytlık hat: peer okumayınca hello yarıda kalır.
    let (blocked, mut wire) = tokio::io::duplex(1);
    let ports = Arc::new(Mutex::new(std::collections::VecDeque::new()));
    let factory_ports = ports.clone();
    let transport = SerialTransport::new(blocked).with_reopener(move || {
        factory_ports
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(Error::Closed)
    });
    let mut cfg = Config::new(vec![decl()]);
    cfg.backoff_min_ms = 20;
    cfg.backoff_max_ms = 80;
    let geri = Geri::new(cfg, geri_adapter_sdk::MemStore::new()).unwrap();
    let (tx, mut rx) = mpsc::channel(geri_sdk_linux::EVENT_CAP);
    let driver = geri_sdk_linux::DriverConfig {
        connect_timeout: Duration::from_millis(40),
        io_timeout: Duration::from_millis(40),
        upload_timeout: Duration::from_millis(40),
    };
    let task = tokio::spawn(geri_sdk_linux::run_with(
        driver,
        geri.clone(),
        NoBlobs(transport),
        tx,
    ));
    // Reset'i oku, hello'nun ilk baytı tamponda kalınca okumayı bırak.
    let mut reset = Vec::new();
    geri_binding_serial::write_reset(&mut reset);
    let mut mark = vec![0; reset.len()];
    tokio::io::AsyncReadExt::read_exact(&mut wire, &mut mark)
        .await
        .unwrap();
    loop {
        if let Event::Error(e) = next(&mut rx).await {
            assert!(e.to_string().contains("send: timed out"), "{e}");
            break;
        }
    }
    let mut partial = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut wire, &mut partial)
        .await
        .unwrap();
    assert_eq!(
        partial.len(),
        1,
        "eski port açık kaldı veya hello tamamlandı"
    );
    let (url, mut seen) = hub(false).await;
    let (device, relay) = tokio::io::duplex(1024);
    let bridge = tokio::spawn(bridge::run(
        relay,
        WsTransport::new(&url, "bridge-token-123456", "bridge").unwrap(),
        DEFAULT_MAX_FRAME,
        |_| {},
    ));
    ports.lock().unwrap().push_back(device);
    geri.state("bluepill", "temp", json!(9)).unwrap();
    loop {
        if let Seen::Record(seq) = next(&mut seen).await {
            assert_eq!(seq, 1);
            break;
        }
    }
    geri.shutdown();
    task.await.unwrap();
    bridge.abort();
}
