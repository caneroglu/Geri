use super::*;
use geri_binding_stream::{decode, encode, write_frame};
use geri_proto::v4::{ApiError, Limits, Welcome};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};

const DEVICE: [u8; 32] = [7; 32];
const HUB: [u8; 32] = [9; 32];

fn welcome() -> ToSource {
    ToSource::Welcome(Welcome {
        v: VERSION,
        agent_id: "router".into(),
        limits: Limits {
            max_batch_records: 100,
            max_inline_bytes: 1024,
            max_body_bytes: 1024 * 1024,
            max_blob_bytes: 0,
            max_webhook_bytes: 0,
        },
    })
}

fn error(status: u16, code: &str, counter: Option<u64>) -> ToSource {
    ToSource::Error {
        status,
        error: ApiError {
            code: code.into(),
            message: "no".into(),
            earliest_cursor: None,
            counter,
        },
    }
}

struct Peer<IO> {
    io: IO,
    reader: Reader,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Peer<IO> {
    fn new(io: IO) -> Self {
        Self {
            io,
            reader: Reader::new(1 << 20),
        }
    }

    async fn recv(&mut self) -> Option<Frame<ToHub>> {
        loop {
            if let Some(payload) = self.reader.next_frame().unwrap() {
                return Some(decode(&payload).unwrap());
            }
            let mut buf = [0u8; 1024];
            let n = self.io.read(&mut buf).await.ok()?;
            if n == 0 {
                return None;
            }
            self.reader.push(&buf[..n]);
        }
    }

    async fn send(&mut self, id: Option<u32>, msg: ToSource) {
        let mut out = Vec::new();
        write_frame(&encode(&Frame { id, msg }, Codec::Json).unwrap(), &mut out);
        self.io.write_all(&out).await.unwrap();
        self.io.flush().await.unwrap();
    }

    async fn serve_session(&mut self) {
        let hello = self.recv().await.unwrap();
        assert_eq!(
            hello.msg,
            ToHub::Hello {
                v: VERSION,
                auth: None
            }
        );
        self.send(hello.id, welcome()).await;
        self.send(
            None,
            ToSource::Ring {
                source_id: "router".into(),
            },
        )
        .await;
        while let Some(frame) = self.recv().await {
            if frame.msg == ToHub::Ping {
                self.send(None, ToSource::Pong).await;
            }
        }
    }
}

async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("tcp://{}", listener.local_addr().unwrap());
    (listener, addr)
}

async fn session(transport: &mut TcpTransport) {
    transport.connect().await.unwrap();
    transport
        .send(Frame {
            id: Some(1),
            msg: ToHub::Hello {
                v: VERSION,
                auth: None,
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        transport.recv().await.unwrap().msg,
        ToSource::Welcome(_)
    ));
    let ring = transport.recv().await.unwrap();
    assert_eq!(ring.id, None);
    assert_eq!(
        ring.msg,
        ToSource::Ring {
            source_id: "router".into()
        }
    );
}

#[tokio::test]
async fn plain_tcp_shows_the_token_in_the_first_hello() {
    let (listener, addr) = listener().await;
    let hub = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut peer = Peer::new(io);
        let hello = peer.recv().await.unwrap();
        assert_eq!(
            hello.msg,
            ToHub::Hello {
                v: VERSION,
                auth: Some("Bearer router-token-123456".into())
            }
        );
        peer.send(hello.id, welcome()).await;
        peer.serve_session().await;
    });
    let mut transport = TcpTransport::new(&addr, "router-token-123456");
    session(&mut transport).await;
    drop(transport);
    hub.await.unwrap();
}

#[tokio::test]
async fn a_signed_hello_learns_the_counter_once() {
    let (listener, addr) = listener().await;
    let signer = Arc::new(Signer::new("router", DEVICE).unwrap());
    let key = signer.public_key();
    let hub = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut peer = Peer::new(io);
        let mut seen = Vec::new();
        for reply in [error(401, "replay", Some(41)), welcome()] {
            let hello = peer.recv().await.unwrap();
            let ToHub::Hello {
                auth: Some(auth), ..
            } = hello.msg
            else {
                panic!("no auth in {hello:?}")
            };
            let signature = geri_sign::Authorization::parse(&auth).unwrap();
            assert!(signature.verify(&key, SIGNED_METHOD, SIGNED_TARGET, b""));
            seen.push(signature.n);
            peer.send(hello.id, reply).await;
        }
        peer.serve_session().await;
        seen
    });
    let mut transport = TcpTransport::new(&addr, "").with_signer(signer);
    session(&mut transport).await;
    drop(transport);
    assert_eq!(hub.await.unwrap(), [1, 42]);
}

#[tokio::test]
async fn a_rejected_hello_fails_the_connect() {
    let (listener, addr) = listener().await;
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut peer = Peer::new(io);
        let hello = peer.recv().await.unwrap();
        peer.send(hello.id, error(401, "unauthorized", None)).await;
        let _ = peer.recv().await;
    });
    let mut transport = TcpTransport::new(&addr, "wrong-token-123456");
    match transport.connect().await {
        Err(Error::Rejected { status, code, .. }) => {
            assert_eq!((status, code.as_str()), (401, "unauthorized"));
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        transport
            .send(Frame {
                id: Some(1),
                msg: ToHub::Ping
            })
            .await,
        Err(Error::Closed)
    ));
}

#[tokio::test]
async fn noise_proves_the_device_without_any_auth_field() {
    let (listener, addr) = listener().await;
    let hub = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let (io, remote) = geri_noise::accept(io, &HUB).await.unwrap();
        let device = geri_sign::public_key(&DEVICE);
        assert!(geri_noise::matches(&remote, &device));
        Peer::new(io).serve_session().await;
    });
    let mut transport = TcpTransport::noise(&addr, DEVICE, geri_sign::public_key(&HUB));
    session(&mut transport).await;
    drop(transport);
    hub.await.unwrap();
}

#[tokio::test]
async fn noise_with_the_wrong_hub_key_does_not_connect() {
    let (listener, addr) = listener().await;
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let _ = geri_noise::accept(io, &HUB).await;
    });
    let mut transport = TcpTransport::noise(&addr, DEVICE, geri_sign::public_key(&[1; 32]));
    assert!(transport.connect().await.is_err());
}

#[tokio::test]
async fn keepalive_answered_stays_quiet_and_a_silent_hub_is_detected() {
    let (listener, addr) = listener().await;
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut peer = Peer::new(io);
        let hello = peer.recv().await.unwrap();
        peer.send(hello.id, welcome()).await;
        for _ in 0..2 {
            assert_eq!(peer.recv().await.unwrap().msg, ToHub::Ping);
            peer.send(None, ToSource::Pong).await;
        }
        assert_eq!(peer.recv().await.unwrap().msg, ToHub::Ping);
        std::future::pending::<()>().await;
    });
    let mut transport =
        TcpTransport::new(&addr, "router-token-123456").with_keepalive(Duration::from_millis(50));
    transport.connect().await.unwrap();
    let started = Instant::now();
    assert!(matches!(transport.recv().await, Err(Error::Silent)));
    assert!(started.elapsed() >= Duration::from_millis(150));
}

#[tokio::test]
async fn a_closed_connection_is_reported() {
    let (listener, addr) = listener().await;
    tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut peer = Peer::new(io);
        let hello = peer.recv().await.unwrap();
        peer.send(hello.id, welcome()).await;
    });
    let mut transport = TcpTransport::new(&addr, "router-token-123456");
    transport.connect().await.unwrap();
    assert!(matches!(transport.recv().await, Err(Error::Closed)));
}
