use crate::api::blocking;
use crate::{
    api::{Shared, SignatureError, replay_error},
    msg::{self, MAX_BODY, Signal, Socket, reject},
};
use axum::http::StatusCode;
use geri_binding_stream::{Codec, Reader, SIGNED_METHOD, SIGNED_TARGET};
use geri_core::v4::AgentPolicy;
use geri_proto::v4::msg::{Frame, ToHub, ToSource};
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::broadcast,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const AUTH_ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
    Noise,
    Plain,
}

pub(crate) async fn listen(app: Shared, listener: TcpListener, mode: Mode) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("tcp accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        tokio::spawn(serve(app.clone(), stream, mode));
    }
}

async fn serve(app: Shared, stream: TcpStream, mode: Mode) {
    let _ = stream.set_nodelay(true);
    match mode {
        Mode::Noise => {
            let Some(seed) = app.signing_seed().copied() else {
                return;
            };
            let accepted =
                tokio::time::timeout(HANDSHAKE_TIMEOUT, geri_noise::accept(stream, &seed)).await;
            let Ok(Ok((io, remote))) = accepted else {
                return;
            };
            let Some(p) = app.by_noise_key(&remote) else {
                return;
            };
            let mut conn = Conn::new(io);
            let Some(slot) = Socket::open(app.clone(), p.agent_id.clone()) else {
                let first = tokio::time::timeout(HANDSHAKE_TIMEOUT, conn.recv()).await;
                let id = first
                    .ok()
                    .flatten()
                    .and_then(|b| geri_binding_stream::decode::<ToHub>(&b).ok())
                    .and_then(|f| f.id);
                let _ = conn.send(&Frame { id, msg: busy() }).await;
                return;
            };
            let signals = app.signals.subscribe();
            connection(app, p, conn, signals, slot).await;
        }
        Mode::Plain => plain(app, stream).await,
    }
}

pub(crate) async fn plain<IO: AsyncRead + AsyncWrite + Unpin>(app: Shared, io: IO) {
    let mut conn = Conn::new(io);
    let authenticated =
        tokio::time::timeout(HANDSHAKE_TIMEOUT, authenticate(&app, &mut conn)).await;
    let Ok(Some((p, id))) = authenticated else {
        return;
    };
    let Some(slot) = Socket::open(app.clone(), p.agent_id.clone()) else {
        let _ = conn.send(&Frame { id, msg: busy() }).await;
        return;
    };
    let signals = app.signals.subscribe();
    let hello = ToHub::Hello {
        v: geri_proto::v4::VERSION,
        auth: None,
    };
    let mut replies = vec![Frame {
        id,
        msg: msg::handle(&app, &p, hello),
    }];
    replies.extend(msg::watched(&app, &p));
    for reply in &replies {
        if conn.send(reply).await.is_err() {
            return;
        }
    }
    connection(app, p, conn, signals, slot).await;
}

fn busy() -> ToSource {
    reject(
        StatusCode::SERVICE_UNAVAILABLE,
        "busy",
        "too many connections for this agent",
    )
}

async fn authenticate<IO: AsyncRead + AsyncWrite + Unpin>(
    app: &Shared,
    conn: &mut Conn<IO>,
) -> Option<(AgentPolicy, Option<u32>)> {
    let unauthorized = |message: &str| reject(StatusCode::UNAUTHORIZED, "unauthorized", message);
    for _ in 0..AUTH_ATTEMPTS {
        let bytes = conn.recv().await?;
        let frame = match geri_binding_stream::decode::<ToHub>(&bytes) {
            Ok(frame) => frame,
            Err(_) => return None,
        };
        let reply = match frame.msg {
            ToHub::Hello {
                auth: Some(auth), ..
            } => {
                if let Some(token) = auth.strip_prefix("Bearer ") {
                    match app.by_token(token) {
                        Some(p) => return Some((p, frame.id)),
                        None => unauthorized("invalid bearer token"),
                    }
                } else if auth.starts_with(geri_sign::SCHEME) {
                    let checked = blocking(app, move |app| {
                        app.check_signature(&auth, SIGNED_METHOD, SIGNED_TARGET, b"")
                    })
                    .await;
                    match checked {
                        Ok(agent) => match app.agent(&agent) {
                            Some(p) => return Some((p, frame.id)),
                            None => unauthorized("unknown agent"),
                        },
                        Err(SignatureError::Invalid(message)) => unauthorized(message),
                        Err(SignatureError::Replay(max)) => ToSource::Error {
                            status: StatusCode::UNAUTHORIZED.as_u16(),
                            error: replay_error(max),
                        },
                        Err(SignatureError::Store(e)) => {
                            eprintln!("tcp counter: {e:?}");
                            reject(StatusCode::INTERNAL_SERVER_ERROR, "internal", "storage")
                        }
                    }
                } else {
                    unauthorized("unknown authorization scheme")
                }
            }
            _ => unauthorized("plain tcp starts with a hello carrying auth"),
        };
        conn.send(&Frame {
            id: frame.id,
            msg: reply,
        })
        .await
        .ok()?;
    }
    None
}

async fn connection<IO: AsyncRead + AsyncWrite + Unpin>(
    app: Shared,
    p: AgentPolicy,
    mut conn: Conn<IO>,
    mut signals: broadcast::Receiver<Signal>,
    _slot: Socket,
) {
    let idle = tokio::time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle);
    loop {
        let replies = tokio::select! {
            incoming = conn.recv() => match incoming {
                Some(bytes) => {
                    idle.as_mut().reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
                    let p = p.clone();
                    blocking(&app, move |app| msg::on_frame(app, &p, &bytes)).await
                }
                None => break,
            },
            () = &mut idle => break,
            signal = signals.recv() => match msg::on_signal(&p, signal) {
                Some(replies) => replies,
                None => break,
            },
        };
        for reply in &replies {
            match tokio::time::timeout(msg::SEND_TIMEOUT, conn.send(reply)).await {
                Ok(Ok(())) => {}
                _ => return,
            }
        }
    }
}

struct Conn<IO> {
    io: IO,
    reader: Reader,
    codec: Codec,
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Conn<IO> {
    fn new(io: IO) -> Self {
        Self {
            io,
            reader: Reader::new(MAX_BODY),
            codec: Codec::Json,
        }
    }

    async fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            match self.reader.next_frame() {
                Ok(Some(payload)) => {
                    if let Some(codec) = Codec::of(&payload) {
                        self.codec = codec;
                    }
                    return Some(payload);
                }
                Ok(None) => {}
                Err(_) => return None,
            }
            let mut buf = [0u8; 8 * 1024];
            match self.io.read(&mut buf).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.reader.push(&buf[..n]),
            }
        }
    }

    async fn send(&mut self, frame: &Frame<ToSource>) -> std::io::Result<()> {
        let payload = geri_binding_stream::encode(frame, self.codec)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut out = Vec::with_capacity(payload.len() + 5);
        geri_binding_stream::write_frame(&payload, &mut out);
        self.io.write_all(&out).await?;
        self.io.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{TEST_HUB_SEED, TEST_SEED, test_app_with_key};
    use geri_adapter_sdk::Transport;
    use geri_proto::v4::VERSION;
    use geri_transport_tcp::{Error, TcpTransport};
    use std::sync::Arc;

    const TOKEN: &str = "router-token-123456";

    async fn hub(app: Shared, mode: Mode) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(listen(app, listener, mode));
        addr
    }

    fn app(max_sockets: usize) -> (tempfile::TempDir, Shared) {
        let dir = tempfile::tempdir().unwrap();
        let app = test_app_with_key(dir.path(), msg::Signals::new(32, max_sockets));
        (dir, app)
    }

    async fn hello_and_ring(app: &Shared, transport: &mut TcpTransport) {
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
        let welcome = transport.recv().await.unwrap();
        assert_eq!(welcome.id, Some(1));
        let ToSource::Welcome(welcome) = welcome.msg else {
            panic!("{welcome:?}")
        };
        assert_eq!(welcome.agent_id, "router");
        app.signals.ring("router");
        assert_eq!(
            transport.recv().await.unwrap().msg,
            ToSource::Ring {
                source_id: "router".into()
            }
        );
    }

    #[tokio::test]
    async fn noise_knows_the_device_by_its_signing_key() {
        let (_dir, app) = app(4);
        let addr = hub(app.clone(), Mode::Noise).await;
        let hub_key = geri_sign::public_key(&TEST_HUB_SEED);
        let mut transport = TcpTransport::noise(&addr, TEST_SEED, hub_key);
        hello_and_ring(&app, &mut transport).await;
    }

    #[tokio::test]
    async fn noise_from_an_unknown_device_gets_nothing() {
        let (_dir, app) = app(4);
        let addr = hub(app, Mode::Noise).await;
        let hub_key = geri_sign::public_key(&TEST_HUB_SEED);
        let mut transport = TcpTransport::noise(&addr, [1; 32], hub_key);
        transport.connect().await.unwrap();
        assert!(matches!(
            transport.recv().await,
            Err(Error::Closed | Error::Io(_))
        ));
    }

    #[tokio::test]
    async fn plain_tcp_takes_a_token_in_hello() {
        let (_dir, app) = app(4);
        let addr = hub(app.clone(), Mode::Plain).await;
        let mut transport = TcpTransport::new(&addr, TOKEN);
        hello_and_ring(&app, &mut transport).await;

        let mut wrong = TcpTransport::new(&addr, "nope-token-1234567");
        match wrong.connect().await {
            Err(Error::Rejected {
                status: 401, code, ..
            }) => assert_eq!(code, "unauthorized"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn plain_tcp_takes_a_signature_and_a_fresh_device_learns_the_counter() {
        let (_dir, app) = app(4);
        let addr = hub(app.clone(), Mode::Plain).await;
        app.store.accept_counter("router", 100).unwrap().unwrap();
        let signer = Arc::new(geri_sign::Signer::new("router", TEST_SEED).unwrap());
        let mut transport = TcpTransport::new(&addr, "").with_signer(signer);
        hello_and_ring(&app, &mut transport).await;
        assert_eq!(app.store.accept_counter("router", 101).unwrap(), Err(101));
    }

    #[tokio::test]
    async fn plain_tcp_refuses_anything_before_an_authenticated_hello() {
        let (_dir, app) = app(4);
        let addr = hub(app, Mode::Plain).await;
        let mut io = TcpStream::connect(&addr).await.unwrap();
        let mut conn = Conn::new(&mut io);
        let claim = Frame {
            id: Some(3),
            msg: ToHub::Claim {
                source_id: "router".into(),
            },
        };
        let payload = geri_binding_stream::encode(&claim, Codec::Json).unwrap();
        let mut out = Vec::new();
        geri_binding_stream::write_frame(&payload, &mut out);
        conn.io.write_all(&out).await.unwrap();
        let reply: Frame<ToSource> =
            geri_binding_stream::decode(&conn.recv().await.unwrap()).unwrap();
        assert_eq!(reply.id, Some(3));
        assert!(matches!(reply.msg, ToSource::Error { status: 401, .. }));
    }

    #[tokio::test]
    async fn noise_answers_busy_to_the_hello() {
        let (_dir, app) = app(1);
        let addr = hub(app.clone(), Mode::Noise).await;
        let hub_key = geri_sign::public_key(&TEST_HUB_SEED);
        let mut first = TcpTransport::noise(&addr, TEST_SEED, hub_key);
        hello_and_ring(&app, &mut first).await;
        let mut second = TcpTransport::noise(&addr, TEST_SEED, hub_key);
        second.connect().await.unwrap();
        second
            .send(Frame {
                id: Some(1),
                msg: ToHub::Hello {
                    v: VERSION,
                    auth: None,
                },
            })
            .await
            .unwrap();
        let reply = second.recv().await.unwrap();
        assert_eq!(reply.id, Some(1));
        assert!(
            matches!(reply.msg, ToSource::Error { status: 503, .. }),
            "{reply:?}"
        );
    }

    #[tokio::test]
    async fn the_connection_limit_is_shared_and_answers_busy() {
        let (_dir, app) = app(1);
        let addr = hub(app.clone(), Mode::Plain).await;
        let mut first = TcpTransport::new(&addr, TOKEN);
        hello_and_ring(&app, &mut first).await;
        let mut second = TcpTransport::new(&addr, TOKEN);
        match second.connect().await {
            Err(Error::Rejected {
                status: 503, code, ..
            }) => assert_eq!(code, "busy"),
            other => panic!("{other:?}"),
        }
        drop(first);
        tokio::time::sleep(Duration::from_millis(50)).await;
        second.connect().await.unwrap();
    }
}
