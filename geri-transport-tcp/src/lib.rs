// Testler Claude tarafından yazılmıştır.

use geri_adapter_sdk::Transport;
use geri_binding_stream::Reader;
use geri_proto::v4::{
    VERSION,
    msg::{Frame, ToHub, ToSource},
};
use geri_sign::Signer;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    time::Instant,
};

pub use geri_binding_stream::Codec;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub const DEFAULT_MAX_FRAME: usize = 16 * 1024 * 1024;

pub use geri_binding_stream::{SIGNED_METHOD, SIGNED_TARGET};

const HELLO_ID: u32 = 0;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("connect timed out")]
    ConnectTimeout,
    #[error("connection closed")]
    Closed,
    #[error("frame: {0}")]
    Frame(#[from] geri_binding_stream::Error),
    #[error("hub rejected the connection: {status} {code}: {message}")]
    Rejected {
        status: u16,
        code: String,
        message: String,
    },
    #[error("unexpected reply to hello")]
    Unexpected,
    #[error("hub stopped answering")]
    Silent,
    #[cfg(feature = "noise")]
    #[error("noise: {0}")]
    Noise(#[from] geri_noise::StreamError),
}

pub trait Dialer {
    type Io: AsyncRead + AsyncWrite + Unpin;

    fn proves_identity(&self) -> bool {
        false
    }

    fn dial(&mut self) -> impl Future<Output = Result<Self::Io, Error>>;
}

enum Auth {
    Token(String),
    Signer(Arc<Signer>),
}

pub struct StreamTransport<D: Dialer> {
    dialer: D,
    auth: Auth,
    codec: Codec,
    keepalive: Duration,
    max_frame: usize,
    conn: Option<D::Io>,
    reader: Reader,
    ping_sent: Option<Instant>,
    next_ping: Instant,
}

impl<D: Dialer> std::fmt::Debug for StreamTransport<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamTransport")
            .field("connected", &self.conn.is_some())
            .field("codec", &self.codec)
            .finish_non_exhaustive()
    }
}

pub type TcpTransport = StreamTransport<TcpDialer>;

#[derive(Debug)]
pub struct TcpDialer {
    addr: String,
    #[cfg(feature = "noise")]
    noise: Option<NoiseKeys>,
}

#[cfg(feature = "noise")]
struct NoiseKeys {
    seed: [u8; 32],
    hub: [u8; 32],
}

#[cfg(feature = "noise")]
impl std::fmt::Debug for NoiseKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NoiseKeys")
    }
}

impl Dialer for TcpDialer {
    type Io = Conn;

    fn proves_identity(&self) -> bool {
        #[cfg(feature = "noise")]
        return self.noise.is_some();
        #[cfg(not(feature = "noise"))]
        false
    }

    async fn dial(&mut self) -> Result<Conn, Error> {
        let stream = TcpStream::connect(&self.addr).await?;
        stream.set_nodelay(true)?;
        #[cfg(feature = "noise")]
        if let Some(NoiseKeys { seed, hub }) = &self.noise {
            return Ok(Conn::Noise(Box::new(
                geri_noise::connect(stream, seed, hub).await?,
            )));
        }
        Ok(Conn::Plain(stream))
    }
}

#[derive(Debug)]
pub enum Conn {
    Plain(TcpStream),
    #[cfg(feature = "noise")]
    Noise(Box<geri_noise::NoiseStream<TcpStream>>),
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(io) => Pin::new(io).poll_read(cx, buf),
            #[cfg(feature = "noise")]
            Self::Noise(io) => Pin::new(io.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(io) => Pin::new(io).poll_write(cx, buf),
            #[cfg(feature = "noise")]
            Self::Noise(io) => Pin::new(io.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(io) => Pin::new(io).poll_flush(cx),
            #[cfg(feature = "noise")]
            Self::Noise(io) => Pin::new(io.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(io) => Pin::new(io).poll_shutdown(cx),
            #[cfg(feature = "noise")]
            Self::Noise(io) => Pin::new(io.as_mut()).poll_shutdown(cx),
        }
    }
}

impl TcpTransport {
    pub fn new(addr: &str, token: impl Into<String>) -> Self {
        Self::over(TcpDialer::new(addr), token)
    }

    #[cfg(feature = "noise")]
    pub fn noise(addr: &str, seed: [u8; 32], hub: [u8; 32]) -> Self {
        let mut dialer = TcpDialer::new(addr);
        dialer.noise = Some(NoiseKeys { seed, hub });
        Self::over(dialer, "")
    }

    pub fn addr(&self) -> &str {
        &self.dialer.addr
    }
}

impl TcpDialer {
    fn new(addr: &str) -> Self {
        Self {
            addr: addr
                .strip_prefix("tcp://")
                .unwrap_or(addr)
                .trim_end_matches('/')
                .into(),
            #[cfg(feature = "noise")]
            noise: None,
        }
    }
}

impl<D: Dialer> StreamTransport<D> {
    pub fn over(dialer: D, token: impl Into<String>) -> Self {
        Self {
            dialer,
            auth: Auth::Token(token.into()),
            codec: Codec::Json,
            keepalive: Duration::from_secs(30),
            max_frame: DEFAULT_MAX_FRAME,
            conn: None,
            reader: Reader::new(DEFAULT_MAX_FRAME),
            ping_sent: None,
            next_ping: Instant::now(),
        }
    }

    pub fn with_signer(mut self, signer: Arc<Signer>) -> Self {
        self.auth = Auth::Signer(signer);
        self
    }

    pub fn with_codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn with_keepalive(mut self, every: Duration) -> Self {
        self.keepalive = every;
        self
    }

    pub fn with_max_frame(mut self, bytes: usize) -> Self {
        self.max_frame = bytes;
        self
    }

    pub fn dialer(&self) -> &D {
        &self.dialer
    }

    fn close(&mut self) {
        self.conn = None;
        self.ping_sent = None;
        self.reader = Reader::new(self.max_frame);
    }

    async fn write_frame(&mut self, frame: &Frame<ToHub>) -> Result<(), Error> {
        let payload = geri_binding_stream::encode(frame, self.codec)?;
        let mut out = Vec::with_capacity(payload.len() + 5);
        geri_binding_stream::write_frame(&payload, &mut out);
        let conn = self.conn.as_mut().ok_or(Error::Closed)?;
        let written = async {
            conn.write_all(&out).await?;
            conn.flush().await
        };
        if let Err(e) = written.await {
            self.close();
            return Err(e.into());
        }
        Ok(())
    }

    async fn read_frame(&mut self) -> Result<Frame<ToSource>, Error> {
        loop {
            match self.reader.next_frame() {
                Ok(Some(payload)) => return Ok(geri_binding_stream::decode(&payload)?),
                Ok(None) => {}
                Err(e) => {
                    self.close();
                    return Err(e.into());
                }
            }
            let conn = self.conn.as_mut().ok_or(Error::Closed)?;
            let mut buf = [0u8; 8 * 1024];
            match conn.read(&mut buf).await {
                Ok(0) => {
                    self.close();
                    return Err(Error::Closed);
                }
                Ok(n) => self.reader.push(&buf[..n]),
                Err(e) => {
                    self.close();
                    return Err(e.into());
                }
            }
        }
    }

    async fn authenticate(&mut self) -> Result<(), Error> {
        let mut learned = false;
        loop {
            let auth = match &self.auth {
                Auth::Token(token) => format!("Bearer {token}"),
                Auth::Signer(signer) => signer.authorization(SIGNED_METHOD, SIGNED_TARGET, b""),
            };
            let hello = Frame {
                id: Some(HELLO_ID),
                msg: ToHub::Hello {
                    v: VERSION,
                    auth: Some(auth),
                },
            };
            self.write_frame(&hello).await?;
            let reply = loop {
                let frame = self.read_frame().await?;
                if frame.id == Some(HELLO_ID) {
                    break frame.msg;
                }
            };
            match reply {
                ToSource::Welcome(_) => return Ok(()),
                ToSource::Error { status, error } => {
                    if let (Auth::Signer(signer), Some(counter), false) =
                        (&self.auth, error.counter, learned)
                        && error.code == "replay"
                    {
                        signer.learn(counter);
                        learned = true;
                        continue;
                    }
                    return Err(Error::Rejected {
                        status,
                        code: error.code,
                        message: error.message,
                    });
                }
                _ => return Err(Error::Unexpected),
            }
        }
    }

    async fn open(&mut self) -> Result<(), Error> {
        self.conn = Some(self.dialer.dial().await?);
        if self.dialer.proves_identity() {
            return Ok(());
        }
        self.authenticate().await
    }
}

impl<D: Dialer> Transport for StreamTransport<D> {
    type Error = Error;

    fn disconnect(&mut self) {
        self.close();
    }

    fn pushes(&self) -> bool {
        true
    }

    async fn connect(&mut self) -> Result<(), Error> {
        self.close();
        let result = match tokio::time::timeout(CONNECT_TIMEOUT, self.open()).await {
            Ok(result) => result,
            Err(_) => Err(Error::ConnectTimeout),
        };
        if result.is_err() {
            self.close();
        }
        result?;
        self.next_ping = Instant::now() + self.keepalive;
        Ok(())
    }

    async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), Error> {
        self.write_frame(&frame).await
    }

    async fn recv(&mut self) -> Result<Frame<ToSource>, Error> {
        loop {
            if self.conn.is_none() {
                return std::future::pending().await;
            }
            let next_ping = self.next_ping;
            let frame = tokio::select! {
                frame = self.read_frame() => Some(frame?),
                () = tokio::time::sleep_until(next_ping) => None,
            };
            let Some(frame) = frame else {
                if self.ping_sent.is_some() {
                    self.close();
                    return Err(Error::Silent);
                }
                let ping = Frame {
                    id: None,
                    msg: ToHub::Ping,
                };
                match tokio::time::timeout(self.keepalive, self.write_frame(&ping)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        self.close();
                        return Err(e);
                    }
                    Err(_) => {
                        self.close();
                        return Err(Error::Silent);
                    }
                }
                self.ping_sent = Some(Instant::now());
                self.next_ping = Instant::now() + self.keepalive;
                continue;
            };
            self.ping_sent = None;
            self.next_ping = Instant::now() + self.keepalive;
            if frame.id.is_none() && frame.msg == ToSource::Pong {
                continue;
            }
            return Ok(frame);
        }
    }
}

#[cfg(test)]
mod tests;
