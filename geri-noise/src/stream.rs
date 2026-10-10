use crate::{Cipher, Error, HANDSHAKE_BUFFER, MAX_MESSAGE, MAX_PLAINTEXT, OsHandshake, TAG};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
    vec::Vec,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Noise(#[from] Error),
}

pub async fn connect<IO>(
    mut io: IO,
    seed: &[u8; 32],
    hub: &[u8; 32],
) -> Result<NoiseStream<IO>, StreamError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut handshake = OsHandshake::initiator(seed, hub)?;
    let mut buf = [0u8; HANDSHAKE_BUFFER];
    let n = handshake.write(&mut buf)?;
    write_message(&mut io, &buf[..n]).await?;
    let reply = read_message(&mut io, HANDSHAKE_BUFFER).await?;
    handshake.read(&reply)?;
    Ok(NoiseStream::new(io, handshake.finish()?))
}

pub async fn accept<IO>(
    mut io: IO,
    seed: &[u8; 32],
) -> Result<(NoiseStream<IO>, [u8; 32]), StreamError>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut handshake = OsHandshake::responder(seed)?;
    let first = read_message(&mut io, HANDSHAKE_BUFFER).await?;
    handshake.read(&first)?;
    let remote = handshake.remote_static().ok_or(Error::Handshake(
        clatter::error::HandshakeError::InvalidState,
    ))?;
    let mut buf = [0u8; HANDSHAKE_BUFFER];
    let n = handshake.write(&mut buf)?;
    write_message(&mut io, &buf[..n]).await?;
    Ok((NoiseStream::new(io, handshake.finish()?), remote))
}

async fn write_message<IO: AsyncWrite + Unpin>(io: &mut IO, message: &[u8]) -> io::Result<()> {
    let mut out = Vec::with_capacity(2 + message.len());
    out.extend_from_slice(&(message.len() as u16).to_be_bytes());
    out.extend_from_slice(message);
    io.write_all(&out).await?;
    io.flush().await
}

async fn read_message<IO: AsyncRead + Unpin>(io: &mut IO, max: usize) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 2];
    io.read_exact(&mut len).await?;
    let len = usize::from(u16::from_be_bytes(len));
    if len == 0 || len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad noise message length",
        ));
    }
    let mut message = std::vec![0u8; len];
    io.read_exact(&mut message).await?;
    Ok(message)
}

#[derive(Debug)]
pub struct NoiseStream<IO> {
    pub(crate) io: IO,
    cipher: Cipher,
    rx: Vec<u8>,
    plain: Vec<u8>,
    plain_at: usize,
    tx: Vec<u8>,
    tx_at: usize,
}

impl<IO> NoiseStream<IO> {
    fn new(io: IO, cipher: Cipher) -> Self {
        Self {
            io,
            cipher,
            rx: Vec::new(),
            plain: Vec::new(),
            plain_at: 0,
            tx: Vec::new(),
            tx_at: 0,
        }
    }

    pub fn get_ref(&self) -> &IO {
        &self.io
    }
}

fn invalid(e: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

impl<IO: AsyncWrite + Unpin> NoiseStream<IO> {
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.tx_at < self.tx.len() {
            let n = ready!(Pin::new(&mut self.io).poll_write(cx, &self.tx[self.tx_at..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.tx_at += n;
        }
        self.tx.clear();
        self.tx_at = 0;
        Poll::Ready(Ok(()))
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for NoiseStream<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if this.plain_at < this.plain.len() {
                let n = buf.remaining().min(this.plain.len() - this.plain_at);
                buf.put_slice(&this.plain[this.plain_at..this.plain_at + n]);
                this.plain_at += n;
                return Poll::Ready(Ok(()));
            }
            if this.rx.len() >= 2 {
                let len = usize::from(u16::from_be_bytes([this.rx[0], this.rx[1]]));
                if len < TAG {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "noise message shorter than its tag",
                    )));
                }
                if this.rx.len() >= 2 + len {
                    this.plain.resize(len - TAG, 0);
                    let n = this
                        .cipher
                        .decrypt(&this.rx[2..2 + len], &mut this.plain)
                        .map_err(invalid)?;
                    this.plain.truncate(n);
                    this.plain_at = 0;
                    this.rx.drain(..2 + len);
                    continue;
                }
            }
            let mut chunk = [0u8; 8 * 1024];
            let mut chunk = ReadBuf::new(&mut chunk);
            ready!(Pin::new(&mut this.io).poll_read(cx, &mut chunk))?;
            if chunk.filled().is_empty() {
                return Poll::Ready(if this.rx.is_empty() {
                    Ok(())
                } else {
                    Err(io::ErrorKind::UnexpectedEof.into())
                });
            }
            this.rx.extend_from_slice(chunk.filled());
        }
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for NoiseStream<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let n = buf.len().min(MAX_PLAINTEXT);
        this.tx.resize(2 + n + TAG, 0);
        let len = this
            .cipher
            .encrypt(&buf[..n], &mut this.tx[2..])
            .map_err(invalid)?;
        debug_assert!(len <= MAX_MESSAGE);
        this.tx[..2].copy_from_slice(&(len as u16).to_be_bytes());
        this.tx.truncate(2 + len);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}
