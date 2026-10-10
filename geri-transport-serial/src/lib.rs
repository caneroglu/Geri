// Testler Claude tarafından yazılmıştır.

use geri_adapter_sdk::Transport;
use geri_binding_serial::{Item, Reader};
use geri_proto::v4::msg::{Frame, ToHub, ToSource};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod bridge;

pub use geri_binding_serial::Codec;
pub use tokio_serial::SerialStream;

pub const DEFAULT_MAX_FRAME: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("serial port: {0}")]
    Port(#[from] tokio_serial::Error),
    #[error("serial io: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame: {0}")]
    Frame(#[from] geri_binding_serial::CodecError),
    #[error("serial line closed")]
    Closed,
    #[error("link reset by the other side")]
    Reset,
}

struct Opener<IO>(Box<dyn FnMut() -> Result<IO, Error> + Send + Sync>);

impl<IO> std::fmt::Debug for Opener<IO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Opener")
    }
}

#[derive(Debug)]
pub struct SerialTransport<IO = SerialStream> {
    io: Option<IO>,
    opener: Option<Opener<IO>>,
    reader: Reader,
    codec: Codec,
    max_frame: usize,
    dropped: u64,
}

impl SerialTransport<SerialStream> {
    pub fn open(path: &str, baud: u32) -> Result<Self, Error> {
        let port = tokio_serial::SerialStream::open(&tokio_serial::new(path, baud))?;
        let path = path.to_owned();
        Ok(Self::new(port).with_reopener(move || {
            Ok(tokio_serial::SerialStream::open(&tokio_serial::new(
                &path, baud,
            ))?)
        }))
    }

    /// Ölü portu yeniden açar (F06): eski IO bırakılır, yarım çerçeve atılır,
    /// sıradaki `connect` reset el sıkışmasıyla yeniden senkron olur.
    pub fn reopen_path(&mut self, path: &str, baud: u32) -> Result<(), Error> {
        let port = tokio_serial::SerialStream::open(&tokio_serial::new(path, baud))?;
        self.reopen(port);
        let path = path.to_owned();
        self.opener = Some(Opener(Box::new(move || {
            Ok(tokio_serial::SerialStream::open(&tokio_serial::new(
                &path, baud,
            ))?)
        })));
        Ok(())
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> SerialTransport<IO> {
    pub fn new(io: IO) -> Self {
        Self {
            io: Some(io),
            opener: None,
            reader: Reader::new(DEFAULT_MAX_FRAME),
            codec: Codec::Cbor,
            max_frame: DEFAULT_MAX_FRAME,
            dropped: 0,
        }
    }

    pub fn with_codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn with_max_frame(mut self, bytes: usize) -> Self {
        self.reader = Reader::new(bytes);
        self.max_frame = bytes;
        self
    }

    /// Kopma sonrası `connect` için yeni IO fabrikası. `open` bunu fiziksel
    /// port için kendisi kurar; özel IO'da çağıran kendi yeniden açma yolunu verir.
    pub fn with_reopener(
        mut self,
        open: impl FnMut() -> Result<IO, Error> + Send + Sync + 'static,
    ) -> Self {
        self.opener = Some(Opener(Box::new(open)));
        self
    }

    /// Kopuk IO yerine yeni tutamaç takar; eski reader kuyruğunu bırakır.
    pub fn reopen(&mut self, io: IO) {
        self.io = Some(io);
        self.reader = Reader::new(self.max_frame);
    }

    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    pub(crate) async fn write_reset(&mut self) -> Result<(), Error> {
        let mut out = Vec::new();
        geri_binding_serial::write_reset(&mut out);
        self.write(&out).await
    }

    pub(crate) async fn write_frame<M: Serialize>(
        &mut self,
        frame: &Frame<M>,
    ) -> Result<(), Error> {
        let payload = geri_binding_serial::encode(frame, self.codec)?;
        let mut out = Vec::with_capacity(geri_binding_serial::wire_len(payload.len()));
        geri_binding_serial::write_frame(&payload, &mut out);
        self.write(&out).await
    }

    async fn write(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let io = self.io.as_mut().ok_or(Error::Closed)?;
        io.write_all(bytes).await?;
        io.flush().await?;
        Ok(())
    }

    pub(crate) async fn next_item(&mut self) -> Result<Item, Error> {
        let mut buf = [0u8; 512];
        loop {
            while let Some(item) = self.reader.next_item() {
                match item {
                    Ok(item) => return Ok(item),
                    Err(_) => self.dropped += 1,
                }
            }
            let n = self
                .io
                .as_mut()
                .ok_or(Error::Closed)?
                .read(&mut buf)
                .await?;
            if n == 0 {
                return Err(Error::Closed);
            }
            self.reader.push(&buf[..n]);
        }
    }

    pub(crate) fn set_codec(&mut self, codec: Codec) {
        self.codec = codec;
    }
}

impl<IO: AsyncRead + AsyncWrite + Unpin> Transport for SerialTransport<IO> {
    type Error = Error;

    fn pushes(&self) -> bool {
        true
    }

    fn disconnect(&mut self) {
        // Fiziksel port fabrikası varsa tanıtıcı bırakılır ve yeniden açılır.
        // Sabit IO (örn. duplex) reset ile aynı hat üzerinde yeniden eşlenir.
        if self.opener.is_some() {
            self.io = None;
        }
        self.reader = Reader::new(self.max_frame);
    }

    async fn connect(&mut self) -> Result<(), Error> {
        if self.io.is_none() {
            self.io = Some((self.opener.as_mut().ok_or(Error::Closed)?.0)()?);
        }
        self.reader = Reader::new(self.max_frame);
        // Binding reset'i önce ayraç, sonra CRC'li boş çerçeve yazar.
        self.write_reset().await
    }

    async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), Error> {
        self.write_frame(&frame).await
    }

    async fn recv(&mut self) -> Result<Frame<ToSource>, Error> {
        loop {
            match self.next_item().await? {
                Item::Reset => return Err(Error::Reset),
                Item::Frame(payload) => match geri_binding_serial::decode(&payload) {
                    Ok(frame) => return Ok(frame),
                    Err(_) => self.dropped += 1,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests;
