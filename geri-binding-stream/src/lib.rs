// Testler Claude tarafından yazılmıştır.

#![no_std]

extern crate alloc;

use alloc::{string::String, string::ToString, vec::Vec};
use geri_proto::{cbor, v4::msg::Frame};
use serde::{Serialize, de::DeserializeOwned};

pub const SIGNED_METHOD: &str = "HELLO";
pub const SIGNED_TARGET: &str = "/v4/tcp";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Codec {
    #[default]
    Json,
    Cbor,
}

impl Codec {
    pub fn of(payload: &[u8]) -> Option<Self> {
        match payload.iter().find(|b| !b.is_ascii_whitespace())? {
            b'{' => Some(Self::Json),
            0xa0..=0xbf => Some(Self::Cbor),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("encode failed: {0}")]
    Encode(String),
    #[error("decode failed: {0}")]
    Decode(String),
    #[error("frame is neither a JSON object nor a CBOR map")]
    UnknownCodec,
    #[error("frame of {len} bytes exceeds the {max} byte limit")]
    TooLarge { len: usize, max: usize },
    #[error("malformed length prefix")]
    BadLength,
}

pub fn encode<M: Serialize>(frame: &Frame<M>, codec: Codec) -> Result<Vec<u8>, Error> {
    match codec {
        Codec::Json => serde_json::to_vec(frame).map_err(|e| Error::Encode(e.to_string())),
        Codec::Cbor => cbor::to_vec(frame).map_err(|e| Error::Encode(e.to_string())),
    }
}

pub fn decode<M: DeserializeOwned>(payload: &[u8]) -> Result<Frame<M>, Error> {
    match Codec::of(payload).ok_or(Error::UnknownCodec)? {
        Codec::Json => serde_json::from_slice(payload).map_err(|e| Error::Decode(e.to_string())),
        Codec::Cbor => cbor::from_slice(payload).map_err(|e| Error::Decode(e.to_string())),
    }
}

pub fn write_frame(payload: &[u8], out: &mut Vec<u8>) {
    let mut len = unsigned_varint::encode::u32_buffer();
    out.extend_from_slice(unsigned_varint::encode::u32(payload.len() as u32, &mut len));
    out.extend_from_slice(payload);
}

#[derive(Debug)]
pub struct Reader {
    buf: Vec<u8>,
    max: usize,
}

impl Reader {
    pub fn new(max_frame: usize) -> Self {
        Self {
            buf: Vec::new(),
            max: max_frame,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, Error> {
        use unsigned_varint::decode;
        let prefix = &self.buf[..self.buf.len().min(5)];
        let (len, rest) = match decode::u64(prefix) {
            Ok(ok) => ok,
            Err(decode::Error::Insufficient) if prefix.len() < 5 => return Ok(None),
            Err(_) => return Err(Error::BadLength),
        };
        if len == 0 || len > u64::from(u32::MAX) {
            return Err(Error::BadLength);
        }
        let len = len as usize;
        if len > self.max {
            return Err(Error::TooLarge { len, max: self.max });
        }
        let start = prefix.len() - rest.len();
        if self.buf.len() < start + len {
            return Ok(None);
        }
        let frame = self.buf[start..start + len].to_vec();
        self.buf.drain(..start + len);
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests;
