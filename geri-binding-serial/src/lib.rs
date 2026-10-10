// Testler Claude tarafından yazılmıştır.

#![no_std]

extern crate alloc;

use alloc::{collections::VecDeque, vec::Vec};

pub use geri_binding_stream::{Codec, Error as CodecError, decode, encode};

#[cfg(not(feature = "crc-no-table"))]
const CRC: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
#[cfg(feature = "crc-no-table")]
const CRC: crc::Crc<u32, crc::NoTable> = crc::Crc::<u32, crc::NoTable>::new(&crc::CRC_32_ISO_HDLC);

pub const CRC_LEN: usize = 4;

pub const fn wire_len(payload: usize) -> usize {
    cobs::max_encoding_length(payload + CRC_LEN) + 1
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    Frame(Vec<u8>),
    Reset,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("corrupt frame")]
    Corrupt,
    #[error("crc mismatch")]
    Crc,
    #[error("frame exceeds the {max} byte limit")]
    TooLarge { max: usize },
}

pub fn write_frame(payload: &[u8], out: &mut Vec<u8>) {
    let mut raw = Vec::with_capacity(payload.len() + CRC_LEN);
    raw.extend_from_slice(payload);
    raw.extend_from_slice(&CRC.checksum(payload).to_le_bytes());
    let start = out.len();
    out.resize(start + cobs::max_encoding_length(raw.len()), 0);
    let n = cobs::encode(&raw, &mut out[start..]);
    out.truncate(start + n);
    out.push(0);
}

pub fn write_reset(out: &mut Vec<u8>) {
    out.push(0);
    write_frame(&[], out);
}

/// Seri hattan gelen baytları sırayla çerçeveye çevirir.
///
/// Akış halinde `cobs` decoder'ı kullanır: bellekte en fazla `max + CRC_LEN` baytlık
/// tek çerçeve durur, boyut sınırı çözülmüş payload'a uygulanır. Sonuçlar (hatalar dahil)
/// hatta geldikleri sırayla kuyruğa girer.
#[derive(Debug)]
pub struct Reader {
    dec: cobs::CobsDecoderOwned,
    max: usize,
    skipping: bool,
    items: VecDeque<Result<Item, Error>>,
}

impl Reader {
    pub fn new(max_payload: usize) -> Self {
        Self {
            dec: cobs::CobsDecoderOwned::new(max_payload + CRC_LEN),
            max: max_payload,
            skipping: false,
            items: VecDeque::new(),
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        for &b in bytes {
            if self.skipping {
                // Taşan çerçevenin kalanı: sonraki ayraca kadar at.
                self.skipping = b != 0;
                continue;
            }
            let item = match self.dec.feed(b) {
                Ok(None) => continue,
                Ok(Some(n)) => Self::open(&self.dec.dest()[..n]),
                Err(cobs::DecodeError::TargetBufTooSmall) => {
                    // Decoder taşınca kendini sıfırlamıyor; kırpılmış çerçeve vermesin.
                    self.dec.reset();
                    self.skipping = true;
                    Err(Error::TooLarge { max: self.max })
                }
                // Erken gelen 0x00 ayraçtır; decoder zaten boşta.
                Err(_) => Err(Error::Corrupt),
            };
            self.items.push_back(item);
        }
    }

    pub fn next_item(&mut self) -> Option<Result<Item, Error>> {
        self.items.pop_front()
    }

    fn open(raw: &[u8]) -> Result<Item, Error> {
        let Some(split) = raw.len().checked_sub(CRC_LEN) else {
            return Err(Error::Corrupt);
        };
        let (payload, crc) = raw.split_at(split);
        if CRC.checksum(payload).to_le_bytes() != crc {
            return Err(Error::Crc);
        }
        Ok(if payload.is_empty() {
            Item::Reset
        } else {
            Item::Frame(payload.to_vec())
        })
    }
}

#[cfg(test)]
mod tests;
