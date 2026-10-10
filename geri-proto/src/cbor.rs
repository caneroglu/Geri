use alloc::vec::Vec;
use core::convert::Infallible;
use serde::{Deserialize, Serialize};

pub use minicbor_serde::error::{DecodeError, EncodeError};

pub fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, EncodeError<Infallible>> {
    let mut bytes = Vec::new();
    value.serialize(minicbor_serde::Serializer::new(&mut bytes).serialize_unit_as_null(true))?;
    Ok(bytes)
}

pub fn from_slice<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, DecodeError> {
    minicbor_serde::from_slice(bytes)
}

pub fn encoded_len<T: Serialize + ?Sized>(value: &T) -> Result<usize, EncodeError<Infallible>> {
    let mut counter = Counter(0);
    value.serialize(minicbor_serde::Serializer::new(&mut counter).serialize_unit_as_null(true))?;
    Ok(counter.0)
}

struct Counter(usize);

impl minicbor::encode::Write for Counter {
    type Error = Infallible;

    fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(())
    }
}
