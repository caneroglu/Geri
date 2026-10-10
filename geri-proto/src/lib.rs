// Testler Claude tarafından yazılmıştır.

#![no_std]
#![warn(missing_docs)]

extern crate alloc;

use alloc::{collections::BTreeMap, string::String};

#[cfg(feature = "cbor")]
pub mod cbor;
mod control;
pub mod v4;
mod value;
pub use control::{Control, Danger, Invalid, PRESS, SET, Shape};
pub use value::{MAX_DEPTH, Plain, Value};

pub const VERSION: u16 = v4::VERSION;

pub type Values = BTreeMap<String, Value>;

#[cfg(test)]
mod tests;
