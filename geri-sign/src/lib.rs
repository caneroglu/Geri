// Testler Claude tarafından yazılmıştır.

#![no_std]
#![warn(missing_docs)]

#[cfg(feature = "std")]
extern crate std;

use core::fmt;
use ed25519_dalek::{Signature, Signer as _, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};

pub const SCHEME: &str = "Geri-Ed25519";
pub const COUNTER_HEADER: &str = "Geri-Counter";
pub const WINDOW: u64 = 64;
const DOMAIN: &[u8] = b"geri-sig-1\0";

pub fn valid_agent(agent: &str) -> bool {
    !agent.is_empty()
        && agent.len() <= 64
        && agent
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub fn digest(agent: &str, n: u64, method: &str, path_and_query: &str, body: &[u8]) -> [u8; 32] {
    let body = Sha256::digest(body);
    let mut h = Sha256::new();
    h.update(DOMAIN);
    h.update(agent);
    h.update([0]);
    h.update(n.to_be_bytes());
    h.update(method);
    h.update(b" ");
    h.update(path_and_query);
    h.update([0]);
    h.update(body);
    h.finalize().into()
}

pub fn public_key(seed: &[u8; 32]) -> [u8; 32] {
    SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

pub fn sign(
    seed: &[u8; 32],
    agent: &str,
    n: u64,
    method: &str,
    path_and_query: &str,
    body: &[u8],
) -> [u8; 64] {
    SigningKey::from_bytes(seed)
        .sign(&digest(agent, n, method, path_and_query, body))
        .to_bytes()
}

pub fn parse_key(hex: &str) -> Option<[u8; 32]> {
    let mut key = [0; 32];
    hex::decode_to_slice(hex.trim(), &mut key).ok()?;
    Some(key)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Authorization<'a> {
    pub agent: &'a str,
    pub n: u64,
    pub signature: [u8; 64],
}

impl<'a> Authorization<'a> {
    pub fn parse(value: &'a str) -> Option<Self> {
        let rest = value.strip_prefix(SCHEME)?.strip_prefix(' ')?;
        let (agent, rest) = rest.split_once(':')?;
        let (n, signature) = rest.split_once(':')?;
        if !valid_agent(agent) || !n.bytes().all(|b| b.is_ascii_digit()) || n.starts_with('0') {
            return None;
        }
        let n = n.parse().ok()?;
        let mut sig = [0; 64];
        hex::decode_to_slice(signature, &mut sig).ok()?;
        Some(Self {
            agent,
            n,
            signature: sig,
        })
    }

    pub fn verify(
        &self,
        public: &[u8; 32],
        method: &str,
        path_and_query: &str,
        body: &[u8],
    ) -> bool {
        let Ok(key) = VerifyingKey::from_bytes(public) else {
            return false;
        };
        let digest = digest(self.agent, self.n, method, path_and_query, body);
        key.verify_strict(&digest, &Signature::from_bytes(&self.signature))
            .is_ok()
    }
}

impl fmt::Display for Authorization<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{SCHEME} {}:{}:", self.agent, self.n)?;
        self.signature.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

impl fmt::Debug for Authorization<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authorization")
            .field("agent", &self.agent)
            .field("n", &self.n)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Window {
    pub max: u64,
    pub seen: u64,
}

impl Window {
    pub fn check(&self, n: u64) -> bool {
        n > self.max || (n > 0 && self.max - n < WINDOW && self.seen & (1 << (self.max - n)) == 0)
    }

    pub fn accept(&mut self, n: u64) -> bool {
        if !self.check(n) {
            return false;
        }
        if n > self.max {
            let shift = n - self.max;
            self.seen = if shift >= WINDOW {
                0
            } else {
                self.seen << shift
            };
            self.seen |= 1;
            self.max = n;
        } else {
            self.seen |= 1 << (self.max - n);
        }
        true
    }
}

pub fn counter(value: &str) -> Option<u64> {
    value.trim().parse().ok()
}

#[cfg(feature = "std")]
mod signer;
#[cfg(feature = "std")]
pub use signer::{Error, Signer};

#[cfg(test)]
mod tests;
