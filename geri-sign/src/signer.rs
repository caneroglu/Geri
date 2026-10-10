use crate::{Authorization, parse_key, valid_agent};
use core::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use ed25519_dalek::{Signer as _, SigningKey};
use std::string::{String, ToString};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    Agent,
    Key,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Agent => "agent id must be 1-64 bytes of A-Za-z0-9-_.",
            Self::Key => "key must be 64 hex characters (32 bytes)",
        })
    }
}

impl std::error::Error for Error {}

pub struct Signer {
    agent: String,
    key: SigningKey,
    next: AtomicU64,
}

impl Signer {
    pub fn new(agent: impl Into<String>, seed: [u8; 32]) -> Result<Self, Error> {
        let agent = agent.into();
        if !valid_agent(&agent) {
            return Err(Error::Agent);
        }
        Ok(Self {
            agent,
            key: SigningKey::from_bytes(&seed),
            next: AtomicU64::new(1),
        })
    }

    pub fn from_hex(agent: impl Into<String>, seed: &str) -> Result<Self, Error> {
        Self::new(agent, parse_key(seed).ok_or(Error::Key)?)
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.key.verifying_key().to_bytes()
    }

    pub fn authorization(&self, method: &str, path_and_query: &str, body: &[u8]) -> String {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let digest = crate::digest(&self.agent, n, method, path_and_query, body);
        Authorization {
            agent: &self.agent,
            n,
            signature: self.key.sign(&digest).to_bytes(),
        }
        .to_string()
    }

    pub fn learn(&self, counter: u64) {
        self.next
            .fetch_max(counter.saturating_add(1), Ordering::Relaxed);
    }
}

impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signer")
            .field("agent", &self.agent)
            .field("next", &self.next.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}
