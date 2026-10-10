// Testler Claude tarafından yazılmıştır.

#![no_std]

#[cfg(feature = "tokio")]
extern crate std;

use clatter::{
    KeyPair, NqHandshakeCore,
    bytearray::SensitiveByteArray,
    crypto::{cipher::ChaChaPoly, dh::X25519, hash::Blake2s},
    handshakepattern::noise_ik,
    traits::Handshaker,
    transportstate::TransportState,
};
use ed25519_dalek::{SigningKey, VerifyingKey};

#[cfg(feature = "getrandom")]
pub use clatter::crypto::rng::DefaultRng;
pub use clatter::traits::Rng;

#[cfg(feature = "tokio")]
pub(crate) mod stream;
#[cfg(feature = "tokio")]
pub use stream::{NoiseStream, StreamError, accept, connect};

pub const PROLOGUE: &[u8] = b"geri-noise-1";
pub const MAX_MESSAGE: usize = 65_535;
pub const TAG: usize = 16;
pub const MAX_PLAINTEXT: usize = MAX_MESSAGE - TAG;
pub const HANDSHAKE_BUFFER: usize = 128;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid public key")]
    BadKey,
    #[error("handshake failed: {0}")]
    Handshake(clatter::error::HandshakeError),
    #[error("handshake message carries a payload")]
    Payload,
    #[error("transport: {0}")]
    Transport(clatter::error::TransportError),
}

impl From<clatter::error::HandshakeError> for Error {
    fn from(e: clatter::error::HandshakeError) -> Self {
        Self::Handshake(e)
    }
}

impl From<clatter::error::TransportError> for Error {
    fn from(e: clatter::error::TransportError) -> Self {
        Self::Transport(e)
    }
}

fn x25519_keypair(seed: &[u8; 32]) -> KeyPair<[u8; 32], SensitiveByteArray<[u8; 32]>> {
    let key = SigningKey::from_bytes(seed);
    KeyPair {
        public: key.verifying_key().to_montgomery().to_bytes(),
        secret: SensitiveByteArray::new(key.to_scalar_bytes()),
    }
}

pub fn x25519_public(ed25519: &[u8; 32]) -> Option<[u8; 32]> {
    let key = VerifyingKey::from_bytes(ed25519).ok()?;
    if key.is_weak() {
        return None;
    }
    Some(key.to_montgomery().to_bytes())
}

pub fn matches(remote_x25519: &[u8; 32], ed25519: &[u8; 32]) -> bool {
    x25519_public(ed25519).is_some_and(|x| &x == remote_x25519)
}

#[cfg(feature = "getrandom")]
pub type OsHandshake = Handshake<DefaultRng>;

pub struct Handshake<R: Rng> {
    state: NqHandshakeCore<X25519, ChaChaPoly, Blake2s, R>,
}

impl<R: Rng> core::fmt::Debug for Handshake<R> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Handshake")
            .field("finished", &self.state.is_finished())
            .finish_non_exhaustive()
    }
}

impl<R: Rng> Handshake<R> {
    pub fn initiator(seed: &[u8; 32], hub: &[u8; 32]) -> Result<Self, Error> {
        let hub = x25519_public(hub).ok_or(Error::BadKey)?;
        let state = NqHandshakeCore::new(
            noise_ik(),
            PROLOGUE,
            true,
            Some(x25519_keypair(seed)),
            None,
            Some(hub),
            None,
        )?;
        Ok(Self { state })
    }

    pub fn responder(seed: &[u8; 32]) -> Result<Self, Error> {
        let state = NqHandshakeCore::new(
            noise_ik(),
            PROLOGUE,
            false,
            Some(x25519_keypair(seed)),
            None,
            None,
            None,
        )?;
        Ok(Self { state })
    }

    pub fn write(&mut self, out: &mut [u8]) -> Result<usize, Error> {
        Ok(self.state.write_message(&[], out)?)
    }

    pub fn read(&mut self, message: &[u8]) -> Result<(), Error> {
        let mut payload = [0u8; HANDSHAKE_BUFFER];
        match self.state.read_message(message, &mut payload)? {
            0 => Ok(()),
            _ => Err(Error::Payload),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }

    pub fn remote_static(&self) -> Option<[u8; 32]> {
        self.state.get_remote_static()
    }

    pub fn finish(self) -> Result<Cipher, Error> {
        Ok(Cipher {
            state: self.state.finalize()?,
        })
    }
}

pub struct Cipher {
    state: TransportState<ChaChaPoly, Blake2s>,
}

impl core::fmt::Debug for Cipher {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Cipher").finish_non_exhaustive()
    }
}

impl Cipher {
    pub fn encrypt(&mut self, plain: &[u8], out: &mut [u8]) -> Result<usize, Error> {
        Ok(self.state.send(plain, out)?)
    }

    pub fn decrypt(&mut self, message: &[u8], out: &mut [u8]) -> Result<usize, Error> {
        Ok(self.state.receive(message, out)?)
    }
}

#[cfg(test)]
mod tests;
