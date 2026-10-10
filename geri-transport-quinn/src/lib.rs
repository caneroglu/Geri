// Testler Claude tarafından yazılmıştır.

use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use geri_transport_tcp::{Dialer, Error, StreamTransport};
use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, WebPkiSupportedAlgorithms, verify_tls13_signature_with_raw_key},
    pki_types::{
        CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer,
        UnixTime,
    },
    server::AlwaysResolvesServerRawPublicKeys,
    sign::CertifiedKey,
};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite, Join, ReadBuf};

pub const ALPN: &[u8] = b"geri";

const KEEPALIVE: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("key: {0}")]
    Key(String),
    #[error("tls: {0}")]
    Tls(#[from] rustls::Error),
    #[error("quic: {0}")]
    Quic(String),
}

pub type QuicTransport = StreamTransport<QuicDialer>;

pub struct QuicDialer {
    addr: String,
    server_name: String,
    config: ClientConfig,
    endpoint: Option<Endpoint>,
}

impl std::fmt::Debug for QuicDialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicDialer")
            .field("addr", &self.addr)
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

pub struct QuicStream {
    io: Join<RecvStream, SendStream>,
    conn: Connection,
}

impl Drop for QuicStream {
    fn drop(&mut self) {
        self.conn.close(0u32.into(), b"bye");
    }
}

impl AsyncRead for QuicStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

impl Dialer for QuicDialer {
    type Io = QuicStream;

    async fn dial(&mut self) -> Result<QuicStream, Error> {
        let addr = tokio::net::lookup_host(&self.addr)
            .await?
            .next()
            .ok_or_else(|| std::io::Error::other(format!("no address for {}", self.addr)))?;
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let local: SocketAddr = if addr.is_ipv6() {
                    "[::]:0".parse().unwrap()
                } else {
                    "0.0.0.0:0".parse().unwrap()
                };
                let mut endpoint = Endpoint::client(local)?;
                endpoint.set_default_client_config(self.config.clone());
                self.endpoint = Some(endpoint.clone());
                endpoint
            }
        };
        let conn = endpoint
            .connect(addr, &self.server_name)
            .map_err(std::io::Error::other)?
            .await
            .map_err(std::io::Error::other)?;
        let (send, recv) = conn.open_bi().await.map_err(std::io::Error::other)?;
        Ok(QuicStream {
            io: tokio::io::join(recv, send),
            conn,
        })
    }
}

impl QuicDialer {
    fn new(
        addr: &str,
        server_name: String,
        tls: rustls::ClientConfig,
    ) -> Result<Self, ConfigError> {
        let mut tls = tls;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto =
            QuicClientConfig::try_from(tls).map_err(|e| ConfigError::Quic(e.to_string()))?;
        let mut config = ClientConfig::new(Arc::new(crypto));
        let mut transport = TransportConfig::default();
        transport
            .max_idle_timeout(Some(IDLE_TIMEOUT.try_into().expect("fits")))
            .keep_alive_interval(Some(KEEPALIVE));
        config.transport_config(Arc::new(transport));
        Ok(Self {
            addr: strip(addr).into(),
            server_name,
            config,
            endpoint: None,
        })
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn rebind(&self) -> std::io::Result<()> {
        let Some(endpoint) = &self.endpoint else {
            return Ok(());
        };
        let local: SocketAddr = if endpoint.local_addr()?.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        endpoint.rebind(std::net::UdpSocket::bind(local)?)
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.endpoint.as_ref()?.local_addr().ok()
    }
}

fn strip(addr: &str) -> &str {
    addr.strip_prefix("quic://")
        .unwrap_or(addr)
        .trim_end_matches('/')
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

const PINNED_NAME: &str = "geri-hub";

pub fn pinned(
    addr: &str,
    hub: [u8; 32],
    token: impl Into<String>,
) -> Result<QuicTransport, ConfigError> {
    let verifier = Pinned::new(&hub)?;
    let tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    Ok(StreamTransport::over(
        QuicDialer::new(addr, PINNED_NAME.into(), tls)?,
        token,
    ))
}

pub fn webpki(addr: &str, token: impl Into<String>) -> Result<QuicTransport, ConfigError> {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    let tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let host = host(strip(addr));
    Ok(StreamTransport::over(
        QuicDialer::new(addr, host, tls)?,
        token,
    ))
}

fn host(addr: &str) -> String {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    host.trim_start_matches('[').trim_end_matches(']').into()
}

fn spki(public: &[u8; 32]) -> Result<Vec<u8>, ConfigError> {
    let key = ed25519_dalek::VerifyingKey::from_bytes(public)
        .map_err(|e| ConfigError::Key(e.to_string()))?;
    let der = key
        .to_public_key_der()
        .map_err(|e| ConfigError::Key(e.to_string()))?;
    Ok(der.as_bytes().to_vec())
}

#[derive(Debug)]
struct Pinned {
    spki: Vec<u8>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl Pinned {
    fn new(hub: &[u8; 32]) -> Result<Self, ConfigError> {
        Ok(Self {
            spki: spki(hub)?,
            algorithms: provider().signature_verification_algorithms,
        })
    }
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.spki.as_slice() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        let spki = SubjectPublicKeyInfoDer::from(cert.as_ref());
        verify_tls13_signature_with_raw_key(message, &spki, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

pub mod hub {
    use super::*;

    pub fn pinned(seed: &[u8; 32]) -> Result<ServerConfig, ConfigError> {
        let key = ed25519_dalek::SigningKey::from_bytes(seed);
        let pkcs8 = key
            .to_pkcs8_der()
            .map_err(|e| ConfigError::Key(e.to_string()))?;
        let signer = rustls::crypto::ring::sign::any_eddsa_type(&PrivatePkcs8KeyDer::from(
            pkcs8.as_bytes(),
        ))?;
        let public = spki(&key.verifying_key().to_bytes())?;
        let certified = CertifiedKey::new(vec![CertificateDer::from(public)], signer);
        let tls = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(AlwaysResolvesServerRawPublicKeys::new(Arc::new(
                certified,
            ))));
        server(tls)
    }

    pub fn certified(
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<ServerConfig, ConfigError> {
        let tls = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_no_client_auth()
            .with_single_cert(chain, key)?;
        server(tls)
    }

    fn server(mut tls: rustls::ServerConfig) -> Result<ServerConfig, ConfigError> {
        tls.alpn_protocols = vec![ALPN.to_vec()];
        let crypto =
            QuicServerConfig::try_from(tls).map_err(|e| ConfigError::Quic(e.to_string()))?;
        let mut config = ServerConfig::with_crypto(Arc::new(crypto));
        let mut transport = TransportConfig::default();
        transport
            .max_idle_timeout(Some(IDLE_TIMEOUT.try_into().expect("fits")))
            .max_concurrent_bidi_streams(1u32.into())
            .max_concurrent_uni_streams(0u32.into());
        config.transport_config(Arc::new(transport));
        Ok(config)
    }

    pub async fn accept(
        conn: &Connection,
    ) -> Result<Join<RecvStream, SendStream>, quinn::ConnectionError> {
        let (send, recv) = conn.accept_bi().await?;
        Ok(tokio::io::join(recv, send))
    }
}

#[cfg(test)]
mod tests;
