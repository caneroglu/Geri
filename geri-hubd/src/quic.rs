use crate::{api::Shared, tcp};
use geri_transport_quinn::hub;
use quinn::{Endpoint, ServerConfig};
use std::{net::SocketAddr, time::Duration};

const STREAM_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn config(
    seed: Option<&[u8; 32]>,
    cert: Option<&str>,
    key: Option<&str>,
) -> Result<ServerConfig, Box<dyn std::error::Error>> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    match (cert, key) {
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
            let key = PrivateKeyDer::from_pem_file(key)?;
            Ok(hub::certified(chain, key)?)
        }
        (None, None) => {
            let seed = seed.ok_or("GERI_QUIC_LISTEN needs GERI_SIGNING_SEED (the hub's key) or GERI_QUIC_CERT + GERI_QUIC_KEY")?;
            Ok(hub::pinned(seed)?)
        }
        _ => Err("GERI_QUIC_CERT and GERI_QUIC_KEY go together".into()),
    }
}

pub(crate) fn listen(
    app: Shared,
    config: ServerConfig,
    addr: SocketAddr,
) -> std::io::Result<Endpoint> {
    let endpoint = Endpoint::server(config, addr)?;
    let accepting = endpoint.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accepting.accept().await {
            let app = app.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let Ok(Ok(io)) = tokio::time::timeout(STREAM_TIMEOUT, hub::accept(&conn)).await
                else {
                    return;
                };
                tcp::plain(app, io).await;
                conn.close(0u32.into(), b"bye");
            });
        }
    });
    Ok(endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{TEST_HUB_SEED, TEST_SEED, test_app_with_key};
    use crate::msg;
    use geri_adapter_sdk::Transport;
    use geri_proto::v4::{
        VERSION,
        msg::{Frame, ToHub, ToSource},
    };
    use std::sync::Arc;

    async fn hello_and_ring(app: &Shared, transport: &mut geri_transport_quinn::QuicTransport) {
        transport.connect().await.unwrap();
        transport
            .send(Frame {
                id: Some(1),
                msg: ToHub::Hello {
                    v: VERSION,
                    auth: None,
                },
            })
            .await
            .unwrap();
        let ToSource::Welcome(welcome) = transport.recv().await.unwrap().msg else {
            panic!("no welcome")
        };
        assert_eq!(welcome.agent_id, "router");
        app.signals.ring("router");
        assert_eq!(
            transport.recv().await.unwrap().msg,
            ToSource::Ring {
                source_id: "router".into()
            }
        );
    }

    #[tokio::test]
    async fn a_device_pins_the_hub_key_and_signs_its_hello() {
        let dir = tempfile::tempdir().unwrap();
        let app = test_app_with_key(dir.path(), msg::Signals::new(32, 4));
        let config = config(Some(&TEST_HUB_SEED), None, None).unwrap();
        let endpoint = listen(app.clone(), config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = endpoint.local_addr().unwrap().to_string();
        let hub_key = geri_sign::public_key(&TEST_HUB_SEED);

        let signer = Arc::new(geri_sign::Signer::new("router", TEST_SEED).unwrap());
        let mut signed = geri_transport_quinn::pinned(&addr, hub_key, "")
            .unwrap()
            .with_signer(signer);
        hello_and_ring(&app, &mut signed).await;
        drop(signed);

        let mut token =
            geri_transport_quinn::pinned(&addr, hub_key, "router-token-123456").unwrap();
        hello_and_ring(&app, &mut token).await;

        let mut wrong = geri_transport_quinn::pinned(&addr, hub_key, "nope-token-1234567").unwrap();
        match wrong.connect().await {
            Err(geri_transport_tcp::Error::Rejected { status: 401, .. }) => {}
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn half_a_certificate_setting_is_refused() {
        assert!(config(None, Some("c.pem"), None).is_err());
        assert!(config(None, None, None).is_err());
    }
}
