use super::*;
use geri_adapter_sdk::Transport;
use geri_binding_stream::{Codec, Reader, decode, encode, write_frame};
use geri_proto::v4::{
    Limits, VERSION, Welcome,
    msg::{Frame, ToHub, ToSource},
};
use geri_transport_tcp::{SIGNED_METHOD, SIGNED_TARGET};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const HUB: [u8; 32] = [9; 32];
const DEVICE: [u8; 32] = [7; 32];

fn public(seed: &[u8; 32]) -> [u8; 32] {
    ed25519_dalek::SigningKey::from_bytes(seed)
        .verifying_key()
        .to_bytes()
}

fn welcome() -> ToSource {
    ToSource::Welcome(Welcome {
        v: VERSION,
        agent_id: "router".into(),
        limits: Limits {
            max_batch_records: 100,
            max_inline_bytes: 1024,
            max_body_bytes: 1024 * 1024,
            max_blob_bytes: 0,
            max_webhook_bytes: 0,
        },
    })
}

struct Peer {
    io: Join<RecvStream, SendStream>,
    reader: Reader,
    remote: SocketAddr,
}

impl Peer {
    async fn recv(&mut self) -> Option<Frame<ToHub>> {
        loop {
            if let Some(payload) = self.reader.next_frame().unwrap() {
                return Some(decode(&payload).unwrap());
            }
            let mut buf = [0u8; 1024];
            let n = self.io.read(&mut buf).await.ok()?;
            if n == 0 {
                return None;
            }
            self.reader.push(&buf[..n]);
        }
    }

    async fn send(&mut self, id: Option<u32>, msg: ToSource) {
        let mut out = Vec::new();
        write_frame(&encode(&Frame { id, msg }, Codec::Json).unwrap(), &mut out);
        self.io.write_all(&out).await.unwrap();
        self.io.flush().await.unwrap();
    }
}

async fn hub(
    config: ServerConfig,
) -> (
    String,
    tokio::sync::mpsc::Receiver<(Peer, Option<String>, Connection)>,
) {
    let endpoint = Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = format!("quic://{}", endpoint.local_addr().unwrap());
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let Ok(conn) = incoming.await else { continue };
            let io = hub::accept(&conn).await.unwrap();
            let mut peer = Peer {
                io,
                reader: Reader::new(1 << 20),
                remote: conn.remote_address(),
            };
            let hello = peer.recv().await.unwrap();
            let ToHub::Hello { auth, .. } = hello.msg else {
                panic!("{hello:?}")
            };
            peer.send(hello.id, welcome()).await;
            tx.send((peer, auth, conn)).await.unwrap();
        }
    });
    (addr, rx)
}

async fn hello(transport: &mut QuicTransport, peer: &mut Peer) {
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
    let hello = peer.recv().await.unwrap();
    peer.send(hello.id, welcome()).await;
    assert!(matches!(
        transport.recv().await.unwrap().msg,
        ToSource::Welcome(_)
    ));
}

#[tokio::test]
async fn a_pinned_hub_key_is_enough_and_the_token_goes_in_hello() {
    let (addr, mut accepted) = hub(hub::pinned(&HUB).unwrap()).await;
    let mut transport = pinned(&addr, public(&HUB), "router-token-123456").unwrap();
    transport.connect().await.unwrap();
    let (mut peer, auth, _conn) = accepted.recv().await.unwrap();
    assert_eq!(auth.as_deref(), Some("Bearer router-token-123456"));
    hello(&mut transport, &mut peer).await;
    peer.send(
        None,
        ToSource::Ring {
            source_id: "router".into(),
        },
    )
    .await;
    assert_eq!(
        transport.recv().await.unwrap().msg,
        ToSource::Ring {
            source_id: "router".into()
        }
    );
}

#[tokio::test]
async fn a_signed_hello_works_the_same_as_on_tcp() {
    let (addr, mut accepted) = hub(hub::pinned(&HUB).unwrap()).await;
    let signer = Arc::new(geri_sign::Signer::new("router", DEVICE).unwrap());
    let mut transport = pinned(&addr, public(&HUB), "").unwrap().with_signer(signer);
    transport.connect().await.unwrap();
    let (_peer, auth, _conn) = accepted.recv().await.unwrap();
    let auth = auth.unwrap();
    let signature = geri_sign::Authorization::parse(&auth).unwrap();
    assert!(signature.verify(&public(&DEVICE), SIGNED_METHOD, SIGNED_TARGET, b""));
}

#[tokio::test]
async fn the_wrong_hub_key_does_not_connect() {
    let (addr, _accepted) = hub(hub::pinned(&HUB).unwrap()).await;
    let mut transport = pinned(&addr, public(&[1; 32]), "t").unwrap();
    let err = transport.connect().await.unwrap_err();
    assert!(err.to_string().contains("certificate"), "{err}");
}

#[tokio::test]
async fn certificate_mode_does_not_accept_a_bare_key() {
    let (addr, _accepted) = hub(hub::pinned(&HUB).unwrap()).await;
    let mut transport = webpki(&addr, "t").unwrap();
    assert!(transport.connect().await.is_err());
}

#[tokio::test]
async fn the_connection_survives_a_new_local_address() {
    let (addr, mut accepted) = hub(hub::pinned(&HUB).unwrap()).await;
    let mut transport = pinned(&addr, public(&HUB), "router-token-123456").unwrap();
    transport.connect().await.unwrap();
    let (mut peer, _auth, conn) = accepted.recv().await.unwrap();
    hello(&mut transport, &mut peer).await;
    let before = transport.dialer().local_addr().unwrap();
    assert_eq!(peer.remote, conn.remote_address());

    transport.dialer().rebind().unwrap();
    let after = transport.dialer().local_addr().unwrap();
    assert_ne!(before.port(), after.port());
    transport
        .send(Frame {
            id: Some(2),
            msg: ToHub::Claim {
                source_id: "router".into(),
            },
        })
        .await
        .unwrap();
    let claim = peer.recv().await.unwrap();
    assert_eq!(claim.id, Some(2));
    peer.send(claim.id, ToSource::Idle).await;
    assert_eq!(transport.recv().await.unwrap().msg, ToSource::Idle);
    assert_eq!(conn.remote_address().port(), after.port());
}

#[test]
fn hosts_are_taken_from_the_address() {
    assert_eq!(host("hub.example.com:4433"), "hub.example.com");
    assert_eq!(host("[::1]:4433"), "::1");
    assert_eq!(strip("quic://h:1/"), "h:1");
}
