use super::*;
use std::vec::Vec;

const DEVICE: [u8; 32] = [7; 32];
const HUB: [u8; 32] = [9; 32];
const OTHER: [u8; 32] = [11; 32];

fn public(seed: &[u8; 32]) -> [u8; 32] {
    SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

fn handshake(
    device: &[u8; 32],
    hub_seed: &[u8; 32],
    hub_public: &[u8; 32],
) -> Result<(Cipher, Cipher, [u8; 32]), Error> {
    let mut i = OsHandshake::initiator(device, hub_public)?;
    let mut r = OsHandshake::responder(hub_seed)?;
    let mut buf = [0u8; HANDSHAKE_BUFFER];
    let n = i.write(&mut buf)?;
    r.read(&buf[..n])?;
    let remote = r.remote_static().unwrap();
    let n = r.write(&mut buf)?;
    i.read(&buf[..n])?;
    Ok((i.finish()?, r.finish()?, remote))
}

fn send(from: &mut Cipher, to: &mut Cipher, plain: &[u8]) -> Result<Vec<u8>, Error> {
    let mut wire = std::vec![0u8; plain.len() + TAG];
    let n = from.encrypt(plain, &mut wire)?;
    let mut out = std::vec![0u8; n];
    let m = to.decrypt(&wire[..n], &mut out)?;
    out.truncate(m);
    Ok(out)
}

#[test]
fn device_and_hub_agree_and_the_hub_learns_who_the_device_is() {
    let (mut device, mut hub, remote) = handshake(&DEVICE, &HUB, &public(&HUB)).unwrap();
    assert!(matches(&remote, &public(&DEVICE)));
    assert!(!matches(&remote, &public(&OTHER)));
    assert_eq!(send(&mut device, &mut hub, b"hello").unwrap(), b"hello");
    assert_eq!(send(&mut hub, &mut device, b"welcome").unwrap(), b"welcome");
    for i in 0..5u8 {
        assert_eq!(send(&mut device, &mut hub, &[i; 3]).unwrap(), [i; 3]);
    }
}

#[test]
fn handshake_messages_are_96_and_48_bytes() {
    let mut i = OsHandshake::initiator(&DEVICE, &public(&HUB)).unwrap();
    let mut r = OsHandshake::responder(&HUB).unwrap();
    let mut buf = [0u8; HANDSHAKE_BUFFER];
    let n = i.write(&mut buf).unwrap();
    assert_eq!(n, 96);
    r.read(&buf[..n]).unwrap();
    assert_eq!(r.write(&mut buf).unwrap(), 48);
}

#[test]
fn a_device_pinning_the_wrong_hub_key_cannot_connect() {
    assert!(handshake(&DEVICE, &HUB, &public(&OTHER)).is_err());
}

#[test]
fn every_flipped_handshake_byte_is_rejected() {
    let mut buf = [0u8; HANDSHAKE_BUFFER];
    let mut i = OsHandshake::initiator(&DEVICE, &public(&HUB)).unwrap();
    let n1 = i.write(&mut buf).unwrap();
    let first = buf[..n1].to_vec();
    for at in 0..first.len() {
        let mut bad = first.clone();
        bad[at] ^= 0x01;
        let mut r = OsHandshake::responder(&HUB).unwrap();
        assert!(r.read(&bad).is_err(), "byte {at} of the first message");
    }
    let mut r = OsHandshake::responder(&HUB).unwrap();
    r.read(&first).unwrap();
    let n2 = r.write(&mut buf).unwrap();
    let second = buf[..n2].to_vec();
    for at in 0..second.len() {
        let mut bad = second.clone();
        bad[at] ^= 0x01;
        let mut i = OsHandshake::initiator(&DEVICE, &public(&HUB)).unwrap();
        i.write(&mut [0u8; HANDSHAKE_BUFFER]).unwrap();
        assert!(i.read(&bad).is_err(), "byte {at} of the second message");
    }
}

#[test]
fn handshake_messages_may_not_carry_data() {
    let mut hs = NqHandshakeCore::<X25519, ChaChaPoly, Blake2s, DefaultRng>::new(
        noise_ik(),
        PROLOGUE,
        true,
        Some(x25519_keypair(&DEVICE)),
        None,
        x25519_public(&public(&HUB)),
        None,
    )
    .unwrap();
    let mut buf = [0u8; 256];
    let n = hs.write_message(b"batch", &mut buf).unwrap();
    let mut r = OsHandshake::responder(&HUB).unwrap();
    assert!(r.read(&buf[..n]).is_err());
}

#[test]
fn tampered_or_replayed_traffic_is_rejected() {
    let (mut device, mut hub, _) = handshake(&DEVICE, &HUB, &public(&HUB)).unwrap();
    let mut wire = [0u8; 64];
    let n = device.encrypt(b"batch", &mut wire).unwrap();
    for at in 0..n {
        let (mut device, mut hub, _) = handshake(&DEVICE, &HUB, &public(&HUB)).unwrap();
        let mut wire = [0u8; 64];
        let n = device.encrypt(b"batch", &mut wire).unwrap();
        wire[at] ^= 0x80;
        assert!(
            hub.decrypt(&wire[..n], &mut [0u8; 64]).is_err(),
            "byte {at}"
        );
    }
    let mut out = [0u8; 64];
    assert_eq!(hub.decrypt(&wire[..n], &mut out).unwrap(), 5);
    assert!(hub.decrypt(&wire[..n], &mut out).is_err());
}

#[test]
fn keys_from_the_same_file_as_signatures() {
    let x = x25519_public(&public(&DEVICE)).unwrap();
    assert_eq!(x, x25519_keypair(&DEVICE).public);
    let mut identity = [0u8; 32];
    identity[0] = 1;
    assert_eq!(x25519_public(&identity), None);
    assert!(matches!(
        OsHandshake::initiator(&DEVICE, &identity),
        Err(Error::BadKey)
    ));
}

mod stream {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[tokio::test]
    async fn a_byte_stream_survives_tiny_reads_and_big_writes() {
        let (a, b) = duplex(61);
        let hub = tokio::spawn(async move { crate::accept(b, &HUB).await.unwrap() });
        let mut device = crate::connect(a, &DEVICE, &public(&HUB)).await.unwrap();
        let (mut hub, remote) = hub.await.unwrap();
        assert!(matches(&remote, &public(&DEVICE)));

        let big: Vec<u8> = (0..200_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let sent = big.clone();
        let writer = tokio::spawn(async move {
            device.write_all(&sent).await.unwrap();
            device.flush().await.unwrap();
            device
        });
        let mut got = std::vec![0u8; big.len()];
        hub.read_exact(&mut got).await.unwrap();
        assert_eq!(got, big);
        let mut device = writer.await.unwrap();

        hub.write_all(b"pong").await.unwrap();
        hub.flush().await.unwrap();
        let mut reply = [0u8; 4];
        device.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pong");

        hub.shutdown().await.unwrap();
        drop(hub);
        assert_eq!(device.read(&mut reply).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn a_stream_cut_mid_message_is_an_error_not_eof() {
        let (a, b) = duplex(1024);
        let hub = tokio::spawn(async move { crate::accept(b, &HUB).await.unwrap() });
        let device = crate::connect(a, &DEVICE, &public(&HUB)).await.unwrap();
        let (mut hub, _) = hub.await.unwrap();
        let mut raw = device.io;
        raw.write_all(&[0, 40, 1, 2, 3]).await.unwrap();
        drop(raw);
        let err = hub.read(&mut [0u8; 8]).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn a_stranger_pinning_another_hub_fails_the_handshake() {
        let (a, b) = duplex(1024);
        let hub = tokio::spawn(async move { crate::accept(b, &HUB).await });
        let other = public(&OTHER);
        let device = crate::connect(a, &DEVICE, &other);
        let (device, hub) = tokio::join!(device, hub);
        assert!(hub.unwrap().is_err());
        assert!(device.is_err());
    }
}
