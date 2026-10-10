extern crate std;

use super::*;
use std::{format, string::ToString};

const SEED: [u8; 32] = [7; 32];
const BODY: &[u8] = br#"{"source_id":"router"}"#;

fn signed(n: u64) -> std::string::String {
    Authorization {
        agent: "router",
        n,
        signature: sign(&SEED, "router", n, "POST", "/v4/batch", BODY),
    }
    .to_string()
}

#[test]
fn a_signed_request_verifies_and_any_change_breaks_it() {
    let header = signed(42);
    let auth = Authorization::parse(&header).unwrap();
    assert_eq!((auth.agent, auth.n), ("router", 42));
    let key = public_key(&SEED);
    assert!(auth.verify(&key, "POST", "/v4/batch", BODY));

    assert!(!auth.verify(&key, "PUT", "/v4/batch", BODY));
    assert!(!auth.verify(&key, "POST", "/v4/batch?x=1", BODY));
    assert!(!auth.verify(&key, "POST", "/v4/batch", b"{}"));
    assert!(!auth.verify(&public_key(&[8; 32]), "POST", "/v4/batch", BODY));
    let other_n = Authorization { n: 43, ..auth };
    assert!(!other_n.verify(&key, "POST", "/v4/batch", BODY));
    let other_agent = Authorization {
        agent: "routerx",
        ..auth
    };
    assert!(!other_agent.verify(&key, "POST", "/v4/batch", BODY));
}

#[test]
fn the_wire_format_is_frozen() {
    let hex = |b: &[u8]| {
        b.iter()
            .map(|b| format!("{b:02x}"))
            .collect::<std::string::String>()
    };
    assert_eq!(
        hex(&digest("router", 42, "POST", "/v4/batch", BODY)),
        "1e23d41fc979efc286f1a480332dccf69d74652cdf2d3f430f1874d06d7314ca"
    );
    assert_eq!(
        hex(&public_key(&SEED)),
        "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c"
    );
    assert_eq!(
        signed(42),
        "Geri-Ed25519 router:42:9eb7ab459bbfb9feff3b4bdad9a4db75a63f1c508a09ffaef17f2e70cd7fb8d495128c8df3026c2c7afe818bada901b43f5e484405674077d7f5ad779e36930a"
    );
}

#[test]
fn malformed_headers_are_rejected() {
    let sig = "ab".repeat(64);
    assert!(Authorization::parse(&format!("Geri-Ed25519 router:1:{sig}")).is_some());
    for bad in [
        format!("Bearer router:1:{sig}"),
        format!("Geri-Ed25519 router:0:{sig}"),
        format!("Geri-Ed25519 router:01:{sig}"),
        format!("Geri-Ed25519 router:+1:{sig}"),
        format!("Geri-Ed25519 router:-1:{sig}"),
        format!("Geri-Ed25519 router:18446744073709551616:{sig}"),
        format!("Geri-Ed25519 :1:{sig}"),
        format!("Geri-Ed25519 rou/ter:1:{sig}"),
        format!("Geri-Ed25519 router:1:{}", &sig[2..]),
        format!("Geri-Ed25519 router:1:{sig}00"),
        format!("Geri-Ed25519 router:1:{sig}:"),
        format!("Geri-Ed25519  router:1:{sig}"),
        "Geri-Ed25519 router:1".into(),
    ] {
        assert!(Authorization::parse(&bad).is_none(), "{bad}");
    }
}

#[test]
fn keys_are_64_hex_characters() {
    assert_eq!(parse_key(&"07".repeat(32)), Some(SEED));
    assert_eq!(parse_key(&format!("{}\n", "07".repeat(32))), Some(SEED));
    assert_eq!(parse_key(&"0A".repeat(32)), Some([10; 32]));
    assert_eq!(parse_key(&"07".repeat(31)), None);
    assert_eq!(parse_key(&"zz".repeat(32)), None);
}

#[test]
fn the_window_takes_each_number_once_and_tolerates_reordering() {
    let mut w = Window::default();
    assert!(!w.accept(0), "0 geçersiz");
    assert!(w.accept(1));
    assert!(!w.accept(1), "tekrar");
    assert!(w.accept(5));
    assert!(w.accept(3));
    assert!(!w.accept(3));
    assert!(w.accept(2) && w.accept(4));
    assert!(!w.accept(4) && !w.accept(5));

    assert!(w.accept(5 + WINDOW - 1));
    assert!(!w.accept(5), "pencerede, görülmüş");
    assert!(w.accept(6), "pencerenin içinde, görülmemiş");
    assert!(!w.check(4), "pencerenin gerisi");

    assert!(w.accept(1_000));
    assert!(!w.accept(1_000 - WINDOW));
    assert!(w.accept(1_000 - WINDOW + 1));
    assert!(!w.check(1_000) && w.check(1_001) && w.check(999));
}

#[test]
fn the_window_survives_a_round_trip_through_storage() {
    let mut w = Window::default();
    for n in [10, 7, 9] {
        assert!(w.accept(n));
    }
    let mut back = Window {
        max: w.max,
        seen: w.seen,
    };
    assert!(!back.accept(7) && !back.accept(9) && !back.accept(10));
    assert!(back.accept(8));
}

#[cfg(feature = "std")]
mod signer {
    use super::*;

    #[test]
    fn the_signer_counts_up_and_learns_from_the_hub() {
        let s = Signer::from_hex("router", &"07".repeat(32)).unwrap();
        assert_eq!(s.public_key(), public_key(&SEED));
        let first = s.authorization("GET", "/v4/welcome", b"");
        let a = Authorization::parse(&first).unwrap();
        assert_eq!(a.n, 1);
        assert!(a.verify(&s.public_key(), "GET", "/v4/welcome", b""));
        assert_eq!(
            Authorization::parse(&s.authorization("GET", "/", b""))
                .unwrap()
                .n,
            2
        );

        s.learn(517);
        assert_eq!(
            Authorization::parse(&s.authorization("GET", "/", b""))
                .unwrap()
                .n,
            518
        );
        s.learn(3);
        assert_eq!(
            Authorization::parse(&s.authorization("GET", "/", b""))
                .unwrap()
                .n,
            519
        );
        s.learn(u64::MAX);
        assert_eq!(
            Authorization::parse(&s.authorization("GET", "/", b""))
                .unwrap()
                .n,
            u64::MAX
        );

        assert_eq!(
            Signer::from_hex("ro:uter", &"07".repeat(32)).unwrap_err(),
            Error::Agent
        );
        assert_eq!(Signer::from_hex("router", "07").unwrap_err(), Error::Key);
        assert!(!format!("{s:?}").contains("07"), "anahtar loglanmaz");
    }
}
