use super::*;
use alloc::vec;
use geri_proto::v4::{
    CommandResult,
    msg::{ToHub, ToSource},
};
use serde_json::json;

fn claim(id: u32) -> Frame<ToHub> {
    Frame {
        id: Some(id),
        msg: ToHub::Claim {
            source_id: "router".into(),
        },
    }
}

fn result() -> Frame<ToHub> {
    Frame {
        id: Some(9),
        msg: ToHub::Result {
            source_id: "router".into(),
            result: CommandResult {
                command_id: "c1".into(),
                ok: true,
                result: json!(null),
            },
        },
    }
}

#[test]
fn frames_round_trip_and_codec_is_detected() {
    for codec in [Codec::Json, Codec::Cbor] {
        let bytes = encode(&result(), codec).unwrap();
        assert_eq!(Codec::of(&bytes), Some(codec));
        assert_eq!(decode::<ToHub>(&bytes).unwrap(), result());
        let pace = Frame {
            id: None,
            msg: ToSource::Pace {
                source_id: "router".into(),
                ms: 1000,
            },
        };
        let bytes = encode(&pace, codec).unwrap();
        assert_eq!(decode::<ToSource>(&bytes).unwrap(), pace);
    }
}

#[test]
fn ping_is_the_exact_text_the_edge_auto_answers() {
    let ping = Frame {
        id: None,
        msg: ToHub::Ping,
    };
    assert_eq!(encode(&ping, Codec::Json).unwrap(), br#"{"t":"ping"}"#);
}

#[test]
fn cbor_keeps_json_null() {
    let bytes = encode(&result(), Codec::Cbor).unwrap();
    assert!(bytes.contains(&0xf6), "{bytes:02x?}");
}

#[test]
fn garbage_is_rejected_not_guessed() {
    assert_eq!(decode::<ToHub>(b""), Err(Error::UnknownCodec));
    assert_eq!(decode::<ToHub>(b"[1,2]"), Err(Error::UnknownCodec));
    assert_eq!(decode::<ToHub>(&[0x01, 0x02]), Err(Error::UnknownCodec));
    assert!(matches!(decode::<ToHub>(b"{\"t\":"), Err(Error::Decode(_))));
    assert_eq!(Codec::of(b"  \n{}"), Some(Codec::Json));
}

#[test]
fn stream_frames_survive_any_split_and_mixed_codecs() {
    let frames = [
        encode(&claim(1), Codec::Json).unwrap(),
        encode(&result(), Codec::Cbor).unwrap(),
        encode(&claim(2), Codec::Cbor).unwrap(),
    ];
    let mut stream = Vec::new();
    for f in &frames {
        write_frame(f, &mut stream);
    }
    for chunk in [stream.len(), 1, 3] {
        let mut reader = Reader::new(1 << 20);
        let mut got = Vec::new();
        for part in stream.chunks(chunk) {
            reader.push(part);
            while let Some(frame) = reader.next_frame().unwrap() {
                got.push(decode::<ToHub>(&frame).unwrap());
            }
        }
        assert_eq!(got, vec![claim(1), result(), claim(2)], "chunk {chunk}");
    }
}

#[test]
fn length_prefix_is_leb128() {
    let mut out = Vec::new();
    write_frame(&[0u8; 300], &mut out);
    assert_eq!(&out[..2], &[0xac, 0x02]);
    assert_eq!(out.len(), 302);
    let mut out = Vec::new();
    write_frame(&[7u8; 5], &mut out);
    assert_eq!(out, [5, 7, 7, 7, 7, 7]);
}

#[test]
fn oversized_frames_are_refused_before_buffering() {
    let mut reader = Reader::new(100);
    let mut prefix = Vec::new();
    write_frame(&[0u8; 101], &mut prefix);
    reader.push(&prefix[..2]);
    assert_eq!(
        reader.next_frame(),
        Err(Error::TooLarge { len: 101, max: 100 })
    );
}

#[test]
fn malformed_prefixes_are_refused() {
    for bad in [
        &[0x00][..],
        &[0xff, 0xff, 0xff, 0xff, 0x7f],
        &[0x80, 0x80, 0x80, 0x80, 0x80],
        &[0x80, 0x00],
    ] {
        let mut reader = Reader::new(usize::MAX);
        reader.push(bad);
        assert_eq!(reader.next_frame(), Err(Error::BadLength), "{bad:02x?}");
    }
    let mut reader = Reader::new(usize::MAX);
    reader.push(&[0x80, 0x80]);
    assert_eq!(reader.next_frame(), Ok(None));
}
