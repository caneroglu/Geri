use super::*;
use alloc::vec;
use geri_proto::v4::msg::{Frame, ToHub};

fn ping(id: u32) -> Vec<u8> {
    encode(
        &Frame {
            id: Some(id),
            msg: ToHub::Ping,
        },
        Codec::Cbor,
    )
    .unwrap()
}

fn items(r: &mut Reader) -> Vec<Result<Item, Error>> {
    core::iter::from_fn(|| r.next_item()).collect()
}

#[test]
fn frames_round_trip_whatever_the_chunking() {
    let payloads = [ping(1), ping(2), vec![0u8; 600], vec![0xff; 300]];
    let mut wire = Vec::new();
    for p in &payloads {
        write_frame(p, &mut wire);
    }
    assert_eq!(wire.iter().filter(|b| **b == 0).count(), payloads.len());
    for chunk in [1, 2, 7, 255, wire.len()] {
        let mut r = Reader::new(1024);
        for part in wire.chunks(chunk) {
            r.push(part);
        }
        let got: Vec<_> = items(&mut r).into_iter().map(Result::unwrap).collect();
        let want: Vec<_> = payloads.iter().cloned().map(Item::Frame).collect();
        assert_eq!(got, want, "chunk {chunk}");
    }
}

#[test]
fn a_frame_decodes_to_the_same_message() {
    let mut wire = Vec::new();
    write_frame(&ping(7), &mut wire);
    let mut r = Reader::new(64);
    r.push(&wire);
    let Some(Ok(Item::Frame(payload))) = r.next_item() else {
        panic!()
    };
    let frame: Frame<ToHub> = decode(&payload).unwrap();
    assert_eq!((frame.id, frame.msg), (Some(7), ToHub::Ping));
}

#[test]
fn an_empty_payload_is_a_reset_and_idle_zeros_are_ignored() {
    let mut wire = vec![0, 0, 0];
    write_reset(&mut wire);
    write_frame(&ping(1), &mut wire);
    let mut r = Reader::new(64);
    r.push(&wire);
    assert_eq!(items(&mut r), [Ok(Item::Reset), Ok(Item::Frame(ping(1)))]);
}

#[test]
fn corruption_drops_one_frame_and_the_next_one_survives() {
    let mut wire = Vec::new();
    write_frame(&ping(1), &mut wire);
    let first = wire.len();
    write_frame(&ping(2), &mut wire);
    write_frame(&ping(3), &mut wire);

    let mut flipped = wire.clone();
    flipped[2] ^= 0x10;
    let mut r = Reader::new(64);
    r.push(&flipped);
    let got = items(&mut r);
    assert!(
        matches!(got[0], Err(Error::Crc | Error::Corrupt)),
        "{got:?}"
    );
    assert_eq!(
        got[1..],
        [Ok(Item::Frame(ping(2))), Ok(Item::Frame(ping(3)))]
    );

    let mut lost = wire.clone();
    lost.remove(first - 1);
    let mut r = Reader::new(64);
    r.push(&lost);
    let got = items(&mut r);
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(got[0].is_err());
    assert_eq!(got[1], Ok(Item::Frame(ping(3))));

    let mut short = wire.clone();
    short.remove(first + 3);
    let mut r = Reader::new(64);
    r.push(&short);
    let got = items(&mut r);
    assert_eq!(got[0], Ok(Item::Frame(ping(1))));
    assert!(got[1].is_err());
    assert_eq!(got[2], Ok(Item::Frame(ping(3))));
}

#[test]
fn oversized_frames_are_dropped_without_buffering_them() {
    let mut wire = Vec::new();
    write_frame(&[7u8; 100], &mut wire);
    write_frame(&ping(1), &mut wire);
    for chunk in [1, 10, wire.len()] {
        let mut r = Reader::new(32);
        for part in wire.chunks(chunk) {
            r.push(part);
            assert_eq!(r.dec.dest().len(), 32 + CRC_LEN, "chunk {chunk}");
        }
        assert_eq!(
            items(&mut r),
            [Err(Error::TooLarge { max: 32 }), Ok(Item::Frame(ping(1)))],
            "chunk {chunk}"
        );
    }
    let mut wire = Vec::new();
    write_frame(&[7u8; 32], &mut wire);
    let mut r = Reader::new(32);
    r.push(&wire);
    assert_eq!(items(&mut r), [Ok(Item::Frame(vec![7u8; 32]))]);
}

#[test]
fn random_noise_never_panics() {
    let mut x: u32 = 0x1234_5678;
    let mut noise = Vec::new();
    for _ in 0..20_000 {
        x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        noise.push((x >> 24) as u8);
    }
    let mut r = Reader::new(256);
    for part in noise.chunks(97) {
        r.push(part);
        while r.next_item().is_some() {}
    }
    let mut wire = vec![0];
    write_frame(&ping(9), &mut wire);
    r.push(&wire);
    assert_eq!(items(&mut r).last(), Some(&Ok(Item::Frame(ping(9)))));
}

#[test]
fn the_limit_applies_to_the_decoded_payload() {
    // Sık sıfırlı payload'da COBS ek yükü 1 bayt: kodlanmış uzunluğa bakan sınır 1025'i kaçırıyordu.
    for (len, ok) in [(1024, true), (1025, false)] {
        let mut wire = Vec::new();
        let payload: Vec<u8> = (0..len).map(|i| (i % 10) as u8).collect();
        write_frame(&payload, &mut wire);
        let mut r = Reader::new(1024);
        r.push(&wire);
        let got = items(&mut r);
        assert_eq!(got.len(), 1, "{len}");
        assert_eq!(got[0].is_ok(), ok, "{len}: {:?}", got[0].as_ref().err());
        if !ok {
            assert_eq!(got[0], Err(Error::TooLarge { max: 1024 }));
        }
    }
}

#[test]
fn errors_keep_their_place_in_the_stream() {
    let mut wire = Vec::new();
    write_frame(&ping(1), &mut wire);
    write_frame(&[7u8; 100], &mut wire);
    write_frame(&ping(2), &mut wire);
    for chunk in [1, 5, wire.len()] {
        let mut r = Reader::new(32);
        for part in wire.chunks(chunk) {
            r.push(part);
        }
        assert_eq!(
            items(&mut r),
            [
                Ok(Item::Frame(ping(1))),
                Err(Error::TooLarge { max: 32 }),
                Ok(Item::Frame(ping(2)))
            ],
            "chunk {chunk}"
        );
    }
}
