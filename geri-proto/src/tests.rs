use super::*;
use alloc::{vec, vec::Vec};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;

fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + core::fmt::Debug>(value: &T) {
    let json = serde_json::to_vec(value).unwrap();
    assert_eq!(&serde_json::from_slice::<T>(&json).unwrap(), value);
    let cbor = cbor(value);
    assert_eq!(&minicbor_serde::from_slice::<T>(&cbor).unwrap(), value);
}

fn cbor<T: Serialize>(value: &T) -> Vec<u8> {
    crate::cbor::to_vec(value).unwrap()
}

fn arbitrary_data() -> Value {
    Value::Map(BTreeMap::from([
        ("source/type".into(), Value::Text("new.source/v7".into())),
        ("raw".into(), Value::Bytes(vec![0, 128, 255])),
        ("counter".into(), Value::Uint(u64::MAX)),
        ("offset".into(), Value::Int(i64::MIN)),
        (
            "nested".into(),
            Value::List(vec![
                Value::Bool(true),
                Value::Null,
                Value::Float(1.25),
                Value::Map(BTreeMap::new()),
            ]),
        ),
    ]))
}

#[test]
fn arbitrary_data_survives_both_codecs() {
    let data = arbitrary_data();
    roundtrip(&data);
    roundtrip(&Values::from([("source".into(), data)]));
}

#[test]
fn bytes_lists_and_numeric_types_remain_distinct() {
    let values = [
        Value::Bytes(vec![1, 2]),
        Value::List(vec![Value::Uint(1), Value::Uint(2)]),
        Value::Bytes(vec![]),
        Value::List(vec![]),
        Value::Int(-1),
        Value::Uint(1),
        Value::Float(1.0),
        Value::Null,
        Value::Text("null".into()),
    ];
    for (i, value) in values.iter().enumerate() {
        roundtrip(value);
        let encoded = serde_json::to_vec(value).unwrap();
        for other in &values[i + 1..] {
            assert_ne!(encoded, serde_json::to_vec(other).unwrap());
            assert_ne!(cbor(value), cbor(other));
        }
    }
    for invalid in [
        r#"{"bytes":[256]}"#,
        r#"{"bytes":[-1]}"#,
        r#"{"bytes":[1.5]}"#,
    ] {
        assert!(serde_json::from_str::<Value>(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn non_finite_numbers_are_rejected_instead_of_becoming_null() {
    for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let values = Values::from([("value".into(), Value::Float(number))]);
        assert!(serde_json::to_vec(&values).is_err());
    }
    assert!(serde_json::from_str::<Value>(r#"{"float":null}"#).is_err());
}

#[test]
fn controls_carry_shape_and_danger() {
    let control: Control = serde_json::from_value(json!({
        "shape": {"type": "range", "min": 250, "max": 60000, "step": 250, "unit": "ms"},
        "danger": "risky", "future": {"ignored": true}
    }))
    .unwrap();
    assert_eq!(control.danger, Danger::Risky);
    assert!(
        matches!(control.shape, Shape::Range { min, step: Some(s), .. } if min == 250.0 && s == 250.0)
    );
    roundtrip(&control);
    for shape in [
        Shape::Trigger,
        Shape::Bool,
        Shape::choice(["2.4", "5"]),
        Shape::Text { max: 64 },
        Shape::item(Shape::Bool),
    ] {
        roundtrip(&Control::new(shape, Danger::Destructive));
    }
}

#[test]
fn unknown_shapes_and_dangers_degrade_safely() {
    let c: Control = serde_json::from_value(json!({
        "shape": {"type": "color", "space": "rgb"}, "danger": "nuclear"
    }))
    .unwrap();
    assert_eq!(
        (c.shape.clone(), c.danger),
        (Shape::Unknown, Danger::Destructive)
    );
    assert_eq!(c.shape.accept(&Value::Null), Err(Invalid::Unsupported));
    let default: Control = serde_json::from_value(json!({"shape": {"type": "bool"}})).unwrap();
    assert_eq!(default.danger, Danger::Safe);
    roundtrip(&Control::new(Shape::Unknown, Danger::Safe));
}

#[test]
fn shapes_validate_and_normalize_commands() {
    let range = Shape::Range {
        min: 250.0,
        max: 60000.0,
        step: Some(250.0),
        unit: None,
    };
    assert_eq!(range.accept(&Value::Float(500.0)), Ok(Value::Uint(500)));
    assert_eq!(range.accept(&Value::Uint(750)), Ok(Value::Uint(750)));
    assert_eq!(range.accept(&Value::Uint(600)), Err(Invalid::Range));
    assert_eq!(range.accept(&Value::Uint(60250)), Err(Invalid::Range));
    assert_eq!(range.accept(&Value::Text("500".into())), Err(Invalid::Type));
    let half = Shape::Range {
        min: -10.0,
        max: 10.0,
        step: Some(0.5),
        unit: None,
    };
    assert_eq!(half.accept(&Value::Float(-2.5)), Ok(Value::Float(-2.5)));
    assert_eq!(half.accept(&Value::from(-3i64)), Ok(Value::Int(-3)));
    assert_eq!(half.accept(&Value::Float(0.3)), Err(Invalid::Range));

    assert_eq!(
        Shape::Bool.accept(&Value::Bool(true)),
        Ok(Value::Bool(true))
    );
    assert_eq!(Shape::Bool.accept(&Value::Uint(1)), Err(Invalid::Type));
    assert_eq!(Shape::Trigger.accept(&Value::Null), Ok(Value::Null));
    assert_eq!(
        Shape::Trigger.accept(&Value::Bool(true)),
        Err(Invalid::Type)
    );
    let choice = Shape::choice(["2.4", "5"]);
    assert_eq!(
        choice.accept(&Value::Text("5".into())),
        Ok(Value::Text("5".into()))
    );
    assert_eq!(choice.accept(&Value::Text("6".into())), Err(Invalid::Range));
    let text = Shape::Text { max: 3 };
    assert!(text.accept(&Value::Text("üüü".into())).is_ok());
    assert_eq!(
        text.accept(&Value::Text("abcd".into())),
        Err(Invalid::Range)
    );

    let item = Shape::item(Shape::Bool);
    let cmd = |id: &str, v: Value| {
        Value::Map(BTreeMap::from([
            ("id".into(), Value::Text(id.into())),
            ("value".into(), v),
            ("extra".into(), Value::Null),
        ]))
    };
    let ok = item.accept(&cmd("t1", Value::Bool(true))).unwrap();
    assert_eq!(ok.get("extra"), None, "girdi normalleşir, fazlası atılır");
    assert_eq!(item.accept(&cmd("t1", Value::Uint(1))), Err(Invalid::Type));
    assert_eq!(item.action(), SET);
    assert_eq!(Shape::item(Shape::Trigger).action(), PRESS);
}

#[test]
fn reported_value_is_compared_by_meaning() {
    let range = Shape::range(0.0, 100.0);
    assert!(range.reached(&Value::Float(500.0), &Value::Uint(500)));
    assert!(!range.reached(&Value::Uint(1000), &Value::Uint(500)));
    assert!(Shape::Bool.reached(&Value::Bool(true), &Value::Bool(true)));
    let item = Shape::item(Shape::Bool);
    let list = Value::List(vec![
        Value::Map(BTreeMap::from([
            ("id".into(), Value::Text("a".into())),
            ("value".into(), Value::Bool(false)),
        ])),
        Value::Map(BTreeMap::from([
            ("id".into(), Value::Text("b".into())),
            ("value".into(), Value::Bool(true)),
        ])),
    ]);
    let want = |id: &str, v| {
        Value::Map(BTreeMap::from([
            ("id".into(), Value::Text(id.into())),
            ("value".into(), Value::Bool(v)),
        ]))
    };
    assert!(item.reached(&list, &want("b", true)));
    assert!(!item.reached(&list, &want("a", true)));
    assert!(!item.reached(&list, &want("zz", false)));
}

#[test]
fn structurally_broken_shapes_are_invalid() {
    assert!(!Shape::range(5.0, 1.0).is_valid());
    assert!(!Shape::range(f64::NEG_INFINITY, 1.0).is_valid());
    assert!(
        !Shape::Range {
            min: 0.0,
            max: 1.0,
            step: Some(0.0),
            unit: None
        }
        .is_valid()
    );
    assert!(!Shape::choice(Vec::<String>::new()).is_valid());
    assert!(!Shape::Text { max: 0 }.is_valid());
    assert!(!Shape::item(Shape::item(Shape::Bool)).is_valid());
    assert!(Shape::item(Shape::Trigger).is_valid());
}

#[test]
fn custom_payload_content_is_preserved() {
    let values: Values = serde_json::from_value(json!({"anything":{"map":{
        "t":{"text":"unseen-type"}, "v":{"list":[{"uint":1}, {"bytes":[9]}]}
    }}}))
    .unwrap();
    assert_eq!(
        values["anything"],
        Value::Map(BTreeMap::from([
            ("t".into(), Value::Text("unseen-type".into())),
            (
                "v".into(),
                Value::List(vec![Value::Uint(1), Value::Bytes(vec![9])])
            ),
        ]))
    );
    roundtrip(&values);
}

#[test]
fn accessors_read_across_numeric_types() {
    let data = arbitrary_data();
    assert_eq!(data.get("counter").and_then(Value::as_u64), Some(u64::MAX));
    assert_eq!(data.get("offset").and_then(Value::as_u64), None);
    assert_eq!(
        data.get("source/type").and_then(Value::as_str),
        Some("new.source/v7")
    );
    assert_eq!(Value::Int(-2).as_f64(), Some(-2.0));
    assert_eq!(Value::Text("1".into()).as_f64(), None);
    assert_eq!(Value::Null.get("x"), None);
}

#[test]
fn non_negative_integers_are_always_uint() {
    assert_eq!(Value::from(5_i64), Value::Uint(5));
    assert_eq!(Value::from(-5_i64), Value::Int(-5));
    let wrong = Value::Int(5);
    assert_eq!(serde_json::to_string(&wrong).unwrap(), r#"{"uint":5}"#);
    assert_eq!(
        minicbor_serde::from_slice::<Value>(&cbor(&wrong)).unwrap(),
        Value::Uint(5)
    );
    assert_eq!(
        serde_json::from_str::<Value>(r#"{"int":5}"#).unwrap(),
        Value::Uint(5)
    );
    for edge in [i64::MIN, -1] {
        roundtrip(&Value::Int(edge));
    }
}

#[test]
fn cbor_uses_native_types_without_tags() {
    assert_eq!(cbor(&Value::Null), [0xf6]);
    assert_eq!(cbor(&Value::Uint(82)), [0x18, 82]);
    assert_eq!(cbor(&Value::Int(-1)), [0x20]);
    assert_eq!(cbor(&Value::Bytes(vec![0, 255])), [0x42, 0, 255]);
    assert_eq!(cbor(&Value::Text("a".into())), [0x61, b'a']);
    let raw = Value::Bytes(vec![255; 8192]);
    assert!(cbor(&raw).len() < 8200);
    assert!(serde_json::to_vec(&raw).unwrap().len() > 4 * 8192);
    let state = Values::from([("source".into(), arbitrary_data())]);
    let (c, j) = (
        cbor(&state).len(),
        serde_json::to_vec(&state).unwrap().len(),
    );
    assert!(c * 2 < j, "cbor {c} json {j}");
}

#[test]
fn foreign_cbor_is_read_by_its_native_types() {
    let mut buf = Vec::new();
    let mut e = minicbor::Encoder::new(&mut buf);
    e.map(5).unwrap();
    e.str("pct").unwrap().u8(82).unwrap();
    e.str("t").unwrap().str("cpu").unwrap();
    e.str("raw").unwrap().bytes(&[9]).unwrap();
    e.str("neg").unwrap().i32(-3).unwrap();
    e.str("f").unwrap().f32(1.5).unwrap();
    let v: Value = minicbor_serde::from_slice(&buf).unwrap();
    assert_eq!(v.get("pct"), Some(&Value::Uint(82)));
    assert_eq!(v.get("raw"), Some(&Value::Bytes(vec![9])));
    assert_eq!(v.get("neg"), Some(&Value::Int(-3)));
    assert_eq!(v.get("f"), Some(&Value::Float(1.5)));
    assert_eq!(v.get("t").and_then(Value::as_str), Some("cpu"));
}

#[test]
fn cbor_rejects_what_the_model_cannot_hold() {
    let decode = |f: &dyn Fn(&mut minicbor::Encoder<&mut Vec<u8>>)| {
        let mut buf = Vec::new();
        f(&mut minicbor::Encoder::new(&mut buf));
        minicbor_serde::from_slice::<Value>(&buf)
    };
    assert!(
        decode(&|e| {
            e.map(1).unwrap().u8(1).unwrap().u8(2).unwrap();
        })
        .is_err()
    );
    assert!(
        decode(&|e| {
            e.f64(f64::NAN).unwrap();
        })
        .is_err()
    );
    assert!(
        decode(&|e| {
            e.int(minicbor::data::Int::try_from(-(1_i128 << 64)).unwrap())
                .unwrap();
        })
        .is_err()
    );
    assert!(minicbor_serde::to_vec(Value::Float(f64::INFINITY)).is_err());
}

#[test]
fn nesting_is_bounded_so_a_tiny_message_cannot_overflow_the_stack() {
    let nested = |depth: usize| {
        let mut buf = vec![0x81; depth - 1];
        buf.push(0x00);
        buf
    };
    assert!(minicbor_serde::from_slice::<Value>(&nested(MAX_DEPTH)).is_ok());
    assert!(minicbor_serde::from_slice::<Value>(&nested(MAX_DEPTH + 1)).is_err());
    assert!(minicbor_serde::from_slice::<Value>(&nested(1 << 20)).is_err());
}

#[test]
fn both_codecs_decode_to_the_same_values() {
    let values = Values::from([("a".into(), arbitrary_data()), ("b".into(), Value::Int(-7))]);
    let from_json: Values = serde_json::from_slice(&serde_json::to_vec(&values).unwrap()).unwrap();
    let from_cbor: Values = minicbor_serde::from_slice(&cbor(&values)).unwrap();
    assert_eq!(from_json, from_cbor);
    assert_eq!(from_json, values);
}

#[test]
fn plain_json_crosses_the_boundary_without_tags() {
    let v: Plain =
        serde_json::from_str(r#"{"id":"t1","value":true,"n":-3,"x":2.5,"l":[1,null]}"#).unwrap();
    assert_eq!(v.0.get("n"), Some(&Value::Int(-3)));
    assert_eq!(v.0.get("value"), Some(&Value::Bool(true)));
    assert_eq!(
        serde_json::to_string(&v).unwrap(),
        r#"{"id":"t1","l":[1,null],"n":-3,"value":true,"x":2.5}"#
    );
    let deep = "[".repeat(MAX_DEPTH + 1) + &"]".repeat(MAX_DEPTH + 1);
    assert!(serde_json::from_str::<Plain>(&deep).is_err());
}
