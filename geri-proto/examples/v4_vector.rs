use geri_proto::v4::{Batch, Record, RecordKind, VERSION};
fn main() {
    let batch = Batch {
        v: VERSION,
        records: vec![Record {
            source_id: "router".into(),
            entity_id: "cpu".into(),
            record_id: "router-1".into(),
            sequence: 1,
            kind: RecordKind::State,
            event_time_ms: Some(123),
            content_type: "application/json".into(),
            data: Some(serde_json::json!({"pct":12.5})),
            indexed: None,
            blob: None,
        }],
    };
    let json = serde_json::to_string_pretty(&batch).unwrap();
    let cbor = minicbor_serde::to_vec(&batch).unwrap();
    println!("{json}");
    println!(
        "CBOR: {}",
        cbor.iter().map(|b| format!("{b:02x}")).collect::<String>()
    );
}
