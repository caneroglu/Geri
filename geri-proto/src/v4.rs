use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

pub mod msg;
pub mod validate;

pub const VERSION: u16 = 4;
pub const MAX_BATCH_RECORDS: usize = 128;
pub const MAX_INLINE_BYTES: usize = 64 * 1024;
/// SQLite ve JavaScript tabanlı hub'larda kayıpsız taşınabilen en büyük sıra.
pub const MAX_SEQUENCE: u64 = (1_u64 << 53) - 1;
pub const RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub max_batch_records: usize,
    pub max_inline_bytes: usize,
    pub max_body_bytes: usize,
    pub max_blob_bytes: usize,
    pub max_webhook_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Welcome {
    pub v: u16,
    pub agent_id: String,
    pub limits: Limits,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub source_id: String,
    pub label: String,
    pub kind: String,
    pub created_at_ms: u64,
    pub visibility: Visibility,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Visibility {
    #[default]
    Private,
    Public,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub source_id: String,
    pub entity_id: String,
    pub label: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub control: Option<crate::Control>,
}

impl Entity {
    pub fn new(source_id: impl Into<String>, entity_id: impl Into<String>) -> Self {
        let entity_id = entity_id.into();
        Self {
            source_id: source_id.into(),
            label: entity_id.clone(),
            entity_id,
            kind: "unknown".into(),
            group: None,
            control: None,
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self
    }
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = kind.into();
        self
    }
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }
    pub fn control(mut self, shape: crate::Shape, danger: crate::Danger) -> Self {
        self.control = Some(crate::Control::new(shape, danger));
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceDeclaration {
    pub source_id: String,
    pub label: String,
    pub kind: String,
    pub entities: Vec<Entity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    State,
    Event,
    Raw,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlobRef {
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub source_id: String,
    pub entity_id: String,
    pub record_id: String,
    pub sequence: u64,
    pub kind: RecordKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_ms: Option<u64>,
    pub content_type: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub data: Option<serde_json::Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    pub indexed: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobRef>,
}

fn present_value<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Batch {
    pub v: u16,
    pub records: Vec<Record>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    Accepted,
    Duplicate,
    Gap,
    Conflict,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordReceipt {
    pub source_id: String,
    pub record_id: String,
    pub status: ReceiptStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sequence: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub v: u16,
    pub records: Vec<RecordReceipt>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredRecord {
    #[serde(flatten)]
    pub record: Record,
    pub cursor: u64,
    pub accepted_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordPage {
    pub records: Vec<StoredRecord>,
    pub next_cursor: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub earliest_cursor: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counter: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandRequest {
    pub source_id: String,
    pub entity_id: String,
    pub action: String,
    #[serde(default)]
    pub args: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub command_id: String,
    pub source_id: String,
    pub entity_id: String,
    pub action: String,
    pub args: serde_json::Value,
    pub issued_by: String,
    pub issued_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandResult {
    pub command_id: String,
    pub ok: bool,
    pub result: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandAudit {
    #[serde(flatten)]
    pub command: Command,
    pub status: CommandStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatched_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Queued,
    Dispatched,
    Done,
    /// Kontrol kaldırıldığı/değiştiği için gönderilmeden sonlandırılmış komut.
    Cancelled,
    #[serde(other)]
    Unknown,
}

impl CommandStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Dispatched => "dispatched",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_text(text: &str) -> Self {
        match text {
            "queued" => Self::Queued,
            "dispatched" => Self::Dispatched,
            "done" => Self::Done,
            "cancelled" => Self::Cancelled,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignedSummary {
    pub algorithm: String,
    pub after_cursor: u64,
    pub through_cursor: u64,
    pub count: usize,
    pub sha256: String,
    pub public_key: String,
    pub signature: String,
}

pub fn valid_id(s: &str) -> bool {
    (1..=64).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
}

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;

    #[test]
    fn json_and_cbor_vectors_round_trip() {
        let batch = Batch {
            v: VERSION,
            records: alloc::vec![Record {
                source_id: "router".into(),
                entity_id: "cpu".into(),
                record_id: "router-1".into(),
                sequence: 1,
                kind: RecordKind::State,
                event_time_ms: Some(123),
                content_type: "application/json".into(),
                data: Some(serde_json::json!({"pct": 12.5})),
                blob: None,
                indexed: None,
            }],
        };
        let json = serde_json::to_vec(&batch).unwrap();
        let cbor = crate::cbor::to_vec(&batch).unwrap();
        assert_eq!(serde_json::from_slice::<Batch>(&json).unwrap(), batch);
        assert_eq!(minicbor_serde::from_slice::<Batch>(&cbor).unwrap(), batch);
        assert_eq!(batch.records[0].record_id, "router-1");
        let vector_json = include_str!("../vectors/v4-batch.json");
        let vector_hex = include_str!("../vectors/v4-batch.cbor.hex").trim();
        assert_eq!(serde_json::from_str::<Batch>(vector_json).unwrap(), batch);
        assert_eq!(
            cbor.iter()
                .map(|b| alloc::format!("{b:02x}"))
                .collect::<String>(),
            vector_hex
        );
    }

    #[test]
    fn malformed_json_and_cbor_corpus_does_not_panic() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        for len in 0..256usize {
            let mut bytes = alloc::vec![0u8;len];
            for b in &mut bytes {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                *b = seed as u8;
            }
            assert!(
                std::panic::catch_unwind(|| {
                    let _ = serde_json::from_slice::<Batch>(&bytes);
                    let _ = minicbor_serde::from_slice::<Batch>(&bytes);
                })
                .is_ok()
            );
        }
    }
}

#[cfg(test)]
mod nullable_record_tests {
    use super::*;
    #[test]
    fn absent_data_and_null_data_are_distinct_in_both_codecs() {
        for (data, indexed) in [
            (None, None),
            (Some(serde_json::Value::Null), Some(serde_json::Value::Null)),
        ] {
            let r = Record {
                source_id: "s".into(),
                entity_id: "e".into(),
                record_id: "r".into(),
                sequence: 1,
                kind: RecordKind::State,
                event_time_ms: None,
                content_type: "application/json".into(),
                data,
                indexed,
                blob: None,
            };
            assert_eq!(
                serde_json::from_slice::<Record>(&serde_json::to_vec(&r).unwrap()).unwrap(),
                r
            );
            #[cfg(feature = "cbor")]
            assert_eq!(
                crate::cbor::from_slice::<Record>(&crate::cbor::to_vec(&r).unwrap()).unwrap(),
                r
            );
        }
    }
}
