use alloc::string::String;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid identifier")]
    InvalidId,
    #[error("source `{source_id}`: {reason}")]
    InvalidSource {
        source_id: String,
        reason: geri_proto::v4::validate::Invalid,
    },
    #[error("source `{0}` is configured more than once")]
    DuplicateSource(String),
    #[error("outbox record `{record_id}` of `{source_id}`: {reason}; record retained")]
    InvalidOutboxRecord {
        source_id: String,
        record_id: String,
        reason: geri_proto::v4::validate::Invalid,
    },
    #[error("an unordered record needs a record_id")]
    MissingRecordId,
    #[error("record ids of the form `{{source}}-{{number}}` are reserved for automatic ids")]
    ReservedRecordId,
    #[error("unknown source `{0}`")]
    UnknownSource(String),
    #[error("payload too large or malformed")]
    InvalidPayload,
    #[error("not a CloudEvents 1.0 event")]
    InvalidCloudEvent,
    #[error("transport: {0}")]
    Transport(String),
    #[error("store: {0}")]
    Store(String),
    #[error("hub answered {status} {code}: {message}")]
    Hub {
        status: u16,
        code: String,
        message: String,
    },
    #[error("sequence gap on `{source_id}`: hub expects {expected}")]
    Gap { source_id: String, expected: u64 },
    #[error("record `{record_id}` of `{source_id}` conflicts with the hub's copy")]
    Conflict {
        source_id: String,
        record_id: String,
    },
    #[error("record `{record_id}` of `{source_id}` exceeds the hub's body limit")]
    RecordTooLarge {
        source_id: String,
        record_id: String,
    },
    #[error("protocol violation: {0}")]
    Protocol(&'static str),
    #[error("hub speaks protocol v{0}")]
    Version(u16),
    #[error("request timed out")]
    Timeout,
}
