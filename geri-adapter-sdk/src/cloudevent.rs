use alloc::string::String;
use geri_proto::v4::Record;
use sha2::{Digest, Sha256};

pub fn stable_id(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

pub fn original_cloudevent(record: &Record) -> Option<&serde_json::Value> {
    record.data.as_ref()?.get("cloudevent")
}
