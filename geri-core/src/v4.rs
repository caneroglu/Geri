use alloc::{collections::BTreeSet, string::String};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use geri_proto::v4::{
    self, ReceiptStatus, Record, RecordReceipt, SignedSummary, SourceDeclaration, StoredRecord,
    Visibility,
};
use sha2::{Digest, Sha256};

pub use geri_proto::v4::validate::MAX_LABEL;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reject {
    Version,
    BatchSize,
    InvalidId,
    Payload,
    Sequence,
    ContentType,
    Forbidden,
}

#[derive(Clone, Debug, Default)]
pub struct AgentPolicy {
    pub agent_id: String,
    pub writable_sources: BTreeSet<String>,
    pub role: Role,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Role {
    #[default]
    Agent,
    Developer,
    Operator,
}

impl AgentPolicy {
    pub fn can_write(&self, source_id: &str) -> bool {
        self.writable_sources.contains(source_id)
    }

    pub fn can_read(&self, visibility: Visibility) -> bool {
        match self.role {
            Role::Operator | Role::Developer => true,
            Role::Agent => visibility == Visibility::Public,
        }
    }

    pub fn can_command(&self) -> bool {
        self.role == Role::Operator
    }
}

pub fn validate_batch(batch: &v4::Batch) -> Result<(), Reject> {
    if batch.v != v4::VERSION {
        return Err(Reject::Version);
    }
    if batch.records.is_empty() || batch.records.len() > v4::MAX_BATCH_RECORDS {
        return Err(Reject::BatchSize);
    }
    for record in &batch.records {
        validate_record(record)?;
    }
    Ok(())
}

pub fn validate_record(record: &Record) -> Result<(), Reject> {
    v4::validate::record(record).map_err(|error| match error {
        v4::validate::Invalid::Id => Reject::InvalidId,
        v4::validate::Invalid::ContentType => Reject::ContentType,
        v4::validate::Invalid::Sequence => Reject::Sequence,
        _ => Reject::Payload,
    })
}

pub fn validate_source(declaration: &SourceDeclaration) -> Result<(), Reject> {
    v4::validate::source(declaration).map_err(|_| Reject::InvalidId)
}

pub fn validate_command(
    control: Option<&geri_proto::Control>,
    action: &str,
    args: &serde_json::Value,
) -> Option<serde_json::Value> {
    let control = control?;
    if control.shape.action() != action {
        return None;
    }
    let value = serde_json::from_value::<geri_proto::Plain>(args.clone())
        .ok()?
        .0;
    let normalized = control.shape.accept(&value).ok()?;
    serde_json::to_value(geri_proto::Plain(normalized)).ok()
}

pub fn decide(record: &Record, existing: Option<(u64, u64)>, last_sequence: u64) -> RecordReceipt {
    let (status, cursor, expected_sequence) = match existing {
        Some((sequence, cursor)) if sequence == record.sequence => {
            (ReceiptStatus::Duplicate, Some(cursor), None)
        }
        Some(_) => (ReceiptStatus::Conflict, None, None),
        None if record.sequence == 0 => (ReceiptStatus::Accepted, None, None),
        None if last_sequence == 0 || record.sequence == last_sequence.saturating_add(1) => {
            (ReceiptStatus::Accepted, None, None)
        }
        None if record.sequence > last_sequence.saturating_add(1) => (
            ReceiptStatus::Gap,
            None,
            Some(last_sequence.saturating_add(1)),
        ),
        None => (ReceiptStatus::Conflict, None, None),
    };
    RecordReceipt {
        source_id: record.source_id.clone(),
        record_id: record.record_id.clone(),
        status,
        cursor,
        expected_sequence,
    }
}

pub fn becomes_current(record: &Record, current_event_time_ms: Option<u64>) -> bool {
    if record.sequence != 0 {
        return true;
    }
    match (record.event_time_ms, current_event_time_ms) {
        (Some(new), Some(current)) => new >= current,
        _ => true,
    }
}

pub fn sign_summary(after_cursor: u64, records: &[StoredRecord], seed: [u8; 32]) -> SignedSummary {
    let hash = page_hash(records);
    let through = records.last().map_or(after_cursor, |r| r.cursor);
    let message = summary_message(after_cursor, through, records.len(), &hash);
    let key = SigningKey::from_bytes(&seed);
    let signature = key.sign(&message);
    SignedSummary {
        algorithm: "ed25519-sha256-v1".into(),
        after_cursor,
        through_cursor: through,
        count: records.len(),
        sha256: hex::encode(hash),
        public_key: hex::encode(key.verifying_key().to_bytes()),
        signature: hex::encode(signature.to_bytes()),
    }
}

fn page_hash(records: &[StoredRecord]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"geri-v4-record-page\0");
    for r in records {
        let raw = serde_json::to_vec(&r.record).expect("serializable record");
        digest.update(r.cursor.to_be_bytes());
        digest.update(r.accepted_at_ms.to_be_bytes());
        digest.update((raw.len() as u64).to_be_bytes());
        digest.update(&raw);
    }
    digest.finalize().into()
}

fn summary_message(after: u64, through: u64, count: usize, hash: &[u8; 32]) -> alloc::vec::Vec<u8> {
    let mut message = alloc::vec::Vec::new();
    message.extend_from_slice(b"GERI V4 LOG SUMMARY\0");
    message.extend_from_slice(&after.to_be_bytes());
    message.extend_from_slice(&through.to_be_bytes());
    message.extend_from_slice(&(count as u64).to_be_bytes());
    message.extend_from_slice(hash);
    message
}

fn parse_hex<const N: usize>(hex: &str) -> Option<[u8; N]> {
    let mut out = [0u8; N];
    hex::decode_to_slice(hex, &mut out).ok()?;
    Some(out)
}

pub fn verify_summary(summary: &SignedSummary, records: &[StoredRecord]) -> bool {
    if summary.algorithm != "ed25519-sha256-v1"
        || summary.count != records.len()
        || summary.through_cursor != records.last().map_or(summary.after_cursor, |r| r.cursor)
        || records
            .first()
            .is_some_and(|r| r.cursor <= summary.after_cursor)
    {
        return false;
    }
    let hash = page_hash(records);
    if parse_hex::<32>(&summary.sha256) != Some(hash) {
        return false;
    }
    let (Some(public), Some(sig)) = (
        parse_hex::<32>(&summary.public_key),
        parse_hex::<64>(&summary.signature),
    ) else {
        return false;
    };
    let Ok(key) = VerifyingKey::from_bytes(&public) else {
        return false;
    };
    key.verify(
        &summary_message(
            summary.after_cursor,
            summary.through_cursor,
            summary.count,
            &hash,
        ),
        &Signature::from_bytes(&sig),
    )
    .is_ok()
}

pub fn parse_signing_seed(hex: &str) -> Option<[u8; 32]> {
    parse_hex(hex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geri_proto::v4::{Batch, RecordKind};

    fn record(sequence: u64) -> Record {
        Record {
            source_id: "router".into(),
            entity_id: "cpu".into(),
            record_id: alloc::format!("r-{sequence}"),
            sequence,
            kind: RecordKind::State,
            event_time_ms: None,
            content_type: "application/json".into(),
            data: Some(serde_json::json!({"pct": 2})),
            indexed: None,
            blob: None,
        }
    }

    #[test]
    fn sequence_and_duplicate_contract() {
        assert_eq!(decide(&record(1), None, 0).status, ReceiptStatus::Accepted);
        assert_eq!(decide(&record(3), None, 1).expected_sequence, Some(2));
        assert_eq!(decide(&record(1), Some((1, 42)), 5).cursor, Some(42));
        assert_eq!(
            decide(&record(1), Some((2, 42)), 5).status,
            ReceiptStatus::Conflict
        );
        assert_eq!(decide(&record(1), None, 5).status, ReceiptStatus::Conflict);
    }

    #[test]
    fn sequence_domain_is_exact_for_all_stores() {
        assert_eq!(validate_record(&record(0)), Ok(()));
        assert_eq!(
            validate_record(&record(geri_proto::v4::MAX_SEQUENCE)),
            Ok(())
        );
        for sequence in [geri_proto::v4::MAX_SEQUENCE + 1, i64::MAX as u64, u64::MAX] {
            assert_eq!(validate_record(&record(sequence)), Err(Reject::Sequence));
        }
    }

    #[test]
    fn first_record_of_an_unknown_source_starts_the_sequence() {
        assert_eq!(
            decide(&record(5000), None, 0).status,
            ReceiptStatus::Accepted
        );
        assert_eq!(decide(&record(5000), None, 1).status, ReceiptStatus::Gap);
    }

    #[test]
    fn unordered_records_are_deduplicated_by_id_only() {
        let unordered = |id: &str| Record {
            record_id: id.into(),
            ..record(0)
        };
        assert_eq!(validate_record(&unordered("u-1")), Ok(()));
        for last in [0, 1, 5000] {
            let r = decide(&unordered("u-1"), None, last);
            assert_eq!(
                (r.status, r.expected_sequence),
                (ReceiptStatus::Accepted, None)
            );
        }
        assert_eq!(
            decide(&unordered("u-1"), Some((0, 9)), 5).status,
            ReceiptStatus::Duplicate
        );
        assert_eq!(decide(&unordered("u-1"), Some((0, 9)), 5).cursor, Some(9));
        assert_eq!(
            decide(&unordered("u-1"), Some((4, 9)), 5).status,
            ReceiptStatus::Conflict
        );
    }

    #[test]
    fn a_late_unordered_state_does_not_replace_a_newer_one() {
        let at = |sequence, time| Record {
            event_time_ms: time,
            ..record(sequence)
        };
        assert!(becomes_current(&at(0, Some(200)), Some(100)));
        assert!(becomes_current(&at(0, Some(100)), Some(100)));
        assert!(!becomes_current(&at(0, Some(50)), Some(100)));
        assert!(becomes_current(&at(0, None), Some(100)));
        assert!(becomes_current(&at(0, Some(50)), None));
        assert!(becomes_current(&at(7, Some(50)), Some(100)));
    }

    #[test]
    fn private_default_and_permissions() {
        let mut policy = AgentPolicy::default();
        assert!(!policy.can_read(Visibility::default()));
        policy.writable_sources.insert("router".into());
        assert!(policy.can_write("router"));
        assert!(!policy.can_write("other"));
        assert!(!policy.can_command());
        policy.role = Role::Operator;
        assert!(policy.can_read(Visibility::Private) && policy.can_command());
    }

    #[test]
    fn rejects_v3_and_invalid_payload() {
        let mut batch = Batch {
            v: 3,
            records: alloc::vec![record(1)],
        };
        assert_eq!(validate_batch(&batch), Err(Reject::Version));
        batch.v = 4;
        batch.records[0].blob = Some(geri_proto::v4::BlobRef {
            sha256: "0".repeat(64),
            size: 1,
        });
        assert_eq!(validate_batch(&batch), Err(Reject::Payload));
    }

    #[test]
    fn signed_summary_is_stable_and_seed_is_strict() {
        let seed = parse_signing_seed(&"11".repeat(32)).unwrap();
        let records = alloc::vec![StoredRecord {
            record: record(1),
            cursor: 7,
            accepted_at_ms: 100
        }];
        let a = sign_summary(6, &records, seed);
        assert_eq!(a, sign_summary(6, &records, seed));
        assert!(verify_summary(&a, &records));
        let mut changed = records.clone();
        changed[0].accepted_at_ms += 1;
        assert!(!verify_summary(&a, &changed));
        assert_ne!(a.signature, sign_summary(5, &records, seed).signature);
        assert!(parse_signing_seed(&"gg".repeat(32)).is_none());
    }

    #[test]
    fn declared_control_is_required_for_commands() {
        let control = geri_proto::Control::new(
            geri_proto::Shape::range(0.0, 10.0),
            geri_proto::Danger::Safe,
        );
        assert_eq!(validate_command(None, "set", &serde_json::json!(3)), None);
        assert_eq!(
            validate_command(Some(&control), "press", &serde_json::Value::Null),
            None
        );
        assert_eq!(
            validate_command(Some(&control), "set", &serde_json::json!(11)),
            None
        );
        assert_eq!(
            validate_command(Some(&control), "set", &serde_json::json!(3)),
            Some(serde_json::json!(3))
        );
    }

    #[test]
    fn summary_output_is_frozen() {
        // Elle yazılan hex'ten `hex` crate'ine geçerken çıktı bayt bayt aynı kalmalı.
        let page = [StoredRecord {
            record: record(1),
            cursor: 7,
            accepted_at_ms: 1_700_000_000_000,
        }];
        let s = sign_summary(6, &page, [7; 32]);
        assert_eq!(
            s.sha256,
            "6827184651a03fe252939a1e9fded79cb74e345f508cfb0fa2cb3e8ad272d547"
        );
        assert_eq!(
            s.public_key,
            "ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c"
        );
        assert_eq!(
            s.signature,
            "ed0283b690b84803ee8aca385f9eaeef487deb7739dd962addbfad2de883da96d46334573e403ffb62af557ade388a010b713aec9d1629a30767dca8f2e3b203"
        );
        assert!(verify_summary(&s, &page));
        let mut upper = s.clone();
        upper.signature = upper.signature.to_uppercase();
        assert!(verify_summary(&upper, &page));
    }
}
