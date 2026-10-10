use alloc::{
    collections::{BTreeMap, BTreeSet},
    format,
    string::String,
    vec::Vec,
};
use geri_proto::v4::{BlobRef, Record, RecordKind};

pub const DEFAULT_SEEN_WINDOW: u64 = 100_000;

#[derive(Clone, Debug, PartialEq)]
pub struct Draft {
    pub source_id: String,
    pub entity_id: String,
    pub record_id: Option<String>,
    pub kind: RecordKind,
    pub event_time_ms: Option<u64>,
    pub content_type: String,
    pub data: Option<serde_json::Value>,
    pub indexed: Option<serde_json::Value>,
    pub blob: Option<BlobRef>,
    pub unordered: bool,
}

impl Draft {
    /// Son kalıcı sıra numarasından sonraki kaydı üretir; alan dolduysa hata verir.
    pub fn into_record(self, last: u64) -> Result<Record, SequenceError> {
        let sequence = if self.unordered {
            0
        } else {
            last.checked_add(1)
                .filter(|sequence| *sequence <= geri_proto::v4::MAX_SEQUENCE)
                .ok_or(SequenceError::Exhausted)?
        };
        let record_id = self
            .record_id
            .unwrap_or_else(|| format!("{}-{}", self.source_id, sequence));
        Ok(Record {
            source_id: self.source_id,
            entity_id: self.entity_id,
            record_id,
            sequence,
            kind: self.kind,
            event_time_ms: self.event_time_ms,
            content_type: self.content_type,
            data: self.data,
            indexed: self.indexed,
            blob: self.blob,
        })
    }
}

pub trait Store {
    type Error: core::fmt::Display;

    fn append(&mut self, draft: Draft, blob: Option<&[u8]>) -> Result<Option<Record>, Self::Error>;

    fn peek(&mut self, max: usize) -> Result<Vec<Record>, Self::Error>;

    fn remove(&mut self, source_id: &str, record_id: &str) -> Result<(), Self::Error>;

    fn pending(&mut self) -> Result<usize, Self::Error>;

    fn blob(&mut self, sha256: &str) -> Result<Option<Vec<u8>>, Self::Error>;

    fn sync(
        &mut self,
        source_id: &str,
        next: Option<u64>,
    ) -> Result<Option<(u64, u64)>, Self::Error>;
}

pub fn is_auto_id(source_id: &str, record_id: &str, sequence: u64) -> bool {
    record_id
        .strip_prefix(source_id)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|n| n.parse() == Ok(sequence))
}

/// Henüz hub'ın görmediği kaydı, desteklenen sequence alanında yeniden numaralandırır.
pub fn renumber(mut record: Record, delta: i64, automatic: bool) -> Result<Record, SequenceError> {
    let old = record.sequence;
    if old == 0 {
        return Ok(record);
    }
    record.sequence = old
        .checked_add_signed(delta)
        .filter(|sequence| *sequence > 0 && *sequence <= geri_proto::v4::MAX_SEQUENCE)
        .ok_or(SequenceError::ResyncRange)?;
    if automatic {
        record.record_id = format!("{}-{}", record.source_id, record.sequence);
    }
    Ok(record)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
/// Sequence üretimi veya hub resync'i sırasında oluşan sınır hatası.
pub enum SequenceError {
    #[error("source sequence is exhausted")]
    /// Kaynak desteklenen en büyük sequence'a ulaştı.
    Exhausted,
    #[error("hub sequence is outside the exact supported domain")]
    /// Hub desteklenen aralık dışında bir sonraki sıra bildirdi.
    OutOfRange,
    #[error("resync would move a sequence outside the exact supported domain")]
    /// Resync sonucu sıralı kayıt alan dışına taşacaktı.
    ResyncRange,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
/// Bellek içi outbox işlemlerinin çakışma ve sıra sınırı hataları.
pub enum MemStoreError {
    #[error(transparent)]
    IdCollision(#[from] IdCollision),
    #[error(transparent)]
    Sequence(#[from] SequenceError),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("record id `{0}` is already used; provide another record_id")]
pub struct IdCollision(pub String);

#[derive(Clone, Debug)]
pub struct MemStore {
    outbox: BTreeMap<u64, Record>,
    ids: BTreeMap<(String, String), u64>,
    next_key: u64,
    seen: BTreeMap<(String, String), u64>,
    seen_order: BTreeMap<(String, u64), String>,
    seen_window: u64,
    last: BTreeMap<String, u64>,
    blobs: BTreeMap<String, Vec<u8>>,
    synced: BTreeSet<String>,
    automatic: BTreeSet<u64>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self {
            outbox: BTreeMap::new(),
            ids: BTreeMap::new(),
            next_key: 0,
            seen: BTreeMap::new(),
            seen_order: BTreeMap::new(),
            seen_window: DEFAULT_SEEN_WINDOW,
            last: BTreeMap::new(),
            blobs: BTreeMap::new(),
            synced: BTreeSet::new(),
            automatic: BTreeSet::new(),
        }
    }
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_seen_window(mut self, records: u64) -> Self {
        self.seen_window = records.max(1);
        self
    }

    fn remember(&mut self, source: &str, record_id: &str, sequence: u64) {
        if let Some(old) = self
            .seen
            .insert((source.into(), record_id.into()), sequence)
        {
            self.seen_order.remove(&(source.into(), old));
        }
        self.seen_order
            .insert((source.into(), sequence), record_id.into());
        let last = self.last.get(source).copied().unwrap_or(0);
        let oldest_kept = last.saturating_sub(self.seen_window) + 1;
        let expired: Vec<u64> = self
            .seen_order
            .range((String::from(source), 0)..(String::from(source), oldest_kept))
            .map(|((_, seq), _)| *seq)
            .collect();
        for seq in expired {
            if let Some(id) = self.seen_order.remove(&(source.into(), seq)) {
                self.seen.remove(&(source.into(), id));
            }
        }
    }
}

impl Store for MemStore {
    type Error = MemStoreError;

    fn append(
        &mut self,
        draft: Draft,
        blob: Option<&[u8]>,
    ) -> Result<Option<Record>, MemStoreError> {
        let last = self.last.get(&draft.source_id).copied().unwrap_or(0);
        if let Some(id) = &draft.record_id {
            let key = (draft.source_id.clone(), id.clone());
            let remembered = self
                .seen
                .get(&key)
                .is_some_and(|seq| seq.saturating_add(self.seen_window) > last);
            if remembered || self.ids.contains_key(&key) {
                return Ok(None);
            }
        }
        let automatic = draft.record_id.is_none();
        let record = draft.into_record(last)?;
        let id = (record.source_id.clone(), record.record_id.clone());
        if self.ids.contains_key(&id)
            || self
                .seen
                .get(&id)
                .is_some_and(|seq| seq.saturating_add(self.seen_window) > last)
        {
            return Err(IdCollision(record.record_id).into());
        }
        if record.sequence > 0 {
            self.last.insert(record.source_id.clone(), record.sequence);
        }
        if let (Some(r), Some(bytes)) = (&record.blob, blob) {
            self.blobs.insert(r.sha256.clone(), bytes.into());
        }
        let key = self.next_key;
        self.next_key += 1;
        if automatic {
            self.automatic.insert(key);
        }
        self.ids
            .insert((record.source_id.clone(), record.record_id.clone()), key);
        self.outbox.insert(key, record.clone());
        Ok(Some(record))
    }

    fn peek(&mut self, max: usize) -> Result<Vec<Record>, MemStoreError> {
        Ok(self.outbox.values().take(max).cloned().collect())
    }

    fn remove(&mut self, source_id: &str, record_id: &str) -> Result<(), MemStoreError> {
        let Some(key) = self.ids.remove(&(source_id.into(), record_id.into())) else {
            return Ok(());
        };
        let Some(record) = self.outbox.remove(&key) else {
            return Ok(());
        };
        if record.sequence > 0 && !self.automatic.remove(&key) {
            self.remember(source_id, record_id, record.sequence);
        }
        if let Some(blob) = record.blob
            && !self
                .outbox
                .values()
                .any(|r| r.blob.as_ref().is_some_and(|b| b.sha256 == blob.sha256))
        {
            self.blobs.remove(&blob.sha256);
        }
        Ok(())
    }

    fn pending(&mut self) -> Result<usize, MemStoreError> {
        Ok(self.outbox.len())
    }

    fn blob(&mut self, sha256: &str) -> Result<Option<Vec<u8>>, MemStoreError> {
        Ok(self.blobs.get(sha256).cloned())
    }

    fn sync(
        &mut self,
        source_id: &str,
        next: Option<u64>,
    ) -> Result<Option<(u64, u64)>, MemStoreError> {
        if next.is_some_and(|n| n > geri_proto::v4::MAX_SEQUENCE) {
            return Err(SequenceError::OutOfRange.into());
        }
        if self.synced.contains(source_id) {
            return Ok(None);
        }
        let Some(next) = next.filter(|n| *n > 0) else {
            self.synced.insert(source_id.into());
            return Ok(None);
        };
        let last = self.last.get(source_id).copied().unwrap_or(0);
        let next_after_last = last.checked_add(1).ok_or(SequenceError::OutOfRange)?;
        let first = self
            .outbox
            .values()
            .find(|r| r.source_id == source_id && r.sequence > 0)
            .map_or(next_after_last, |r| r.sequence);
        if first == next {
            self.synced.insert(source_id.into());
            return Ok(None);
        }
        let delta = i64::try_from(i128::from(next) - i128::from(first))
            .map_err(|_| SequenceError::ResyncRange)?;
        let shifted_last = shift_sequence(last, delta)?;
        let shifted_seen = self
            .seen
            .iter()
            .filter(|((source, _), _)| source == source_id)
            .map(|(key, sequence)| Ok((key.clone(), shift_sequence(*sequence, delta)?)))
            .collect::<Result<Vec<_>, SequenceError>>()?;
        let shifted_order = self
            .seen_order
            .range((String::from(source_id), 0)..=(String::from(source_id), u64::MAX))
            .map(|((_, sequence), id)| Ok((shift_sequence(*sequence, delta)?, id.clone())))
            .collect::<Result<Vec<_>, SequenceError>>()?;
        let moved: Vec<u64> = self
            .outbox
            .iter()
            .filter(|(_, r)| r.source_id == source_id && r.sequence > 0)
            .map(|(k, _)| *k)
            .collect();
        let mut ids = self.ids.clone();
        for key in &moved {
            let r = &self.outbox[key];
            ids.remove(&(r.source_id.clone(), r.record_id.clone()));
        }
        for key in &moved {
            let r = renumber(
                self.outbox[key].clone(),
                delta,
                self.automatic.contains(key),
            )?;
            if ids
                .insert((r.source_id.clone(), r.record_id.clone()), *key)
                .is_some()
            {
                return Err(IdCollision(r.record_id).into());
            }
        }
        for key in &moved {
            let record = &self.outbox[key];
            self.ids
                .remove(&(record.source_id.clone(), record.record_id.clone()));
        }
        for key in moved {
            if let Some(record) = self.outbox.remove(&key) {
                let record = renumber(record, delta, self.automatic.contains(&key))?;
                self.ids
                    .insert((record.source_id.clone(), record.record_id.clone()), key);
                self.outbox.insert(key, record);
            }
        }
        self.last.insert(source_id.into(), shifted_last);
        for (key, sequence) in shifted_seen {
            self.seen.insert(key, sequence);
        }
        self.seen_order.retain(|(source, _), _| source != source_id);
        for (seq, id) in shifted_order {
            self.seen_order.insert((source_id.into(), seq), id);
        }
        self.synced.insert(source_id.into());
        Ok(Some((first, next)))
    }
}

/// Sequence değerini saturating işlem yapmadan verilen fark kadar kaydırır.
pub fn shift_sequence(sequence: u64, delta: i64) -> Result<u64, SequenceError> {
    sequence
        .checked_add_signed(delta)
        .filter(|shifted| *shifted <= geri_proto::v4::MAX_SEQUENCE)
        .ok_or(SequenceError::ResyncRange)
}
