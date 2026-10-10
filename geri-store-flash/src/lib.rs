// Testler Claude tarafından yazılmıştır.

#![no_std]

extern crate alloc;

use alloc::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt::Debug;
use embassy_embedded_hal::adapter::BlockingAsync;
use embassy_futures::block_on;
use embedded_storage::nor_flash::{MultiwriteNorFlash, NorFlash};
use geri_adapter_sdk::{Draft, IdCollision, SequenceError, Store, renumber};
use geri_proto::v4::{Record, RecordKind};
use sequential_storage::{
    cache::{Cache, Uncached},
    map::{MapConfig, MapStorage, SerializationError},
    queue::{QueueConfig, QueueStorage},
};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const UNSYNCED: u8 = 0;
pub const SYNCED: u8 = 1;

const ENTRY_MAX: usize = 8 + 17 + 8 + 64;

const DEFAULT_CONTENT_TYPE: &str = "application/json";

#[derive(Clone, Debug)]
pub struct Config {
    pub seen_window: u64,
    pub lazy_records: usize,
    pub max_names: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            seen_window: 128,
            lazy_records: 0,
            max_names: 32,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error<E: Debug> {
    #[error("flash: {0:?}")]
    Flash(sequential_storage::Error<E>),
    #[error("outbox is full")]
    Full,
    #[error("seen table is full of ids inside the window; enlarge it or shrink the window")]
    SeenFull,
    #[error("name dictionary is full; raise max_names or use fewer sources/entities")]
    NamesFull,
    #[error("record does not fit in a flash page")]
    TooLarge,
    #[error("the flash store does not keep blobs")]
    Blob,
    #[error("outbox holds an item that is not a Géri record: {0}")]
    Corrupt(String),
    #[error("config: {0}")]
    Config(&'static str),
    #[error(transparent)]
    IdCollision(#[from] IdCollision),
    #[error(transparent)]
    Sequence(#[from] SequenceError),
}

impl<E: Debug> From<sequential_storage::Error<E>> for Error<E> {
    fn from(e: sequential_storage::Error<E>) -> Self {
        match e {
            sequential_storage::Error::ItemTooBig => Self::TooLarge,
            e => Self::Flash(e),
        }
    }
}

pub const fn seen_capacity(seen_bytes: usize, erase_size: usize, word_size: usize) -> usize {
    let pages = seen_bytes / erase_size;
    if pages < 2 {
        return 0;
    }
    let entry = ENTRY_MAX.div_ceil(word_size) * word_size + word_size;
    let per_page = (erase_size - 2 * word_size) / entry;
    (pages - 1) * per_page
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Key {
    Seen([u8; 16]),
    Name(u8),
    Sync([u8; 8]),
}

impl sequential_storage::map::Key for Key {
    fn serialize_into(&self, buffer: &mut [u8]) -> Result<usize, SerializationError> {
        let (tag, body): (u8, &[u8]) = match self {
            Key::Seen(k) => (0, k),
            Key::Name(n) => (1, core::slice::from_ref(n)),
            Key::Sync(k) => (2, k),
        };
        let out = buffer
            .get_mut(..1 + body.len())
            .ok_or(SerializationError::BufferTooSmall)?;
        out[0] = tag;
        out[1..].copy_from_slice(body);
        Ok(out.len())
    }

    fn deserialize_from(buffer: &[u8]) -> Result<(Self, usize), SerializationError> {
        match buffer {
            [0, rest @ ..] if rest.len() >= 16 => {
                let mut k = [0; 16];
                k.copy_from_slice(&rest[..16]);
                Ok((Key::Seen(k), 17))
            }
            [1, n, ..] => Ok((Key::Name(*n), 2)),
            [2, rest @ ..] if rest.len() >= 8 => {
                let mut k = [0; 8];
                k.copy_from_slice(&rest[..8]);
                Ok((Key::Sync(k), 9))
            }
            _ => Err(SerializationError::InvalidFormat),
        }
    }
}

fn hash8(s: &str) -> [u8; 8] {
    let digest = Sha256::digest(s.as_bytes());
    let mut out = [0; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

fn key(source_id: &str, record_id: &str) -> Key {
    let mut k = [0; 16];
    k[..8].copy_from_slice(&hash8(source_id));
    k[8..].copy_from_slice(&hash8(record_id));
    Key::Seen(k)
}

#[derive(Debug, Default)]
struct Source {
    last: u64,
    synced: bool,
    delta: Option<i64>,
    unsynced: usize,
    stored: usize,
}

pub struct FlashStore<Q: NorFlash, M: NorFlash> {
    queue: QueueStorage<BlockingAsync<Q>, Cache<Uncached, Uncached, Uncached, ()>>,
    seen: MapStorage<Key, BlockingAsync<M>, Cache<Uncached, Uncached, Uncached, Key>>,
    buf: Vec<u8>,
    window: u64,
    sources: BTreeMap<String, Source>,
    floors: BTreeMap<[u8; 8], u64>,
    pending_ids: BTreeMap<(String, String), u64>,
    names: Vec<Option<String>>,
    max_names: usize,
    map_capacity: usize,
    tail: VecDeque<Record>,
    lazy: usize,
    count: usize,
}

struct Item {
    synced: bool,
    automatic: bool,
    record: Record,
}

struct RecordMeta {
    synced: bool,
    sequence: u64,
    automatic: bool,
}

type Packed<'a> = (
    u8,
    u8,
    u8,
    u64,
    Option<u64>,
    Option<&'a str>,
    Option<&'a str>,
    &'a Option<Value>,
    &'a Option<Value>,
);
type Unpacked = (
    u8,
    u8,
    u8,
    u64,
    Option<u64>,
    Option<String>,
    Option<String>,
    Option<Value>,
    Option<Value>,
);

fn kind_byte(kind: RecordKind) -> u8 {
    match kind {
        RecordKind::State => 0,
        RecordKind::Event => 1,
        RecordKind::Raw => 2,
    }
}

fn decode<E: Debug>(bytes: &[u8], names: &[Option<String>]) -> Result<Item, Error<E>> {
    let corrupt = |e: &dyn core::fmt::Display| Error::Corrupt(e.to_string());
    let (&kind, rest) = bytes
        .split_first()
        .ok_or_else(|| Error::Corrupt("empty item".into()))?;
    match kind {
        UNSYNCED | SYNCED => {
            let (k, src, ent, sequence, event_time_ms, record_id, content_type, data, indexed): Unpacked =
                geri_proto::cbor::from_slice(rest).map_err(|e| corrupt(&e))?;
            let name = |n: u8| {
                names
                    .get(n as usize)
                    .cloned()
                    .flatten()
                    .ok_or_else(|| Error::Corrupt(alloc::format!("unknown name {n}")))
            };
            let source_id = name(src)?;
            let automatic = record_id.is_none();
            let data = data.or_else(|| (k & 0x80 != 0).then_some(Value::Null));
            let indexed = indexed.or_else(|| (k & 0x40 != 0).then_some(Value::Null));
            Ok(Item {
                synced: kind == SYNCED,
                automatic,
                record: Record {
                    record_id: record_id
                        .unwrap_or_else(|| alloc::format!("{source_id}-{sequence}")),
                    source_id,
                    entity_id: name(ent)?,
                    sequence,
                    kind: match k & 0x3f {
                        0 => RecordKind::State,
                        1 => RecordKind::Event,
                        2 => RecordKind::Raw,
                        k => return Err(Error::Corrupt(alloc::format!("unknown record kind {k}"))),
                    },
                    event_time_ms,
                    content_type: content_type.unwrap_or_else(|| DEFAULT_CONTENT_TYPE.into()),
                    data,
                    indexed,
                    blob: None,
                },
            })
        }
        other => Err(Error::Corrupt(alloc::format!("unknown item kind {other}"))),
    }
}

impl<Q, M> FlashStore<Q, M>
where
    Q: NorFlash + MultiwriteNorFlash,
    M: NorFlash<Error = Q::Error> + MultiwriteNorFlash,
    Q::Error: Debug,
{
    pub fn open(queue: Q, seen: M, config: Config) -> Result<Self, Error<Q::Error>> {
        let queue_bytes = (queue.capacity() / Q::ERASE_SIZE * Q::ERASE_SIZE) as u32;
        let seen_bytes = (seen.capacity() / M::ERASE_SIZE * M::ERASE_SIZE) as u32;
        if (seen_capacity(
            seen_bytes as usize,
            M::ERASE_SIZE,
            M::WRITE_SIZE.max(M::READ_SIZE),
        ) as u64)
            < config.seen_window + 2 * config.max_names as u64
        {
            return Err(Error::Config(
                "seen region cannot hold seen_window ids + max_names names and sync marks (see seen_capacity)",
            ));
        }
        if config.max_names > 256 {
            return Err(Error::Config("max_names is at most 256"));
        }
        let queue_config = QueueConfig::try_new(0..queue_bytes)
            .map_err(|_| Error::Config("queue region must be at least one page"))?;
        let seen_config = MapConfig::try_new(0..seen_bytes)
            .map_err(|_| Error::Config("seen region must be at least two pages"))?;
        let mut store = Self {
            queue: QueueStorage::new(
                BlockingAsync::new(queue),
                queue_config,
                Cache::new_uncached(),
            ),
            seen: MapStorage::new(BlockingAsync::new(seen), seen_config, Cache::new_uncached()),
            buf: vec![0; Q::ERASE_SIZE.max(M::ERASE_SIZE)],
            window: config.seen_window.max(1),
            sources: BTreeMap::new(),
            floors: BTreeMap::new(),
            pending_ids: BTreeMap::new(),
            names: vec![None; config.max_names],
            max_names: config.max_names,
            map_capacity: seen_capacity(
                seen_bytes as usize,
                M::ERASE_SIZE,
                M::WRITE_SIZE.max(M::READ_SIZE),
            ),
            tail: VecDeque::new(),
            lazy: config.lazy_records,
            count: 0,
        };
        store.recover()?;
        Ok(store)
    }

    fn recover(&mut self) -> Result<(), Error<Q::Error>> {
        let mut scratch = vec![0; self.buf.len()];
        let mut marks: BTreeMap<String, i64> = BTreeMap::new();
        let mut iter = block_on(self.seen.fetch_all_items(&mut self.buf))?;
        while let Some((k, v)) = block_on(iter.next::<&[u8]>(&mut scratch))? {
            match k {
                Key::Seen(k) if v.len() >= 8 => {
                    let seq = u64::from_le_bytes(v[..8].try_into().unwrap_or_default());
                    let mut source = [0; 8];
                    source.copy_from_slice(&k[..8]);
                    let floor = self.floors.entry(source).or_default();
                    *floor = (*floor).max(seq);
                }
                Key::Name(n) => {
                    let name =
                        core::str::from_utf8(v).map_err(|e| Error::Corrupt(e.to_string()))?;
                    if self.names.len() <= n as usize {
                        self.names.resize(n as usize + 1, None);
                    }
                    self.names[n as usize] = Some(name.into());
                }
                Key::Seen(_) => {}
                Key::Sync(_) if v.len() >= 8 => {
                    let delta = i64::from_le_bytes(v[..8].try_into().unwrap_or_default());
                    let source =
                        core::str::from_utf8(&v[8..]).map_err(|e| Error::Corrupt(e.to_string()))?;
                    marks.insert(source.into(), delta);
                }
                Key::Sync(_) => return Err(Error::Corrupt("truncated sync mark".into())),
            }
        }

        let mut items = Vec::new();
        let mut iter = block_on(self.queue.iter())?;
        while let Some(entry) = block_on(iter.next(&mut self.buf))? {
            let Item {
                synced,
                automatic,
                record,
            } = decode(&entry, &self.names)?;
            items.push((synced, automatic, record));
        }
        for (synced, automatic, record) in items {
            if record.sequence == 0 {
                self.pending_ids
                    .insert((record.source_id, record.record_id), 0);
                self.count += 1;
                continue;
            }
            let source = self.source(&record.source_id);
            let record = if synced {
                source.synced = true;
                record
            } else {
                source.unsynced += 1;
                match marks.get(&record.source_id) {
                    Some(&delta) => {
                        source.synced = true;
                        source.delta = Some(delta);
                        renumber(record, delta, automatic)?
                    }
                    None => record,
                }
            };
            source.last = source.last.max(record.sequence);
            source.stored += 1;
            self.pending_ids
                .insert((record.source_id, record.record_id), record.sequence);
            self.count += 1;
        }

        let stale: BTreeSet<String> = marks
            .into_keys()
            .filter(|s| self.sources.get(s).is_none_or(|src| src.unsynced == 0))
            .collect();
        if !stale.is_empty() {
            self.pop_marks(|s| stale.contains(s))?;
        }
        Ok(())
    }

    fn source(&mut self, source_id: &str) -> &mut Source {
        if !self.sources.contains_key(source_id) {
            let last = self.floors.get(&hash8(source_id)).copied().unwrap_or(0);
            self.sources.insert(
                source_id.into(),
                Source {
                    last,
                    ..Source::default()
                },
            );
        }
        self.sources.get_mut(source_id).expect("just inserted")
    }

    fn pop_marks(&mut self, mut matches: impl FnMut(&str) -> bool) -> Result<(), Error<Q::Error>> {
        let mut remove = Vec::new();
        let mut scratch = vec![0; self.buf.len()];
        let mut iter = block_on(self.seen.fetch_all_items(&mut self.buf))?;
        while let Some((k, v)) = block_on(iter.next::<&[u8]>(&mut scratch))? {
            if let Key::Sync(_) = k
                && let Some(source) = v.get(8..).and_then(|s| core::str::from_utf8(s).ok())
                && matches(source)
            {
                remove.push(k);
            }
        }
        for k in remove {
            block_on(self.seen.remove_item(&mut self.buf, &k))?;
        }
        Ok(())
    }

    fn push(&mut self, kind: u8, payload: &[u8]) -> Result<(), Error<Q::Error>> {
        let mut item = Vec::with_capacity(payload.len() + 1);
        item.push(kind);
        item.extend_from_slice(payload);
        match block_on(self.queue.push(&item, false)) {
            Err(sequential_storage::Error::FullStorage) => Err(Error::Full),
            other => Ok(other?),
        }
    }

    fn write(&mut self, record: &Record, automatic: bool) -> Result<(), Error<Q::Error>> {
        let unordered = record.sequence == 0;
        let synced = unordered || self.source(&record.source_id).synced;
        let (src, ent) = self.name_ids(&record.source_id, &record.entity_id)?;
        let packed: Packed = (
            kind_byte(record.kind)
                | if record.data.is_some() { 0x80 } else { 0 }
                | if record.indexed.is_some() { 0x40 } else { 0 },
            src,
            ent,
            record.sequence,
            record.event_time_ms,
            (!automatic).then_some(record.record_id.as_str()),
            (record.content_type != DEFAULT_CONTENT_TYPE).then_some(record.content_type.as_str()),
            &record.data,
            &record.indexed,
        );
        let bytes = geri_proto::cbor::to_vec(&packed).map_err(|e| Error::Corrupt(e.to_string()))?;
        self.push(if synced { SYNCED } else { UNSYNCED }, &bytes)?;
        if unordered {
            return Ok(());
        }
        let s = self.source(&record.source_id);
        s.stored += 1;
        if !synced {
            s.unsynced += 1;
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<(), Error<Q::Error>> {
        while let Some(record) = self.tail.front().cloned() {
            self.write(&record, true)?;
            self.tail.pop_front();
        }
        Ok(())
    }

    fn seen(
        &mut self,
        source_id: &str,
        record_id: &str,
        last: u64,
    ) -> Result<bool, Error<Q::Error>> {
        if self
            .pending_ids
            .contains_key(&(source_id.into(), record_id.into()))
        {
            return Ok(true);
        }
        let found = block_on(
            self.seen
                .fetch_item::<&[u8]>(&mut self.buf, &key(source_id, record_id)),
        )?;
        Ok(found.is_some_and(|v| {
            v.len() >= 8
                && &v[8..] == record_id.as_bytes()
                && u64::from_le_bytes(v[..8].try_into().unwrap_or_default())
                    .saturating_add(self.window)
                    > last
        }))
    }

    fn remember(
        &mut self,
        source_id: &str,
        record_id: &str,
        sequence: u64,
    ) -> Result<(), Error<Q::Error>> {
        let mut value = Vec::with_capacity(8 + record_id.len());
        value.extend_from_slice(&sequence.to_le_bytes());
        value.extend_from_slice(record_id.as_bytes());
        self.put(&key(source_id, record_id), &value)
    }

    fn put(&mut self, k: &Key, value: &[u8]) -> Result<(), Error<Q::Error>> {
        if !matches!(k, Key::Sync(_))
            && block_on(self.seen.fetch_item::<&[u8]>(&mut self.buf, k))?.is_none()
            && self.non_sync_entries()? >= self.map_capacity - self.max_names
        {
            self.prune()?;
            if self.non_sync_entries()? >= self.map_capacity - self.max_names {
                return Err(Error::SeenFull);
            }
        }
        match block_on(self.seen.store_item(&mut self.buf, k, &value)) {
            Err(sequential_storage::Error::FullStorage) => {}
            other => return Ok(other?),
        }
        self.prune()?;
        match block_on(self.seen.store_item(&mut self.buf, k, &value)) {
            Err(sequential_storage::Error::FullStorage) => Err(Error::SeenFull),
            other => Ok(other?),
        }
    }

    fn non_sync_entries(&mut self) -> Result<usize, Error<Q::Error>> {
        let mut count = 0;
        let mut scratch = vec![0; self.buf.len()];
        let mut iter = block_on(self.seen.fetch_all_items(&mut self.buf))?;
        while let Some((k, _)) = block_on(iter.next::<&[u8]>(&mut scratch))? {
            if !matches!(k, Key::Sync(_)) {
                count += 1;
            }
        }
        Ok(count)
    }

    fn name_ids(&mut self, source: &str, entity: &str) -> Result<(u8, u8), Error<Q::Error>> {
        let find = |names: &[Option<String>], name: &str| {
            names.iter().position(|n| n.as_deref() == Some(name))
        };
        let mut new: Vec<&str> = [source, entity]
            .into_iter()
            .filter(|n| find(&self.names, n).is_none())
            .collect();
        new.dedup();
        let free = |names: &[Option<String>]| names.iter().filter(|n| n.is_none()).count();
        if free(&self.names[..self.max_names]) < new.len() {
            self.forget_unused_names(&[source, entity])?;
        }
        for name in new {
            let i = self.names[..self.max_names]
                .iter()
                .position(Option::is_none)
                .ok_or(Error::NamesFull)?;
            self.put(&Key::Name(i as u8), name.as_bytes())?;
            self.names[i] = Some(name.into());
        }
        let id = |name| {
            find(&self.names, name)
                .map(|i| i as u8)
                .ok_or(Error::NamesFull)
        };
        Ok((id(source)?, id(entity)?))
    }

    fn forget_unused_names(&mut self, keep: &[&str]) -> Result<(), Error<Q::Error>> {
        let mut used: BTreeSet<String> = keep.iter().map(|s| String::from(*s)).collect();
        let mut iter = block_on(self.queue.iter())?;
        while let Some(entry) = block_on(iter.next(&mut self.buf))? {
            let record = decode(&entry, &self.names)?.record;
            used.insert(record.source_id);
            used.insert(record.entity_id);
        }
        for i in 0..self.names.len() {
            if self.names[i].as_ref().is_some_and(|n| !used.contains(n)) {
                block_on(self.seen.remove_item(&mut self.buf, &Key::Name(i as u8)))?;
                self.names[i] = None;
            }
        }
        Ok(())
    }

    fn locate(
        &mut self,
        source_id: &str,
        record_id: &str,
        pop: bool,
    ) -> Result<Option<RecordMeta>, Error<Q::Error>> {
        let delta = self.sources.get(source_id).and_then(|s| s.delta);
        let mut iter = block_on(self.queue.iter())?;
        while let Some(entry) = block_on(iter.next(&mut self.buf))? {
            let Item {
                synced,
                automatic,
                record,
            } = decode(&entry, &self.names)?;
            if record.source_id != source_id {
                continue;
            }
            let record = match (synced, delta) {
                (false, Some(delta)) => renumber(record, delta, automatic)?,
                _ => record,
            };
            if record.record_id == record_id {
                if pop {
                    block_on(entry.pop())?;
                }
                return Ok(Some(RecordMeta {
                    synced,
                    sequence: record.sequence,
                    automatic,
                }));
            }
        }
        Ok(None)
    }

    fn first_unsynced(&mut self, source_id: &str) -> Result<Option<u64>, Error<Q::Error>> {
        let mut first: Option<u64> = None;
        let mut iter = block_on(self.queue.iter())?;
        while let Some(entry) = block_on(iter.next(&mut self.buf))? {
            if let Item {
                synced: false,
                record,
                ..
            } = decode(&entry, &self.names)?
                && record.source_id == source_id
                && record.sequence > 0
            {
                first = Some(first.map_or(record.sequence, |f| f.min(record.sequence)));
            }
        }
        Ok(first)
    }

    fn prune(&mut self) -> Result<(), Error<Q::Error>> {
        let lasts: BTreeMap<[u8; 8], u64> = self
            .sources
            .iter()
            .map(|(s, src)| (hash8(s), src.last))
            .collect();
        let mut expired = Vec::new();
        let mut scratch = vec![0; self.buf.len()];
        let mut iter = block_on(self.seen.fetch_all_items(&mut self.buf))?;
        while let Some((k, v)) = block_on(iter.next::<&[u8]>(&mut scratch))? {
            let Key::Seen(seen) = k else { continue };
            let mut source = [0; 8];
            source.copy_from_slice(&seen[..8]);
            let last = lasts
                .get(&source)
                .or_else(|| self.floors.get(&source))
                .copied()
                .unwrap_or(0);
            let seq = v
                .get(..8)
                .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap_or_default()));
            if seq.saturating_add(self.window) <= last {
                expired.push(k);
            }
        }
        for k in expired {
            block_on(self.seen.remove_item(&mut self.buf, &k))?;
        }
        Ok(())
    }
}

impl<Q, M> Store for FlashStore<Q, M>
where
    Q: NorFlash + MultiwriteNorFlash,
    M: NorFlash<Error = Q::Error> + MultiwriteNorFlash,
    Q::Error: Debug,
{
    type Error = Error<Q::Error>;

    fn append(&mut self, draft: Draft, blob: Option<&[u8]>) -> Result<Option<Record>, Self::Error> {
        if blob.is_some() || draft.blob.is_some() {
            return Err(Error::Blob);
        }
        let last = self.source(&draft.source_id).last;
        if let Some(id) = &draft.record_id
            && self.seen(&draft.source_id, id, last)?
        {
            return Ok(None);
        }
        let explicit = draft.record_id.is_some();
        let lazy = self.lazy > 0 && !explicit && draft.kind == RecordKind::State;
        let record = draft.into_record(last)?;
        if !explicit && self.seen(&record.source_id, &record.record_id, last)? {
            return Err(IdCollision(record.record_id).into());
        }
        if lazy && self.tail.len() >= self.lazy {
            self.spill()?;
        }
        if lazy && self.source(&record.source_id).stored == 0 {
            self.tail.push_back(record.clone());
        } else {
            self.spill()?;
            self.write(&record, !explicit)?;
        }
        self.pending_ids.insert(
            (record.source_id.clone(), record.record_id.clone()),
            record.sequence,
        );
        if record.sequence > 0 {
            self.source(&record.source_id).last = record.sequence;
        }
        self.count += 1;
        Ok(Some(record))
    }

    fn peek(&mut self, max: usize) -> Result<Vec<Record>, Self::Error> {
        let mut out = Vec::new();
        let mut iter = block_on(self.queue.iter())?;
        while out.len() < max
            && let Some(entry) = block_on(iter.next(&mut self.buf))?
        {
            let Item {
                synced,
                automatic,
                record,
            } = decode(&entry, &self.names)?;
            let delta = self.sources.get(&record.source_id).and_then(|s| s.delta);
            out.push(match (synced, delta) {
                (false, Some(delta)) => renumber(record, delta, automatic)?,
                _ => record,
            });
        }
        let room = max.saturating_sub(out.len());
        out.extend(self.tail.iter().take(room).cloned());
        Ok(out)
    }

    fn remove(&mut self, source_id: &str, record_id: &str) -> Result<(), Self::Error> {
        if let Some(i) = self
            .tail
            .iter()
            .position(|r| r.source_id == source_id && r.record_id == record_id)
        {
            self.tail.remove(i);
            self.pending_ids
                .remove(&(source_id.into(), record_id.into()));
            self.count -= 1;
            return Ok(());
        }
        let Some(RecordMeta {
            synced,
            sequence,
            automatic,
        }) = self.locate(source_id, record_id, false)?
        else {
            return Ok(());
        };
        if sequence > 0 && !automatic {
            self.remember(source_id, record_id, sequence)?;
        }
        self.locate(source_id, record_id, true)?;
        self.count -= 1;
        self.pending_ids
            .remove(&(source_id.into(), record_id.into()));
        if sequence == 0 {
            return Ok(());
        }
        let s = self.source(source_id);
        s.stored -= 1;
        if !synced {
            s.unsynced -= 1;
            if s.unsynced == 0 && s.delta.take().is_some() {
                self.pop_marks(|s| s == source_id)?;
            }
        }
        Ok(())
    }

    fn pending(&mut self) -> Result<usize, Self::Error> {
        Ok(self.count)
    }

    fn blob(&mut self, _sha256: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(None)
    }

    fn sync(
        &mut self,
        source_id: &str,
        next: Option<u64>,
    ) -> Result<Option<(u64, u64)>, Self::Error> {
        if next.is_some_and(|n| n > geri_proto::v4::MAX_SEQUENCE) {
            return Err(SequenceError::OutOfRange.into());
        }
        let (synced, unsynced, last) = {
            let s = self.source(source_id);
            (s.synced, s.unsynced, s.last)
        };
        if synced {
            return Ok(None);
        }
        let in_tail = self.tail.iter().find(|r| r.source_id == source_id);
        let next_after_last = last.checked_add(1).ok_or(SequenceError::OutOfRange)?;
        let first = match (unsynced, in_tail) {
            (0, Some(r)) => r.sequence,
            (0, None) => next_after_last,
            _ => self.first_unsynced(source_id)?.unwrap_or(next_after_last),
        };
        let delta = next
            .filter(|n| *n > 0)
            .map(|n| i64::try_from(i128::from(n) - i128::from(first)))
            .transpose()
            .map_err(|_| SequenceError::ResyncRange)?
            .unwrap_or(0);
        let shifted_last = last
            .checked_add_signed(delta)
            .filter(|n| *n <= geri_proto::v4::MAX_SEQUENCE)
            .ok_or(SequenceError::ResyncRange)?;
        let shifted_first = first
            .checked_add_signed(delta)
            .filter(|n| *n > 0 && *n <= geri_proto::v4::MAX_SEQUENCE)
            .ok_or(SequenceError::ResyncRange)?;
        let mut shifted_ids = BTreeMap::new();
        let mut iter = block_on(self.queue.iter())?;
        while let Some(entry) = block_on(iter.next(&mut self.buf))? {
            if let Item {
                synced,
                automatic,
                record,
            } = decode(&entry, &self.names)?
                && record.source_id == source_id
            {
                let r = if synced {
                    record
                } else {
                    renumber(record, delta, automatic)?
                };
                if shifted_ids
                    .insert(r.record_id.clone(), r.sequence)
                    .is_some()
                {
                    return Err(IdCollision(r.record_id).into());
                }
            }
        }
        for r in self.tail.iter().filter(|r| r.source_id == source_id) {
            let r = renumber(r.clone(), delta, true)?;
            if shifted_ids
                .insert(r.record_id.clone(), r.sequence)
                .is_some()
            {
                return Err(IdCollision(r.record_id).into());
            }
        }
        if unsynced > 0 {
            let mut payload = Vec::with_capacity(8 + source_id.len());
            payload.extend_from_slice(&delta.to_le_bytes());
            payload.extend_from_slice(source_id.as_bytes());
            self.put(&Key::Sync(hash8(source_id)), &payload)?;
        }
        let s = self.source(source_id);
        s.synced = true;
        s.last = shifted_last;
        if unsynced > 0 {
            s.delta = Some(delta);
        }
        if delta == 0 {
            return Ok(None);
        }
        for r in self.tail.iter_mut().filter(|r| r.source_id == source_id) {
            *r = renumber(r.clone(), delta, true)?;
        }
        self.pending_ids.retain(|(s, _), _| s != source_id);
        for (id, sequence) in shifted_ids {
            self.pending_ids.insert((source_id.into(), id), sequence);
        }
        Ok(Some((first, shifted_first)))
    }
}

#[cfg(test)]
mod tests;
