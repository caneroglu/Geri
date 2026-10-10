use crate::{Draft, Error, Store, stable_id};
use alloc::{
    collections::{BTreeSet, VecDeque},
    format,
    string::{String, ToString},
    vec::Vec,
};
use geri_proto::v4::{
    BlobRef, Command, CommandResult, Limits, MAX_BATCH_RECORDS, Receipt, ReceiptStatus, RecordKind,
    SourceDeclaration, VERSION,
    msg::{Frame, ToHub, ToSource},
    valid_id, validate,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const MAX_BLOB_BYTES: usize = 16 * 1024 * 1024;
const SEQUENCE_DIGITS: usize = 20;
pub(crate) const MAX_QUEUED_ERRORS: usize = 64;

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub sources: Vec<SourceDeclaration>,
    pub claim_every_ms: u64,
    pub live_watched_ms: u64,
    pub live_idle_ms: u64,
    pub request_timeout_ms: u64,
    pub backoff_min_ms: u64,
    pub backoff_max_ms: u64,
}

impl Config {
    pub fn new(sources: Vec<SourceDeclaration>) -> Self {
        Self {
            sources,
            claim_every_ms: 10_000,
            live_watched_ms: 2_000,
            live_idle_ms: 10_000,
            request_timeout_ms: 30_000,
            backoff_min_ms: 1_000,
            backoff_max_ms: 60_000,
        }
    }

    /// İlanları ve tekrar eden kaynak kimliklerini I/O başlamadan doğrular.
    pub fn validate(&self) -> Result<(), Error> {
        let mut seen = BTreeSet::new();
        for source in &self.sources {
            validate_declaration(source)?;
            if !seen.insert(&source.source_id) {
                return Err(Error::DuplicateSource(source.source_id.clone()));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    Connected {
        push: bool,
    },
    Command(Command),
    Watched {
        source_id: String,
        watched: bool,
    },
    Renumbered {
        source_id: String,
        from: u64,
        to: u64,
    },
    UploadBlobs(Vec<BlobRef>),
    Error(Error),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Offline,
    Hello,
    Ready,
}

#[derive(Debug)]
enum Req {
    Hello,
    Declare(usize, SourceDeclaration),
    Batch {
        keys: Vec<(String, String)>,
        blobs: Vec<BlobRef>,
    },
    Live(usize),
    Claim(usize),
    Result(String, CommandResult),
}

#[derive(Debug)]
struct InFlight {
    id: u32,
    req: Req,
    deadline: u64,
}

#[derive(Clone, Copy, Debug)]
struct Gate {
    at: u64,
    backoff: u64,
}

impl Gate {
    fn open(&self, now: u64) -> bool {
        now >= self.at
    }
    fn ok(&mut self, now: u64, min: u64) {
        self.at = now;
        self.backoff = min;
    }
    fn fail(&mut self, now: u64, max: u64) {
        self.at = now.saturating_add(self.backoff);
        self.backoff = (self.backoff.saturating_mul(2)).min(max);
    }
}

#[derive(Debug)]
struct Source {
    decl: SourceDeclaration,
    declared: bool,
    live: Option<Map<String, Value>>,
    next_live_at: u64,
    pace_ms: Option<u32>,
    watched: bool,
    ring: bool,
    commandable: bool,
    next_claim_at: Option<u64>,
}

impl Source {
    fn new(decl: SourceDeclaration) -> Self {
        Self {
            commandable: commandable(&decl),
            decl,
            declared: false,
            live: None,
            next_live_at: 0,
            pace_ms: None,
            watched: false,
            ring: false,
            next_claim_at: None,
        }
    }
}

#[derive(Debug)]
pub struct Session<S> {
    config: Config,
    store: S,
    phase: Phase,
    connected: bool,
    pushes: bool,
    limits: Option<Limits>,
    sources: Vec<Source>,
    in_flight: Option<InFlight>,
    next_id: u32,
    results: VecDeque<(String, CommandResult)>,
    events: VecDeque<Event>,
    outbox_maybe: bool,
    synced: BTreeSet<String>,
    batch_cap: usize,
    reconnect: Gate,
    handshake: Gate,
    outbox: Gate,
    replies: Gate,
    claim_hold: bool,
    /// Claim aramasının başladığı kaynak: komutu bitmeyen kaynak ötekileri aç bırakmasın.
    /// (Live'da gerek yok: sıradaki zamanı cevaptan sonra kurulur.)
    claim_from: usize,
}

impl<S: Store> Session<S> {
    /// Geçersiz Config, oturum veya bağlantı yaratmadan hata döner.
    pub fn new(config: Config, store: S) -> Result<Self, Error> {
        config.validate()?;
        let min = config.backoff_min_ms;
        let gate = Gate {
            at: 0,
            backoff: min,
        };
        Ok(Self {
            sources: config.sources.iter().cloned().map(Source::new).collect(),
            config,
            store,
            phase: Phase::Offline,
            connected: false,
            pushes: false,
            limits: None,
            in_flight: None,
            next_id: 0,
            results: VecDeque::new(),
            events: VecDeque::new(),
            synced: BTreeSet::new(),
            batch_cap: MAX_BATCH_RECORDS,
            outbox_maybe: true,
            reconnect: gate,
            handshake: gate,
            outbox: gate,
            replies: gate,
            claim_hold: false,
            claim_from: 0,
        })
    }

    /// Sürücü döngüsü bağlantı geçişlerini izleyebilsin diye salt okunur durum.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Tüketici komut kuyruğu doluyken yeni claim üretilmesin diye sürücü freni.
    /// Kayıpsız backpressure: bekleyen claim'ler değil, yeni claim istekleri durur.
    pub fn set_claim_hold(&mut self, hold: bool) {
        self.claim_hold = hold;
    }

    pub fn declare(&mut self, decl: SourceDeclaration) -> Result<(), Error> {
        validate_declaration(&decl)?;
        match self
            .sources
            .iter_mut()
            .find(|s| s.decl.source_id == decl.source_id)
        {
            Some(source) => {
                source.commandable = commandable(&decl);
                source.decl = decl;
                source.declared = false;
            }
            None => self.sources.push(Source::new(decl)),
        }
        Ok(())
    }

    pub fn state(
        &mut self,
        source_id: &str,
        entity_id: &str,
        data: Value,
    ) -> Result<Option<geri_proto::v4::Record>, Error> {
        self.record(inline(
            source_id,
            entity_id,
            None,
            RecordKind::State,
            None,
            data,
        ))
    }

    pub fn event(
        &mut self,
        source_id: &str,
        entity_id: &str,
        record_id: Option<&str>,
        event_time_ms: Option<u64>,
        data: Value,
    ) -> Result<Option<geri_proto::v4::Record>, Error> {
        self.record(inline(
            source_id,
            entity_id,
            record_id,
            RecordKind::Event,
            event_time_ms,
            data,
        ))
    }

    pub fn unordered(
        &mut self,
        source_id: &str,
        entity_id: &str,
        record_id: &str,
        kind: RecordKind,
        event_time_ms: Option<u64>,
        data: Value,
    ) -> Result<Option<geri_proto::v4::Record>, Error> {
        let mut draft = inline(
            source_id,
            entity_id,
            Some(record_id),
            kind,
            event_time_ms,
            data,
        );
        draft.unordered = true;
        self.record(draft)
    }

    pub fn record(&mut self, draft: Draft) -> Result<Option<geri_proto::v4::Record>, Error> {
        self.check(&draft)?;
        self.append(draft, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn blob(
        &mut self,
        source_id: &str,
        entity_id: &str,
        record_id: Option<&str>,
        kind: RecordKind,
        event_time_ms: Option<u64>,
        content_type: &str,
        bytes: &[u8],
        indexed: Value,
    ) -> Result<Option<geri_proto::v4::Record>, Error> {
        if bytes.is_empty() || bytes.len() > MAX_BLOB_BYTES {
            return Err(Error::InvalidPayload);
        }
        let draft = Draft {
            source_id: source_id.into(),
            entity_id: entity_id.into(),
            record_id: record_id.map(Into::into),
            kind,
            event_time_ms,
            content_type: content_type.into(),
            data: None,
            indexed: Some(indexed),
            blob: Some(BlobRef {
                sha256: hex::encode(Sha256::digest(bytes)),
                size: bytes.len() as u64,
            }),
            unordered: false,
        };
        self.check(&draft)?;
        self.append(draft, Some(bytes))
    }

    pub fn cloudevent(
        &mut self,
        source_id: &str,
        event: Value,
    ) -> Result<Option<geri_proto::v4::Record>, Error> {
        let field = |name| {
            event
                .get(name)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
        };
        let (Some("1.0"), Some(src), Some(id), Some(_)) = (
            field("specversion"),
            field("source"),
            field("id"),
            field("type"),
        ) else {
            return Err(Error::InvalidCloudEvent);
        };
        let key = stable_id(&format!("{src}\0{id}"));
        let mut data = Map::new();
        data.insert("cloudevent".into(), event);
        self.record(inline(
            source_id,
            "cloudevent",
            Some(&key),
            RecordKind::Event,
            None,
            Value::Object(data),
        ))
    }

    pub fn live(&mut self, source_id: &str, values: Map<String, Value>) -> Result<(), Error> {
        let source = self.source_mut(source_id)?;
        source.live = Some(values);
        Ok(())
    }

    pub fn result(&mut self, command: &Command, ok: bool, result: Value) {
        self.results.push_back((
            command.source_id.clone(),
            CommandResult {
                command_id: command.command_id.clone(),
                ok,
                result,
            },
        ));
    }

    pub fn blobs_uploaded(&mut self, now: u64) {
        self.outbox.ok(now, self.config.backoff_min_ms);
    }

    pub fn limits(&self) -> Option<&Limits> {
        self.limits.as_ref()
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn into_store(self) -> S {
        self.store
    }

    pub fn wants_connect(&self, now: u64) -> bool {
        !self.connected && self.reconnect.open(now)
    }

    pub fn handle_connected(&mut self, now: u64, pushes: bool) {
        self.connected = true;
        self.pushes = pushes;
        self.phase = Phase::Hello;
        self.handshake.ok(now, self.config.backoff_min_ms);
        for i in 0..self.sources.len() {
            self.set_watched(i, false);
            let source = &mut self.sources[i];
            source.declared = false;
            source.pace_ms = None;
        }
    }

    pub fn handle_disconnected(&mut self, now: u64) {
        self.connected = false;
        self.phase = Phase::Offline;
        if let Some(InFlight {
            req: Req::Result(source, result),
            ..
        }) = self.in_flight.take()
        {
            self.results.push_front((source, result));
        }
        self.reconnect.fail(now, self.config.backoff_max_ms);
    }

    pub fn handle_timeout(&mut self, now: u64) {
        if self.in_flight.as_ref().is_some_and(|f| now >= f.deadline) {
            self.error(Error::Timeout);
            self.handle_disconnected(now);
        }
    }

    pub fn handle_frame(&mut self, now: u64, frame: Frame<ToSource>) {
        let Some(id) = frame.id else {
            self.handle_signal(now, frame.msg);
            return;
        };
        if self.in_flight.as_ref().is_none_or(|f| f.id != id) {
            return;
        }
        let req = self.in_flight.take().map(|f| f.req).unwrap_or(Req::Hello);
        match (req, frame.msg) {
            (Req::Hello, ToSource::Welcome(welcome)) => {
                if welcome.v != VERSION {
                    self.error(Error::Version(welcome.v));
                    self.handshake.fail(now, self.config.backoff_max_ms);
                    return;
                }
                self.limits = Some(welcome.limits);
                self.phase = Phase::Ready;
                self.reconnect.ok(now, self.config.backoff_min_ms);
                self.handshake.ok(now, self.config.backoff_min_ms);
                for source in &mut self.sources {
                    source.next_live_at = now;
                    if !source.commandable {
                        source.ring = false;
                        source.next_claim_at = None;
                    } else if self.pushes {
                        source.ring = true;
                        source.next_claim_at = None;
                    } else {
                        source.next_claim_at = Some(now);
                    }
                }
                self.events
                    .push_back(Event::Connected { push: self.pushes });
            }
            (Req::Declare(i, sent), ToSource::Ok { next_sequence, .. }) => {
                if let Some(source) = self.sources.get_mut(i) {
                    source.declared = source.decl == sent;
                    let source_id = source.decl.source_id.clone();
                    if !self.sync(now, source_id, next_sequence) {
                        self.sources[i].declared = false;
                        self.handshake.fail(now, self.config.backoff_max_ms);
                        return;
                    }
                }
                self.handshake.ok(now, self.config.backoff_min_ms);
            }
            (Req::Batch { keys, .. }, ToSource::Receipt(receipt)) => {
                self.handle_receipt(now, keys, receipt);
            }
            (Req::Batch { blobs, .. }, ToSource::Error { error, .. })
                if error.code == "missing_blob" && !blobs.is_empty() =>
            {
                self.events.push_back(Event::UploadBlobs(blobs));
                self.outbox.fail(now, self.config.backoff_max_ms);
            }
            (Req::Live(i), ToSource::Ok { watched, .. }) => {
                if !self.pushes {
                    self.set_watched(i, watched.unwrap_or(false));
                }
                let every = self.live_every(i);
                if let Some(source) = self.sources.get_mut(i) {
                    source.next_live_at = now.saturating_add(every);
                }
            }
            (Req::Claim(i), ToSource::Command(command)) => {
                let Some(source) = self.sources.get_mut(i) else {
                    return;
                };
                if command.source_id != source.decl.source_id {
                    self.protocol(now, "command for another source");
                    return;
                }
                if self.pushes {
                    source.ring = true;
                } else {
                    source.next_claim_at = Some(now);
                }
                self.events.push_back(Event::Command(command));
            }
            (Req::Claim(i), ToSource::Idle) => {
                let every = self.config.claim_every_ms;
                let pushes = self.pushes;
                if let Some(source) = self.sources.get_mut(i) {
                    source.next_claim_at =
                        (!pushes && source.commandable).then(|| now.saturating_add(every));
                }
            }
            (Req::Result(..), ToSource::Ok { .. }) => {
                self.replies.ok(now, self.config.backoff_min_ms);
            }
            (req, ToSource::Error { status, error }) => {
                self.error(Error::Hub {
                    status,
                    code: error.code,
                    message: error.message,
                });
                self.handle_rejection(now, req, status);
            }
            _ => self.protocol(now, "answer does not match the request"),
        }
    }

    pub fn poll_transmit(&mut self, now: u64) -> Option<Frame<ToHub>> {
        if !self.connected || self.in_flight.is_some() {
            return None;
        }
        let (req, msg) = match self.phase {
            Phase::Offline => return None,
            Phase::Hello if self.handshake.open(now) => (
                Req::Hello,
                ToHub::Hello {
                    v: VERSION,
                    auth: None,
                },
            ),
            Phase::Hello => return None,
            Phase::Ready => self.next_request(now)?,
        };
        self.next_id = self.next_id.wrapping_add(1);
        self.in_flight = Some(InFlight {
            id: self.next_id,
            req,
            deadline: now.saturating_add(self.config.request_timeout_ms),
        });
        Some(Frame {
            id: Some(self.next_id),
            msg,
        })
    }

    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// Sürücü kapanırken henüz uygulama kanalına teslim edemediği olayı korur.
    pub fn return_event(&mut self, event: Event) {
        self.events.push_front(event);
    }

    /// Yalnız cevap bekleyen isteğin son tarihi; başka işlerin hazır zamanı değildir.
    pub fn request_deadline(&self) -> Option<u64> {
        self.in_flight.as_ref().map(|request| request.deadline)
    }

    pub fn poll_timeout(&self) -> Option<u64> {
        if !self.connected {
            return Some(self.reconnect.at);
        }
        if let Some(f) = &self.in_flight {
            return Some(f.deadline);
        }
        match self.phase {
            Phase::Offline => None,
            Phase::Hello => Some(self.handshake.at),
            Phase::Ready => {
                if self.sources.iter().any(|s| !s.declared) {
                    return Some(self.handshake.at);
                }
                let mut next: Option<u64> = None;
                let mut at = |t: u64| next = Some(next.map_or(t, |n| n.min(t)));
                if !self.results.is_empty() {
                    at(self.replies.at);
                }
                if self.outbox_maybe {
                    at(self.outbox.at);
                }
                for (i, source) in self.sources.iter().enumerate() {
                    if !self.claim_hold && source.ring {
                        at(0);
                    }
                    if !self.claim_hold
                        && let Some(t) = source.next_claim_at
                    {
                        at(t);
                    }
                    if self.live_allowed(i) {
                        at(source.next_live_at);
                    }
                }
                next
            }
        }
    }

    fn next_request(&mut self, now: u64) -> Option<(Req, ToHub)> {
        if self.handshake.open(now)
            && let Some(i) = self.sources.iter().position(|s| !s.declared)
        {
            return Some((
                Req::Declare(i, self.sources[i].decl.clone()),
                ToHub::Declare(self.sources[i].decl.clone()),
            ));
        }
        if self.sources.iter().any(|s| !s.declared) {
            return None;
        }
        if self.replies.open(now)
            && let Some((source_id, result)) = self.results.pop_front()
        {
            let msg = ToHub::Result {
                source_id: source_id.clone(),
                result: result.clone(),
            };
            return Some((Req::Result(source_id, result), msg));
        }
        let n = self.sources.len();
        for i in (0..n).map(|k| (self.claim_from + k) % n) {
            let source = &self.sources[i];
            if !self.claim_hold && (source.ring || source.next_claim_at.is_some_and(|t| now >= t)) {
                let source_id = source.decl.source_id.clone();
                let source = &mut self.sources[i];
                source.ring = false;
                source.next_claim_at = None;
                self.claim_from = i + 1;
                return Some((Req::Claim(i), ToHub::Claim { source_id }));
            }
        }
        if self.outbox_maybe && self.outbox.open(now) {
            let max = self
                .limits
                .as_ref()
                .map_or(MAX_BATCH_RECORDS, |l| l.max_batch_records)
                .clamp(1, MAX_BATCH_RECORDS)
                .min(self.batch_cap);
            match self.store.peek(max) {
                Ok(records) if records.is_empty() => self.outbox_maybe = false,
                Ok(mut records) => {
                    let limit = self
                        .limits
                        .as_ref()
                        .map_or(usize::MAX, |l| l.max_body_bytes);
                    let envelope = Frame {
                        id: Some(u32::MAX),
                        msg: ToHub::Batch(geri_proto::v4::Batch {
                            v: VERSION,
                            records: Vec::new(),
                        }),
                    };
                    let mut json_bytes = json_len(&envelope);
                    let mut cbor_bytes = cbor_len(&envelope).saturating_add(8);
                    let mut fit = 0;
                    for record in &records {
                        if let Err(reason) = validate::record(record) {
                            self.error(Error::InvalidOutboxRecord {
                                source_id: record.source_id.clone(),
                                record_id: record.record_id.clone(),
                                reason,
                            });
                            self.outbox.fail(now, self.config.backoff_max_ms);
                            return None;
                        }
                        json_bytes = json_bytes.saturating_add(json_len(record).saturating_add(1));
                        cbor_bytes = cbor_bytes.saturating_add(cbor_len(record));
                        if json_bytes > limit || cbor_bytes > limit {
                            break;
                        }
                        fit += 1;
                    }
                    records.truncate(fit.max(1));
                    let numbered = |r: &&geri_proto::v4::Record| r.sequence > 0;
                    for r in records.iter().filter(numbered) {
                        self.sync(now, r.source_id.clone(), None);
                    }
                    if records
                        .iter()
                        .filter(numbered)
                        .any(|r| !self.synced.contains(&r.source_id))
                    {
                        return None;
                    }
                    let keys = records
                        .iter()
                        .map(|r| (r.source_id.clone(), r.record_id.clone()))
                        .collect();
                    let blobs = records.iter().filter_map(|r| r.blob.clone()).collect();
                    let batch = geri_proto::v4::Batch {
                        v: VERSION,
                        records,
                    };
                    return Some((Req::Batch { keys, blobs }, ToHub::Batch(batch)));
                }
                Err(e) => {
                    self.error(Error::Store(e.to_string()));
                    self.outbox.fail(now, self.config.backoff_max_ms);
                }
            }
        }
        for i in 0..n {
            if self.live_allowed(i) && now >= self.sources[i].next_live_at {
                let every = self.live_every(i);
                let source = &mut self.sources[i];
                let values = source.live.clone().unwrap_or_default();
                source.next_live_at = now.saturating_add(every);
                let source_id = source.decl.source_id.clone();
                return Some((Req::Live(i), ToHub::Live { source_id, values }));
            }
        }
        None
    }

    fn handle_signal(&mut self, now: u64, msg: ToSource) {
        match msg {
            ToSource::Pace { source_id, ms } => {
                if let Some(i) = self.index(&source_id) {
                    self.sources[i].pace_ms = Some(ms);
                    self.sources[i].next_live_at = now;
                    self.set_watched(i, ms > 0);
                }
            }
            ToSource::Ring { source_id } => {
                if let Some(i) = self.index(&source_id) {
                    self.sources[i].ring = self.sources[i].commandable;
                }
            }
            _ => {}
        }
    }

    fn sync(&mut self, now: u64, source_id: String, next: Option<u64>) -> bool {
        if self.synced.contains(&source_id) {
            return true;
        }
        match self.store.sync(&source_id, next) {
            Ok(renumbered) => {
                if let Some((from, to)) = renumbered {
                    self.events.push_back(Event::Renumbered {
                        source_id: source_id.clone(),
                        from,
                        to,
                    });
                    self.outbox_maybe = true;
                }
                self.synced.insert(source_id);
                true
            }
            Err(e) => {
                self.error(Error::Store(e.to_string()));
                self.outbox.fail(now, self.config.backoff_max_ms);
                false
            }
        }
    }

    fn handle_receipt(&mut self, now: u64, keys: Vec<(String, String)>, receipt: Receipt) {
        if receipt.v != VERSION || receipt.records.len() != keys.len() {
            self.protocol(now, "receipt does not match the batch");
            return;
        }
        for ((source_id, record_id), answer) in keys.into_iter().zip(receipt.records) {
            if answer.source_id != source_id || answer.record_id != record_id {
                self.protocol(now, "receipt does not match the batch");
                return;
            }
            let problem = match answer.status {
                ReceiptStatus::Accepted | ReceiptStatus::Duplicate if answer.cursor.is_some() => {
                    match self.store.remove(&source_id, &record_id) {
                        Ok(()) => continue,
                        Err(e) => Error::Store(e.to_string()),
                    }
                }
                ReceiptStatus::Gap => Error::Gap {
                    source_id,
                    expected: answer.expected_sequence.unwrap_or(0),
                },
                ReceiptStatus::Conflict | ReceiptStatus::Rejected => Error::Conflict {
                    source_id,
                    record_id,
                },
                _ => Error::Protocol("receipt without a cursor"),
            };
            self.error(problem);
            self.outbox.fail(now, self.config.backoff_max_ms);
            return;
        }
        self.batch_cap = MAX_BATCH_RECORDS;
        self.outbox.ok(now, self.config.backoff_min_ms);
    }

    fn handle_rejection(&mut self, now: u64, req: Req, status: u16) {
        let max = self.config.backoff_max_ms;
        match req {
            Req::Hello | Req::Declare(..) => self.handshake.fail(now, max),
            Req::Batch { keys, .. } => {
                if status == 413 {
                    if keys.len() > 1 {
                        self.batch_cap = (keys.len() / 2).max(1);
                        return;
                    }
                    if let Some((source_id, record_id)) = keys.into_iter().next() {
                        self.error(Error::RecordTooLarge {
                            source_id,
                            record_id,
                        });
                    }
                }
                self.outbox.fail(now, max);
            }
            Req::Live(i) => {
                let every = self.config.live_idle_ms;
                if let Some(source) = self.sources.get_mut(i) {
                    source.next_live_at = now.saturating_add(every);
                }
            }
            Req::Claim(i) => {
                let every = self.config.claim_every_ms;
                if let Some(source) = self.sources.get_mut(i) {
                    source.next_claim_at = Some(now.saturating_add(every));
                }
            }
            Req::Result(source, result) => {
                if status >= 500 {
                    self.results.push_front((source, result));
                }
                self.replies.fail(now, max);
            }
        }
    }

    fn error(&mut self, error: Error) {
        let queued = self
            .events
            .iter()
            .filter(|e| matches!(e, Event::Error(_)))
            .count();
        if queued < MAX_QUEUED_ERRORS {
            self.events.push_back(Event::Error(error));
        }
    }

    fn protocol(&mut self, now: u64, what: &'static str) {
        self.error(Error::Protocol(what));
        self.handle_disconnected(now);
    }

    fn check(&self, draft: &Draft) -> Result<(), Error> {
        let id_ok = match &draft.record_id {
            Some(id) if looks_automatic(&draft.source_id, id) => {
                return Err(Error::ReservedRecordId);
            }
            Some(id) => valid_id(id),
            None if draft.unordered => return Err(Error::MissingRecordId),
            None => draft.source_id.len() + 1 + SEQUENCE_DIGITS <= 64,
        };
        if !valid_id(&draft.source_id) || !valid_id(&draft.entity_id) || !id_ok {
            return Err(Error::InvalidId);
        }
        self.index(&draft.source_id)
            .ok_or_else(|| Error::UnknownSource(draft.source_id.clone()))?;
        validate::payload(
            &draft.content_type,
            draft.data.as_ref(),
            draft.indexed.as_ref(),
            draft.blob.as_ref(),
        )
        .map_err(|_| Error::InvalidPayload)
    }

    fn append(
        &mut self,
        draft: Draft,
        blob: Option<&[u8]>,
    ) -> Result<Option<geri_proto::v4::Record>, Error> {
        let record = self
            .store
            .append(draft, blob)
            .map_err(|e| Error::Store(e.to_string()))?;
        if record.is_some() {
            self.outbox_maybe = true;
        }
        Ok(record)
    }

    fn index(&self, source_id: &str) -> Option<usize> {
        self.sources
            .iter()
            .position(|s| s.decl.source_id == source_id)
    }

    fn source_mut(&mut self, source_id: &str) -> Result<&mut Source, Error> {
        let i = self
            .index(source_id)
            .ok_or_else(|| Error::UnknownSource(source_id.into()))?;
        Ok(&mut self.sources[i])
    }

    fn live_allowed(&self, i: usize) -> bool {
        let source = &self.sources[i];
        source.live.is_some() && (!self.pushes || source.pace_ms.is_some_and(|ms| ms > 0))
    }

    fn live_every(&self, i: usize) -> u64 {
        let source = &self.sources[i];
        match (self.pushes, source.pace_ms) {
            (true, Some(ms)) if ms > 0 => u64::from(ms),
            _ if source.watched => self.config.live_watched_ms,
            _ => self.config.live_idle_ms,
        }
    }

    fn set_watched(&mut self, i: usize, watched: bool) {
        let source = &mut self.sources[i];
        if source.watched != watched {
            source.watched = watched;
            self.events.push_back(Event::Watched {
                source_id: source.decl.source_id.clone(),
                watched,
            });
        }
    }
}

fn validate_declaration(decl: &SourceDeclaration) -> Result<(), Error> {
    validate::source(decl).map_err(|reason| Error::InvalidSource {
        source_id: decl.source_id.clone(),
        reason,
    })
}

fn inline(
    source_id: &str,
    entity_id: &str,
    record_id: Option<&str>,
    kind: RecordKind,
    event_time_ms: Option<u64>,
    data: Value,
) -> Draft {
    Draft {
        source_id: source_id.into(),
        entity_id: entity_id.into(),
        record_id: record_id.map(Into::into),
        kind,
        event_time_ms,
        content_type: "application/json".into(),
        data: Some(data),
        indexed: None,
        blob: None,
        unordered: false,
    }
}

fn commandable(decl: &SourceDeclaration) -> bool {
    decl.entities.iter().any(|e| e.control.is_some())
}

fn looks_automatic(source_id: &str, record_id: &str) -> bool {
    record_id
        .strip_prefix(source_id)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

fn json_len<T: serde::Serialize + ?Sized>(value: &T) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |b| b.len())
}

fn cbor_len<T: serde::Serialize + ?Sized>(value: &T) -> usize {
    geri_proto::cbor::encoded_len(value).unwrap_or(usize::MAX)
}
