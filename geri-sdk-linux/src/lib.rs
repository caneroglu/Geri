// Testler Claude tarafından yazılmıştır.

use geri_adapter_sdk::{Session, Store, Transport};
use geri_store_sqlite::SqliteStore;
use geri_transport_reqwest::HttpTransport;
use geri_transport_ws::WsTransport;
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, VecDeque},
    io,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    sync::{Notify, mpsc, watch},
    time::Instant,
};

pub use geri_adapter_sdk::{Config, Error, Event};
pub use geri_proto::{
    self,
    v4::{Command, Entity, Record, RecordKind, SourceDeclaration},
};
pub use geri_transport_reqwest::Codec;

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("outbox: {0}")]
    Store(#[from] geri_store_sqlite::Error),
    #[error("source: {0}")]
    Source(#[from] Error),
    #[error("hub url must start with http://, https://, ws:// or wss://")]
    Url,
    #[error("key file {path}: {source}")]
    KeyFile { path: PathBuf, source: io::Error },
    #[error("key: {0}")]
    Key(#[from] geri_sign::Error),
    #[error("hub key must be 64 hex characters")]
    HubKey,
    #[error("noise needs the device key: call .key(file)")]
    NoiseNeedsKey,
    #[error("hub_key is only used with a tcp:// or quic:// hub address")]
    HubKeyWithoutTcp,
    #[cfg(feature = "quic")]
    #[error("quic: {0}")]
    Quic(#[from] geri_transport_quinn::ConfigError),
    #[error("{0}:// support is not compiled in: enable the `{0}` feature")]
    Disabled(&'static str),
    #[error("start() must be called inside a tokio runtime")]
    NoRuntime,
}

#[derive(Debug)]
pub struct Builder {
    url: String,
    token: String,
    agent_id: String,
    key: Option<PathBuf>,
    hub_key: Option<String>,
    outbox: Option<PathBuf>,
    codec: Codec,
    link: Link,
    config: Config,
    driver: DriverConfig,
}

/// Sürücü döngüsü sınırları (F06–F08): bağlantı kurma, tek I/O ve blob yükleme
/// için üst süreler. I/O askıda kalırsa oturum deadline'ı da send'i keser;
/// `disconnect` yarım akışı bırakır, sonraki `connect` temiz oturum kurar.
#[derive(Clone, Copy, Debug)]
pub struct DriverConfig {
    pub connect_timeout: Duration,
    pub io_timeout: Duration,
    pub upload_timeout: Duration,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            io_timeout: Duration::from_secs(30),
            upload_timeout: Duration::from_secs(120),
        }
    }
}

/// Uygulamaya giden olay kuyruğu kapasitesi (F08): yavaş tüketici karşısında
/// bellek sınırı. Komutlar taşma deposunda korunur; eksik blob yüklemesi
/// kalıcı outbox batch'inin sonraki missing_blob cevabıyla yeniden denenir.
pub const EVENT_CAP: usize = 256;
/// Taşma deposundaki kayıpsız olay üst sınırı; dolunca oturumdan olay çekme
/// durur (kayıpsız backpressure), kanal boşalınca devam eder.
const OVERFLOW_CAP: usize = EVENT_CAP;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Link {
    #[default]
    Auto,
    WebSocket,
    Http,
}

pub fn builder(
    url: impl Into<String>,
    token: impl Into<String>,
    agent_id: impl Into<String>,
) -> Builder {
    Builder {
        url: url.into(),
        token: token.into(),
        agent_id: agent_id.into(),
        key: None,
        hub_key: None,
        outbox: None,
        codec: Codec::Json,
        link: Link::Auto,
        config: Config::new(Vec::new()),
        driver: DriverConfig::default(),
    }
}

impl Builder {
    pub fn source(mut self, decl: SourceDeclaration) -> Self {
        self.config.sources.push(decl);
        self
    }

    pub fn outbox(mut self, path: impl Into<PathBuf>) -> Self {
        self.outbox = Some(path.into());
        self
    }

    pub fn key(mut self, path: impl Into<PathBuf>) -> Self {
        self.key = Some(path.into());
        self
    }

    pub fn hub_key(mut self, hex: impl Into<String>) -> Self {
        self.hub_key = Some(hex.into());
        self
    }

    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn link(mut self, link: Link) -> Self {
        self.link = link;
        self
    }

    pub fn configure(mut self, f: impl FnOnce(&mut Config)) -> Self {
        f(&mut self.config);
        self
    }

    /// `connect` için üst süre (varsayılan 10 sn).
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.driver.connect_timeout = d;
        self
    }

    /// Tek `send` için üst süre (varsayılan 30 sn). Oturumun istek deadline'ı
    /// daha erken dolarsa send yine kesilir ve yeni bağlantı kurulur.
    pub fn io_timeout(mut self, d: Duration) -> Self {
        self.driver.io_timeout = d;
        self
    }

    /// Tek blob yükleme için üst süre (varsayılan 120 sn).
    pub fn upload_timeout(mut self, d: Duration) -> Self {
        self.driver.upload_timeout = d;
        self
    }

    pub fn start(self) -> Result<(Geri, mpsc::Receiver<Event>), StartError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| StartError::NoRuntime)?;
        self.config.validate()?;
        let store = match &self.outbox {
            Some(path) => SqliteStore::open(path)?,
            None => SqliteStore::memory()?,
        };
        let ws_codec = match self.codec {
            Codec::Json => geri_transport_ws::Codec::Json,
            Codec::Cbor => geri_transport_ws::Codec::Cbor,
        };
        let seed = match &self.key {
            Some(path) => {
                let hex = std::fs::read_to_string(path).map_err(|source| StartError::KeyFile {
                    path: path.clone(),
                    source,
                })?;
                Some(hex.trim().to_owned())
            }
            None => None,
        };
        let signer = match &seed {
            Some(hex) => Some(Arc::new(geri_sign::Signer::from_hex(&self.agent_id, hex)?)),
            None => None,
        };
        let geri = Geri::new(self.config, store)?;
        let (events, rx) = mpsc::channel(EVENT_CAP);
        let driver = self.driver;
        #[cfg(not(feature = "tcp"))]
        if self.url.starts_with("tcp://") {
            return Err(StartError::Disabled("tcp"));
        }
        #[cfg(not(feature = "quic"))]
        if self.url.starts_with("quic://") {
            return Err(StartError::Disabled("quic"));
        }
        #[cfg(feature = "tcp")]
        if let Some(addr) = self.url.strip_prefix("tcp://") {
            let mut tcp = match &self.hub_key {
                Some(hub) => {
                    let hub = geri_sign::parse_key(hub.trim()).ok_or(StartError::HubKey)?;
                    let seed = seed
                        .as_deref()
                        .and_then(geri_sign::parse_key)
                        .ok_or(StartError::NoiseNeedsKey)?;
                    geri_transport_tcp::TcpTransport::noise(addr, seed, hub)
                }
                None => {
                    let tcp = geri_transport_tcp::TcpTransport::new(addr, &self.token);
                    match signer {
                        Some(signer) => tcp.with_signer(signer),
                        None => tcp,
                    }
                }
            };
            tcp = tcp.with_codec(ws_codec);
            runtime.spawn(run_with(driver, geri.clone(), NoBlobs(tcp), events));
            return Ok((geri, rx));
        }
        #[cfg(feature = "quic")]
        if let Some(addr) = self.url.strip_prefix("quic://") {
            let mut quic = match &self.hub_key {
                Some(hub) => {
                    let hub = geri_sign::parse_key(hub.trim()).ok_or(StartError::HubKey)?;
                    geri_transport_quinn::pinned(addr, hub, &self.token)?
                }
                None => geri_transport_quinn::webpki(addr, &self.token)?,
            };
            if let Some(signer) = signer {
                quic = quic.with_signer(signer);
            }
            quic = quic.with_codec(ws_codec);
            runtime.spawn(run_with(driver, geri.clone(), NoBlobs(quic), events));
            return Ok((geri, rx));
        }
        if self.hub_key.is_some() {
            return Err(StartError::HubKeyWithoutTcp);
        }
        let mut ws = WsTransport::new(&self.url, &self.token, &self.agent_id)
            .map_err(|_| StartError::Url)?
            .with_codec(ws_codec);
        let mut http =
            HttpTransport::new(self.url, self.token, self.agent_id).with_codec(self.codec);
        if let Some(signer) = signer {
            ws = ws.with_signer(signer.clone());
            http = http.with_signer(signer);
        }
        let transport = AutoTransport::new(ws, http, self.link);
        runtime.spawn(run_registered(
            driver,
            geri.clone(),
            transport,
            events,
            geri.begin_driver(),
        ));
        Ok((geri, rx))
    }
}

#[derive(Debug)]
pub struct Geri<S = SqliteStore> {
    session: Arc<Mutex<Session<S>>>,
    wake: Arc<Notify>,
    epoch: watch::Sender<u64>,
    driver_lock: Arc<tokio::sync::Mutex<()>>,
    started: Instant,
}

impl<S> Clone for Geri<S> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            wake: self.wake.clone(),
            epoch: self.epoch.clone(),
            driver_lock: self.driver_lock.clone(),
            started: self.started,
        }
    }
}

impl<S: Store> Geri<S> {
    /// Geçersiz ilanları sürücü kurulmadan reddeder.
    pub fn new(config: Config, store: S) -> Result<Self, Error> {
        Ok(Self {
            session: Arc::new(Mutex::new(Session::new(config, store)?)),
            wake: Arc::new(Notify::new()),
            epoch: watch::channel(0).0,
            driver_lock: Arc::new(tokio::sync::Mutex::new(())),
            started: Instant::now(),
        })
    }

    /// Sürücü görevini durdurur (F08): soket bir sonraki turda bırakılır, task biter.
    /// Askıda I/O iptal edilir. Temizliğin tamamlanmasını beklemek için
    /// `shutdown_and_wait` kullanılır. Alıcı kapandığında da aynı iptal uygulanır.
    pub fn shutdown(&self) {
        self.epoch
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    /// İptalden sonra çalışan sürücünün I/O ve transport temizliğini bekler.
    pub async fn shutdown_and_wait(&self) {
        self.shutdown();
        let _stopped = self.driver_lock.lock().await;
    }

    fn begin_driver(&self) -> (watch::Receiver<u64>, u64) {
        let rx = self.epoch.subscribe();
        let mut mine = 0;
        self.epoch.send_modify(|epoch| {
            *epoch = epoch.wrapping_add(1);
            mine = *epoch;
        });
        (rx, mine)
    }

    pub fn state(&self, source_id: &str, entity_id: &str, data: Value) -> Result<(), Error> {
        self.write(|s| s.state(source_id, entity_id, data))
    }

    pub fn event(
        &self,
        source_id: &str,
        entity_id: &str,
        record_id: Option<&str>,
        event_time_ms: Option<u64>,
        data: Value,
    ) -> Result<(), Error> {
        self.write(|s| s.event(source_id, entity_id, record_id, event_time_ms, data))
    }

    pub fn unordered(
        &self,
        source_id: &str,
        entity_id: &str,
        record_id: &str,
        kind: RecordKind,
        event_time_ms: Option<u64>,
        data: Value,
    ) -> Result<(), Error> {
        self.write(|s| s.unordered(source_id, entity_id, record_id, kind, event_time_ms, data))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn blob(
        &self,
        source_id: &str,
        entity_id: &str,
        record_id: Option<&str>,
        kind: RecordKind,
        event_time_ms: Option<u64>,
        content_type: &str,
        bytes: &[u8],
        indexed: Value,
    ) -> Result<(), Error> {
        self.write(|s| {
            s.blob(
                source_id,
                entity_id,
                record_id,
                kind,
                event_time_ms,
                content_type,
                bytes,
                indexed,
            )
        })
    }

    pub fn cloudevent(&self, source_id: &str, event: Value) -> Result<(), Error> {
        self.write(|s| s.cloudevent(source_id, event))
    }

    pub fn live(&self, source_id: &str, values: Map<String, Value>) -> Result<(), Error> {
        self.lock().live(source_id, values)?;
        self.wake.notify_one();
        Ok(())
    }

    pub fn result(&self, command: &Command, ok: bool, result: Value) {
        self.lock().result(command, ok, result);
        self.wake.notify_one();
    }

    pub fn declare(&self, decl: SourceDeclaration) -> Result<(), Error> {
        self.lock().declare(decl)?;
        self.wake.notify_one();
        Ok(())
    }

    pub fn pending(&self) -> Result<usize, Error> {
        self.lock()
            .store_mut()
            .pending()
            .map_err(|e| Error::Store(e.to_string()))
    }

    fn write<T>(&self, f: impl FnOnce(&mut Session<S>) -> Result<T, Error>) -> Result<(), Error> {
        f(&mut self.lock())?;
        self.wake.notify_one();
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Session<S>> {
        self.session.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[allow(async_fn_in_trait)]
pub trait UploadBlob {
    type Error: std::fmt::Display;
    async fn upload_blob(&self, sha256: &str, bytes: Vec<u8>) -> Result<(), Self::Error>;
}

impl UploadBlob for HttpTransport {
    type Error = geri_transport_reqwest::Error;
    async fn upload_blob(&self, sha256: &str, bytes: Vec<u8>) -> Result<(), Self::Error> {
        self.put_blob(sha256, bytes).await
    }
}

#[derive(Debug)]
pub struct NoBlobs<T>(pub T);

impl<T: Transport> Transport for NoBlobs<T> {
    type Error = T::Error;

    fn disconnect(&mut self) {
        self.0.disconnect();
    }

    fn pushes(&self) -> bool {
        self.0.pushes()
    }

    async fn connect(&mut self) -> Result<(), T::Error> {
        self.0.connect().await
    }

    async fn send(
        &mut self,
        frame: geri_proto::v4::msg::Frame<geri_proto::v4::msg::ToHub>,
    ) -> Result<(), T::Error> {
        self.0.send(frame).await
    }

    async fn recv(
        &mut self,
    ) -> Result<geri_proto::v4::msg::Frame<geri_proto::v4::msg::ToSource>, T::Error> {
        self.0.recv().await
    }
}

impl<T> UploadBlob for NoBlobs<T> {
    type Error = &'static str;

    async fn upload_blob(&self, _sha256: &str, _bytes: Vec<u8>) -> Result<(), Self::Error> {
        Err("this transport cannot upload blobs")
    }
}

#[derive(Debug)]
pub struct AutoTransport {
    ws: WsTransport,
    http: HttpTransport,
    link: Link,
    on_ws: bool,
    retry_ws_at: Option<Instant>,
    ws_retry: Duration,
}

const WS_RETRY: Duration = Duration::from_secs(300);

impl AutoTransport {
    pub fn new(ws: WsTransport, http: HttpTransport, link: Link) -> Self {
        Self {
            ws,
            http,
            link,
            on_ws: false,
            retry_ws_at: None,
            ws_retry: WS_RETRY,
        }
    }

    pub fn with_ws_retry(mut self, every: Duration) -> Self {
        self.ws_retry = every;
        self
    }

    pub fn on_websocket(&self) -> bool {
        self.on_ws
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    #[error(transparent)]
    Http(#[from] geri_transport_reqwest::Error),
    #[error(transparent)]
    Ws(#[from] geri_transport_ws::Error),
    #[error("switching back to websocket")]
    Upgrade,
}

impl Transport for AutoTransport {
    type Error = LinkError;

    fn disconnect(&mut self) {
        self.ws.disconnect();
        self.http.disconnect();
    }

    fn pushes(&self) -> bool {
        self.on_ws
    }

    async fn connect(&mut self) -> Result<(), LinkError> {
        let try_ws = match self.link {
            Link::Http => false,
            Link::WebSocket => true,
            Link::Auto => self.retry_ws_at.is_none_or(|at| Instant::now() >= at),
        };
        if try_ws {
            match self.ws.connect().await {
                Ok(()) => {
                    self.on_ws = true;
                    self.retry_ws_at = None;
                    return Ok(());
                }
                Err(e) if self.link == Link::WebSocket => return Err(e.into()),
                Err(_) => self.retry_ws_at = Some(Instant::now() + self.ws_retry),
            }
        }
        self.on_ws = false;
        Ok(self.http.connect().await?)
    }

    async fn send(
        &mut self,
        frame: geri_proto::v4::msg::Frame<geri_proto::v4::msg::ToHub>,
    ) -> Result<(), LinkError> {
        if self.on_ws {
            Ok(self.ws.send(frame).await?)
        } else {
            Ok(self.http.send(frame).await?)
        }
    }

    async fn recv(
        &mut self,
    ) -> Result<geri_proto::v4::msg::Frame<geri_proto::v4::msg::ToSource>, LinkError> {
        if self.on_ws {
            return match self.ws.recv().await {
                Err(geri_transport_ws::Error::Replaced) if self.link == Link::Auto => {
                    self.retry_ws_at = Some(Instant::now() + self.ws_retry);
                    Err(geri_transport_ws::Error::Replaced.into())
                }
                other => Ok(other?),
            };
        }
        let retry = match (self.link, self.retry_ws_at) {
            (Link::Auto, Some(at)) => Some(at),
            _ => None,
        };
        tokio::select! {
            biased;
            frame = self.http.recv() => Ok(frame?),
            () = sleep(retry) => {
                self.retry_ws_at = None;
                Err(LinkError::Upgrade)
            }
        }
    }
}

impl UploadBlob for AutoTransport {
    type Error = geri_transport_reqwest::Error;
    async fn upload_blob(&self, sha256: &str, bytes: Vec<u8>) -> Result<(), Self::Error> {
        self.http.put_blob(sha256, bytes).await
    }
}

/// Taşma deposu (F08): kanal doluyken komut/blob olayları sırayla kayıpsız tutulur,
/// izleme/bağlantı/hata olayları son değerine indirgenir. Kaynak sayısı kadar
/// izleme/renumber girdisi, tekil bağlantı/hata hücresi ve `OVERFLOW_CAP` ile
/// sınırlı komut deposu taşır.
#[derive(Debug, Default)]
struct Overflow {
    queued: VecDeque<Event>,
    watched: HashMap<String, bool>,
    renumbered: HashMap<String, (u64, u64)>,
    connected: Option<bool>,
    error: Option<Event>,
    last_error: Option<String>,
}

impl Overflow {
    fn is_empty(&self) -> bool {
        self.queued.is_empty()
            && self.watched.is_empty()
            && self.renumbered.is_empty()
            && self.connected.is_none()
            && self.error.is_none()
    }

    /// Dolup taşma deposu da dolarsa false döner: çağıran oturumdan olay çekmeyi
    /// bırakır, kanal boşalınca kaldığı yerden devam eder (kayıpsız backpressure).
    fn stash(&mut self, event: Event) -> bool {
        match event {
            Event::Command(_) | Event::UploadBlobs(_) => {
                if self.queued.len() >= OVERFLOW_CAP {
                    return false;
                }
                self.queued.push_back(event);
            }
            Event::Watched { source_id, watched } => {
                self.watched.insert(source_id, watched);
            }
            Event::Renumbered {
                source_id,
                from,
                to,
            } => {
                self.renumbered.insert(source_id, (from, to));
            }
            Event::Connected { push } => self.connected = Some(push),
            Event::Error(e) => {
                let msg = e.to_string();
                if self.last_error.as_ref() != Some(&msg)
                    && self.error.as_ref().is_none_or(
                        |prev| !matches!(prev, Event::Error(prev) if prev.to_string() == msg),
                    )
                {
                    self.error = Some(Event::Error(e));
                }
            }
        }
        true
    }

    fn flush(&mut self, events: &mpsc::Sender<Event>) {
        while let Some(event) = self.queued.pop_front() {
            match events.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.queued.push_front(event);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
        let mut rest: Vec<_> = std::mem::take(&mut self.watched).into_iter().collect();
        while let Some((source_id, watched)) = rest.pop() {
            let event = Event::Watched {
                source_id: source_id.clone(),
                watched,
            };
            if events.try_send(event).is_err() {
                self.watched.insert(source_id, watched);
                self.watched.extend(rest.drain(..));
                return;
            }
        }
        let mut renumbered: Vec<_> = std::mem::take(&mut self.renumbered).into_iter().collect();
        while let Some((source_id, (from, to))) = renumbered.pop() {
            let event = Event::Renumbered {
                source_id: source_id.clone(),
                from,
                to,
            };
            if events.try_send(event).is_err() {
                self.renumbered.insert(source_id, (from, to));
                self.renumbered.extend(renumbered.drain(..));
                return;
            }
        }
        if let Some(push) = self.connected.take()
            && events.try_send(Event::Connected { push }).is_err()
        {
            self.connected = Some(push);
            return;
        }
        if let Some(event) = self.error.take() {
            let msg = match &event {
                Event::Error(e) => e.to_string(),
                _ => unreachable!(),
            };
            match events.try_send(event) {
                Ok(()) => self.last_error = Some(msg),
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.error = Some(event);
                }
                Err(mpsc::error::TrySendError::Closed(event)) => {
                    self.error = Some(event);
                }
            }
        }
    }
}

/// Kanal müsaitse gönderir, doluysa taşma deposuna koyar.
fn push(events: &mpsc::Sender<Event>, overflow: &mut Overflow, event: Event) -> bool {
    if !overflow.is_empty() {
        return overflow.stash(event);
    }
    match events.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(event)) => overflow.stash(event),
        Err(mpsc::error::TrySendError::Closed(_)) => true,
    }
}

/// Bağlantı kaybı: ilk geçişte tek hata olayı + tek gate ilerlemesi (F06).
/// Kopukken gelen anında hatalar kapıyı ileri itmez, olay seli üretmez;
/// döngü backoff kapısında park eder. Bağlanma denemesi gerçekten yapıldıysa
/// (`gate` true) kapı normal backoff ile ilerler.
fn mark_broken<S: Store>(
    geri: &Geri<S>,
    events: &mpsc::Sender<Event>,
    overflow: &mut Overflow,
    down: &mut bool,
    now: u64,
    what: String,
    gate: bool,
) {
    if *down && !gate {
        return;
    }
    *down = true;
    push(events, overflow, Event::Error(Error::Transport(what)));
    geri.lock().handle_disconnected(now);
}

pub async fn run<S, T>(geri: Geri<S>, transport: T, events: mpsc::Sender<Event>)
where
    S: Store,
    T: Transport + UploadBlob,
{
    run_with(DriverConfig::default(), geri, transport, events).await
}

pub async fn run_with<S, T>(
    cfg: DriverConfig,
    geri: Geri<S>,
    transport: T,
    events: mpsc::Sender<Event>,
) where
    S: Store,
    T: Transport + UploadBlob,
{
    let token = geri.begin_driver();
    run_registered(cfg, geri, transport, events, token).await;
}

async fn cancelled(mut rx: watch::Receiver<u64>, epoch: u64) {
    loop {
        if *rx.borrow_and_update() != epoch {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

async fn run_registered<S: Store, T: Transport + UploadBlob>(
    cfg: DriverConfig,
    geri: Geri<S>,
    mut transport: T,
    events: mpsc::Sender<Event>,
    token: (watch::Receiver<u64>, u64),
) {
    let (rx, epoch) = token;
    let stop = cancelled(rx, epoch);
    tokio::pin!(stop);
    let guard = tokio::select! {
        biased;
        () = &mut stop => return,
        () = events.closed() => return,
        guard = geri.driver_lock.lock() => guard,
    };
    let mut overflow = Overflow::default();
    tokio::select! {
        biased;
        () = &mut stop => {},
        () = events.closed() => {},
        () = drive(cfg, &geri, &mut transport, &events, &mut overflow) => {},
    }
    transport.disconnect();
    let mut session = geri.lock();
    session.handle_disconnected(geri.started.elapsed().as_millis() as u64);
    session.set_claim_hold(false);
    // Kanalın kabul etmediği komutlar yeni sürücüde tekrar teslim edilebilir.
    while let Some(event) = overflow.queued.pop_back() {
        session.return_event(event);
    }
    drop(session);
    drop(guard);
}

async fn drive<S: Store, T: Transport + UploadBlob>(
    cfg: DriverConfig,
    geri: &Geri<S>,
    transport: &mut T,
    events: &mpsc::Sender<Event>,
    overflow: &mut Overflow,
) {
    let start = geri.started;
    let now = || start.elapsed().as_millis() as u64;
    let at = |ms: u64| {
        let at = start + Duration::from_millis(ms);
        at.max(Instant::now() + Duration::from_millis(10))
    };
    // Oturum çevrimdışı başlar: ilk recv denemesi bağlanmadan yapılmaz (F06).
    let mut down = true;
    loop {
        if geri.lock().wants_connect(now()) {
            match tokio::time::timeout(cfg.connect_timeout, transport.connect()).await {
                Ok(Ok(())) => {
                    geri.lock().handle_connected(now(), transport.pushes());
                    down = false;
                }
                Ok(Err(e)) => {
                    transport.disconnect();
                    mark_broken(
                        geri,
                        events,
                        overflow,
                        &mut down,
                        now(),
                        format!("connect: {e}"),
                        true,
                    );
                }
                Err(_) => {
                    transport.disconnect();
                    mark_broken(
                        geri,
                        events,
                        overflow,
                        &mut down,
                        now(),
                        "connect: timed out".into(),
                        true,
                    );
                }
            }
        }
        geri.lock().handle_timeout(now());
        if !down && !geri.lock().is_connected() {
            transport.disconnect();
        }
        down = !geri.lock().is_connected();

        overflow.flush(events);
        geri.lock()
            .set_claim_hold(events.capacity() == 0 || !overflow.is_empty());
        loop {
            let frame = geri.lock().poll_transmit(now());
            let Some(frame) = frame else { break };
            // Oturum deadline'ı send'de takılı kalınca da uygulanır (F07): hangisi
            // önce dolarsa bağlantı kopuk sayılır, sıradaki turda yeni bağlantı kurulur.
            let session_deadline = geri.lock().poll_timeout().map(at);
            tokio::select! {
                biased;
                out = tokio::time::timeout(cfg.io_timeout, transport.send(frame)) => match out {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => { transport.disconnect(); mark_broken(geri, events, overflow, &mut down, now(), format!("send: {e}"), false); break; }
                    Err(_) => { transport.disconnect(); mark_broken(geri, events, overflow, &mut down, now(), "send: timed out".into(), false); break; }
                },
                () = sleep(session_deadline) => { transport.disconnect(); mark_broken(geri, events, overflow, &mut down, now(), "send: session deadline".into(), false); break; }
            }
        }

        overflow.flush(events);
        loop {
            // Taşma deposu doluysa oturumdaki olaylara dokunma: kayıpsız bekle (F08).
            if overflow.queued.len() >= OVERFLOW_CAP {
                break;
            }
            let event = geri.lock().poll_event();
            let Some(event) = event else { break };
            match event {
                Event::UploadBlobs(blobs) => {
                    let rest = blobs;
                    let mut done = 0;
                    let mut uploaded = true;
                    while done < rest.len() {
                        let bytes = geri.lock().store_mut().blob(&rest[done].sha256);
                        let up = async {
                            match bytes {
                                Ok(Some(bytes)) => transport
                                    .upload_blob(&rest[done].sha256, bytes)
                                    .await
                                    .map_err(|e| e.to_string()),
                                Ok(None) => {
                                    Err(format!("blob {} is not in the outbox", rest[done].sha256))
                                }
                                Err(e) => Err(e.to_string()),
                            }
                        };
                        let upload_deadline = geri.lock().request_deadline().map(at);
                        let outcome = tokio::select! {
                            result = tokio::time::timeout(cfg.upload_timeout, up) => {
                                result.unwrap_or_else(|_| Err("upload: timed out".into()))
                            },
                            () = sleep(upload_deadline) => Err("upload: session deadline".into()),
                        };
                        let failed = match outcome {
                            Ok(()) => {
                                done += 1;
                                None
                            }
                            Err(e) => Some(e),
                        };
                        if let Some(e) = failed {
                            push(events, overflow, Event::Error(Error::Transport(e)));
                            // Kalıcı outbox batch'i yeniden sorar; eksik blob'lar
                            // sonraki missing_blob cevabında tekrar yüklenir.
                            uploaded = false;
                            break;
                        }
                    }
                    if uploaded {
                        geri.lock().blobs_uploaded(now());
                    }
                }
                other => {
                    push(events, overflow, other);
                }
            }
        }
        // Taşma varken yeni claim üretme: kuyruk boşalınca fren kalkar (F08).
        geri.lock()
            .set_claim_hold(events.capacity() == 0 || !overflow.is_empty());

        let deadline = geri.lock().poll_timeout().map(at);
        let need_capacity = events.capacity() == 0 || !overflow.is_empty();
        if down {
            // Kopukken recv deneme: anında hata döngüsü yok, backoff kapısında park (F06).
            tokio::select! {
                () = geri.wake.notified() => {}
                () = sleep(deadline) => {}
                permit = events.reserve(), if need_capacity => { drop(permit); }
            }
            continue;
        }
        tokio::select! {
            frame = transport.recv() => match frame {
                Ok(frame) => geri.lock().handle_frame(now(), frame),
                Err(e) => {
                    transport.disconnect();
                    mark_broken(geri, events, overflow, &mut down, now(), format!("recv: {e}"), false);
                },
            },
            () = geri.wake.notified() => {}
            () = sleep(deadline) => {}
            permit = events.reserve(), if need_capacity => { drop(permit); }
        }
    }
}

async fn sleep(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests;
