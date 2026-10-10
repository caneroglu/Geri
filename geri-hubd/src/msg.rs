use crate::api::{App, Shared, auth, blocking, can_read, error, failure, no_store, now_ms};
use axum::{
    body::Bytes,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use geri_binding_http::{Codec, Method, hub};
use geri_core::v4::AgentPolicy;
use geri_proto::v4::{
    ApiError, Limits, MAX_BATCH_RECORDS, MAX_INLINE_BYTES, VERSION, Welcome,
    msg::{Frame, ToHub, ToSource},
};
use serde_json::{Map, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

pub(crate) const LIVE_PACE_MS: u32 = 1_000;
pub(crate) const MAX_BODY: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum Signal {
    Ring(String),
    Pace(String, u32),
}

type LiveValues = broadcast::Receiver<Arc<Map<String, Value>>>;

struct Topic {
    viewers: usize,
    values: broadcast::Sender<Arc<Map<String, Value>>>,
}

pub(crate) struct Signals {
    tx: broadcast::Sender<Signal>,
    topics: Mutex<HashMap<String, Topic>>,
    sockets: Mutex<HashMap<String, usize>>,
    connections: Mutex<HashMap<String, (usize, u64)>>,
    max_viewers: usize,
    max_sockets: usize,
}

pub(crate) const MAX_VIEWERS: usize = 32;
pub(crate) const MAX_SOCKETS_PER_AGENT: usize = 4;

impl Default for Signals {
    fn default() -> Self {
        Self::new(MAX_VIEWERS, MAX_SOCKETS_PER_AGENT)
    }
}

impl Signals {
    pub(crate) fn new(max_viewers: usize, max_sockets: usize) -> Self {
        Self {
            tx: broadcast::channel(256).0,
            topics: Mutex::default(),
            sockets: Mutex::default(),
            connections: Mutex::default(),
            max_viewers,
            max_sockets,
        }
    }

    pub(crate) fn ring(&self, source: &str) {
        let _ = self.tx.send(Signal::Ring(source.into()));
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Signal> {
        self.tx.subscribe()
    }

    pub(crate) fn connected(&self, source: &str) -> Option<u64> {
        self.connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(source)
            .map(|(_, at)| now_ms().saturating_sub(*at) / 1000)
    }

    fn topics(&self) -> std::sync::MutexGuard<'_, HashMap<String, Topic>> {
        self.topics.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn watched(&self, source: &str) -> bool {
        self.topics().get(source).is_some_and(|t| t.viewers > 0)
    }

    fn publish(&self, source: &str, values: Map<String, Value>) -> bool {
        let topics = self.topics();
        match topics.get(source) {
            Some(topic) if topic.viewers > 0 => {
                let _ = topic.values.send(Arc::new(values));
                true
            }
            _ => false,
        }
    }
}

pub(crate) struct Viewer {
    app: Shared,
    source: String,
}

impl Viewer {
    pub(crate) fn join(app: Shared, source: String) -> Option<(Self, LiveValues)> {
        let (rx, first) = {
            let mut topics = app.signals.topics();
            let topic = topics.entry(source.clone()).or_insert_with(|| Topic {
                viewers: 0,
                values: broadcast::channel(16).0,
            });
            if topic.viewers >= app.signals.max_viewers {
                return None;
            }
            topic.viewers += 1;
            (topic.values.subscribe(), topic.viewers == 1)
        };
        if first {
            let _ = app
                .signals
                .tx
                .send(Signal::Pace(source.clone(), LIVE_PACE_MS));
        }
        Some((Self { app, source }, rx))
    }
}

pub(crate) struct Socket {
    app: Shared,
    agent: String,
    sources: Vec<String>,
}

impl Socket {
    pub(crate) fn open(app: Shared, agent: String) -> Option<Self> {
        {
            let mut sockets = app
                .signals
                .sockets
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let open = sockets.entry(agent.clone()).or_default();
            if *open >= app.signals.max_sockets {
                return None;
            }
            *open += 1;
        }
        let sources: Vec<_> = app
            .agent(&agent)
            .map(|p| p.writable_sources.into_iter().collect())
            .unwrap_or_default();
        {
            let mut connections = app
                .signals
                .connections
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for source in &sources {
                let entry = connections.entry(source.clone()).or_insert((0, now_ms()));
                entry.0 += 1;
            }
        }
        for source in &sources {
            app.hud.notify(source);
        }
        Some(Self {
            app,
            agent,
            sources,
        })
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        let mut disconnected = Vec::new();
        {
            let mut connections = self
                .app
                .signals
                .connections
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for source in &self.sources {
                if let Some((count, _)) = connections.get_mut(source) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        connections.remove(source);
                        disconnected.push(source.clone());
                    }
                }
            }
        }
        for source in disconnected {
            self.app.hud.disconnected(&source);
        }

        let mut sockets = self
            .app
            .signals
            .sockets
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(open) = sockets.get_mut(&self.agent) {
            *open = open.saturating_sub(1);
            if *open == 0 {
                sockets.remove(&self.agent);
            }
        }
    }
}

fn busy(what: &str) -> Response {
    let mut response = error(StatusCode::SERVICE_UNAVAILABLE, "busy", what);
    response
        .headers_mut()
        .insert("retry-after", axum::http::HeaderValue::from_static("30"));
    response
}

impl Drop for Viewer {
    fn drop(&mut self) {
        let last = {
            let mut topics = self.app.signals.topics();
            match topics.get_mut(&self.source) {
                Some(topic) => {
                    topic.viewers = topic.viewers.saturating_sub(1);
                    topic.viewers == 0
                }
                None => false,
            }
        };
        if last {
            let _ = self
                .app
                .signals
                .tx
                .send(Signal::Pace(self.source.clone(), 0));
        }
    }
}

pub(crate) fn reject(status: StatusCode, code: &str, message: &str) -> ToSource {
    ToSource::Error {
        status: status.as_u16(),
        error: ApiError {
            code: code.into(),
            message: message.into(),
            earliest_cursor: None,
            counter: None,
        },
    }
}

fn fail(e: crate::store::StoreError) -> ToSource {
    let (status, error) = failure(e);
    ToSource::Error {
        status: status.as_u16(),
        error,
    }
}

pub(crate) fn handle(app: &App, p: &AgentPolicy, msg: ToHub) -> ToSource {
    let forbidden = || reject(StatusCode::FORBIDDEN, "forbidden", "source write denied");
    match msg {
        ToHub::Hello { .. } => ToSource::Welcome(Welcome {
            v: VERSION,
            agent_id: p.agent_id.clone(),
            limits: Limits {
                max_batch_records: MAX_BATCH_RECORDS,
                max_inline_bytes: MAX_INLINE_BYTES,
                max_body_bytes: MAX_BODY,
                max_blob_bytes: 16 * 1024 * 1024,
                max_webhook_bytes: 16 * 1024 * 1024,
            },
        }),
        ToHub::Declare(decl) => match app.store.upsert_source(p, &decl, now_ms()) {
            Ok(next) => {
                app.hud.reload(app, &decl.source_id);
                ToSource::Ok {
                    watched: None,
                    next_sequence: Some(next),
                }
            }
            Err(e) => fail(e),
        },
        ToHub::Batch(batch) => match app.store.ingest(p, &batch, now_ms()) {
            Ok(receipt) => {
                let sources: std::collections::BTreeSet<_> = receipt
                    .records
                    .iter()
                    .filter(|r| r.status == geri_proto::v4::ReceiptStatus::Accepted)
                    .map(|r| r.source_id.as_str())
                    .collect();
                for source in sources {
                    app.hud.reload(app, source);
                }
                ToSource::Receipt(receipt)
            }
            Err(e) => fail(e),
        },
        ToHub::Live { source_id, values } => {
            if !p.can_write(&source_id) {
                return forbidden();
            }
            match app.store.visibility(&source_id) {
                Ok(Some(_)) => {
                    if app.signals.watched(&source_id) {
                        app.hud.live(&source_id, &values);
                    }
                    ToSource::Ok {
                        watched: Some(app.signals.publish(&source_id, values)),
                        next_sequence: None,
                    }
                }
                Ok(None) => reject(StatusCode::NOT_FOUND, "not_found", "source not declared"),
                Err(e) => fail(e),
            }
        }
        ToHub::Claim { source_id } => {
            if !p.can_write(&source_id) {
                return forbidden();
            }
            match app.store.claim_command(&source_id, now_ms()) {
                Ok(Some(command)) => {
                    app.hud.reload(app, &source_id);
                    ToSource::Command(command)
                }
                Ok(None) => ToSource::Idle,
                Err(e) => fail(e),
            }
        }
        ToHub::Result { source_id, result } => {
            if !p.can_write(&source_id) {
                return forbidden();
            }
            match app.store.finish_command(&source_id, &result) {
                Ok(true) => {
                    app.hud.reload(app, &source_id);
                    ToSource::Ok {
                        watched: None,
                        next_sequence: None,
                    }
                }
                Ok(false) => reject(
                    StatusCode::CONFLICT,
                    "unknown_command",
                    "command not dispatched or result differs",
                ),
                Err(e) => fail(e),
            }
        }
        ToHub::Ping => ToSource::Pong,
        ToHub::Unknown => reject(
            StatusCode::BAD_REQUEST,
            "unknown_message",
            "message type not supported",
        ),
    }
}

pub(crate) async fn http(
    State(app): State<Shared>,
    method: axum::http::Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let p = match auth(&headers, &app) {
        Ok(p) => p,
        Err(e) => return e,
    };
    if body.len() > MAX_BODY {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "too_large",
            "body exceeds 1 MiB",
        );
    }
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let Some(method) = Method::parse(method.as_str()) else {
        return error(
            StatusCode::METHOD_NOT_ALLOWED,
            "not_found",
            "method not allowed",
        );
    };
    let path = uri.path_and_query().map_or(uri.path(), |p| p.as_str());
    let msg = match hub::request(method, path, header("content-type"), &body) {
        Ok(msg) => msg,
        Err(geri_binding_http::Error::Decode(e)) => {
            return error(StatusCode::BAD_REQUEST, "malformed", &e);
        }
        Err(geri_binding_http::Error::InvalidId) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid",
                "missing or invalid identifier",
            );
        }
        Err(geri_binding_http::Error::PathMismatch) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid",
                "source path and body differ",
            );
        }
        Err(_) => return error(StatusCode::NOT_FOUND, "not_found", "no such endpoint"),
    };
    let codec = Codec::from_content_type(header("accept"));
    let (answer, signals) =
        blocking(&app, move |app| (handle(app, &p, msg), piggyback(app, &p))).await;
    match hub::response(&answer, codec) {
        Ok(res) => {
            let status = StatusCode::from_u16(res.status).unwrap_or(StatusCode::OK);
            let mut response = if res.body.is_empty() {
                status.into_response()
            } else {
                (status, [("content-type", codec.content_type())], res.body).into_response()
            };
            if let Some(signals) = signals {
                response
                    .headers_mut()
                    .insert(geri_binding_http::SIGNALS, signals);
            }
            no_store(response)
        }
        Err(_) => error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "encode",
            "response encoding failed",
        ),
    }
}

pub(crate) async fn wait(State(app): State<Shared>, uri: Uri, headers: HeaderMap) -> Response {
    let p = match auth(&headers, &app) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let path = uri.path_and_query().map_or(uri.path(), |p| p.as_str());
    let ms = hub::wait(Method::Get, path).unwrap_or(geri_binding_http::MAX_WAIT_MS);
    let mut signals = app.signals.subscribe();
    let check = |p: &AgentPolicy| {
        let p = p.clone();
        blocking(&app, move |app| piggyback(app, &p))
    };
    let mut ready = check(&p).await;
    if ready.is_none() {
        let deadline = tokio::time::sleep(std::time::Duration::from_millis(u64::from(ms)));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                () = &mut deadline => break,
                signal = signals.recv() => match signal {
                    Ok(Signal::Ring(source)) if p.can_write(&source) => break,
                    Ok(_) => {}
                    Err(_) => break,
                },
            }
        }
        ready = check(&p).await;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    if let Some(value) = ready {
        response
            .headers_mut()
            .insert(geri_binding_http::SIGNALS, value);
    }
    no_store(response)
}

fn piggyback(app: &App, p: &AgentPolicy) -> Option<HeaderValue> {
    let rings: Vec<ToSource> = app
        .store
        .queued_among(&p.writable_sources)
        .ok()?
        .into_iter()
        .map(|source_id| ToSource::Ring { source_id })
        .collect();
    HeaderValue::from_str(&hub::signals(&rings)?).ok()
}

pub(crate) async fn live_stream(
    State(app): State<Shared>,
    Path(source): Path<String>,
    headers: HeaderMap,
) -> Response {
    let p = match auth(&headers, &app) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let (p2, id) = (p.clone(), source.clone());
    if !blocking(&app, move |app| can_read(app, &p2, &id)).await {
        return error(StatusCode::FORBIDDEN, "forbidden", "source read denied");
    }
    let Some((viewer, rx)) = Viewer::join(app, source) else {
        return busy("too many viewers for this source");
    };
    let stream = futures_util::stream::unfold((viewer, rx), |(viewer, mut rx)| async move {
        loop {
            match rx.recv().await {
                Ok(values) => {
                    let event = Event::default()
                        .event("live")
                        .data(serde_json::to_string(&*values).unwrap_or_default());
                    return Some((Ok::<_, std::convert::Infallible>(event), (viewer, rx)));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    no_store(
        Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response(),
    )
}

pub(crate) async fn ws(
    State(app): State<Shared>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let p = match auth(&headers, &app) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let Some(slot) = Socket::open(app.clone(), p.agent_id.clone()) else {
        return busy("too many websockets for this agent");
    };
    upgrade
        .max_message_size(MAX_BODY)
        .on_upgrade(move |socket| connection(app, p, socket, slot))
}

pub(crate) fn on_frame(app: &App, p: &AgentPolicy, bytes: &[u8]) -> Vec<Frame<ToSource>> {
    let frame = match geri_binding_stream::decode::<ToHub>(bytes) {
        Ok(frame) => frame,
        Err(e) => {
            let msg = reject(StatusCode::BAD_REQUEST, "malformed", &e.to_string());
            return vec![Frame { id: None, msg }];
        }
    };
    let hello = matches!(frame.msg, ToHub::Hello { .. });
    let mut out = vec![Frame {
        id: frame.id,
        msg: handle(app, p, frame.msg),
    }];
    if hello {
        out.extend(watched(app, p));
    }
    out
}

pub(crate) fn watched(app: &App, p: &AgentPolicy) -> Vec<Frame<ToSource>> {
    p.writable_sources
        .iter()
        .filter(|source| app.signals.watched(source))
        .map(|source| Frame {
            id: None,
            msg: ToSource::Pace {
                source_id: source.clone(),
                ms: LIVE_PACE_MS,
            },
        })
        .collect()
}

pub(crate) fn on_signal(
    p: &AgentPolicy,
    signal: Result<Signal, broadcast::error::RecvError>,
) -> Option<Vec<Frame<ToSource>>> {
    let msgs = match signal {
        Ok(Signal::Ring(source)) if p.can_write(&source) => {
            vec![ToSource::Ring { source_id: source }]
        }
        Ok(Signal::Pace(source, ms)) if p.can_write(&source) => {
            vec![ToSource::Pace {
                source_id: source,
                ms,
            }]
        }
        Ok(_) => vec![],
        Err(broadcast::error::RecvError::Lagged(_)) => p
            .writable_sources
            .iter()
            .map(|s| ToSource::Ring {
                source_id: s.clone(),
            })
            .collect(),
        Err(broadcast::error::RecvError::Closed) => return None,
    };
    Some(
        msgs.into_iter()
            .map(|msg| Frame { id: None, msg })
            .collect(),
    )
}

async fn connection(app: Shared, p: AgentPolicy, mut socket: WebSocket, _slot: Socket) {
    let mut signals = app.signals.subscribe();
    let mut codec = geri_binding_stream::Codec::Json;
    loop {
        let replies = tokio::select! {
            incoming = socket.recv() => {
                let bytes = match incoming {
                    Some(Ok(Message::Text(text))) => {
                        codec = geri_binding_stream::Codec::Json;
                        Bytes::from(text)
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        codec = geri_binding_stream::Codec::Cbor;
                        bytes
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    _ => break,
                };
                let p = p.clone();
                blocking(&app, move |app| on_frame(app, &p, &bytes)).await
            }
            signal = signals.recv() => match on_signal(&p, signal) {
                Some(replies) => replies,
                None => break,
            },
        };
        for reply in &replies {
            match tokio::time::timeout(SEND_TIMEOUT, send(&mut socket, reply, codec)).await {
                Ok(Ok(())) => {}
                _ => return,
            }
        }
    }
}

pub(crate) const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn send(
    socket: &mut WebSocket,
    frame: &Frame<ToSource>,
    codec: geri_binding_stream::Codec,
) -> Result<(), axum::Error> {
    let bytes = geri_binding_stream::encode(frame, codec).map_err(axum::Error::new)?;
    let message = match codec {
        geri_binding_stream::Codec::Json => {
            Message::Text(String::from_utf8(bytes).map_err(axum::Error::new)?.into())
        }
        geri_binding_stream::Codec::Cbor => Message::Binary(bytes.into()),
    };
    socket.send(message).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{router, test_app, test_app_with};
    use axum::{body::Body, http::Request};
    use futures_util::{SinkExt, StreamExt};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tempfile::tempdir;
    use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};
    use tower::ServiceExt;

    const AGENT: &str = "Bearer router-token-123456";
    const OPERATOR: &str = "Bearer operator-token-123456";

    fn declaration() -> geri_proto::v4::SourceDeclaration {
        geri_proto::v4::SourceDeclaration {
            source_id: "router".into(),
            label: "Router".into(),
            kind: "openwrt".into(),
            entities: vec![geri_proto::v4::Entity {
                group: None,
                source_id: "router".into(),
                entity_id: "wan".into(),
                label: "WAN".into(),
                kind: "switch".into(),
                control: Some(geri_proto::Control {
                    shape: geri_proto::Shape::Trigger,
                    danger: Default::default(),
                }),
            }],
        }
    }

    fn request(method: &str, uri: &str, auth: &str, cbor: bool, body: Vec<u8>) -> Request<Body> {
        let ct = if cbor {
            "application/cbor"
        } else {
            "application/json"
        };
        Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", auth)
            .header("content-type", ct)
            .header("accept", ct)
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn every_message_endpoint_speaks_cbor_too() {
        let dir = tempdir().unwrap();
        let app = router(test_app(dir.path()));
        let decl = geri_proto::cbor::to_vec(&declaration()).unwrap();
        let res = app
            .clone()
            .oneshot(request("PUT", "/v4/sources/router", AGENT, true, decl))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let reply: serde_json::Value = geri_proto::cbor::from_slice(&body).unwrap();
        assert_eq!(reply, json!({"next_sequence": 1}));
        let live = geri_proto::cbor::to_vec(&json!({"cpu": 3})).unwrap();
        let res = app
            .clone()
            .oneshot(request("PUT", "/v4/live/router", AGENT, true, live))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()["content-type"], "application/cbor");
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let reply: Value = geri_proto::cbor::from_slice(&body).unwrap();
        assert_eq!(reply, json!({"watched": false}));
        let res = app
            .clone()
            .oneshot(request(
                "GET",
                "/v4/commands/claim?source=router",
                AGENT,
                true,
                vec![],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn http_answers_carry_a_ring_while_a_command_waits() {
        let dir = tempdir().unwrap();
        let app = router(test_app(dir.path()));
        let send = |method: &str, uri: &str, auth: &str, body: Value| {
            let body = if body.is_null() {
                vec![]
            } else {
                serde_json::to_vec(&body).unwrap()
            };
            app.clone().oneshot(request(method, uri, auth, false, body))
        };
        let signals = |res: &Response| {
            res.headers()
                .get(geri_binding_http::SIGNALS)
                .map(|v| v.to_str().unwrap().to_owned())
        };
        let decl = serde_json::to_value(declaration()).unwrap();
        let res = send("PUT", "/v4/sources/router", AGENT, decl)
            .await
            .unwrap();
        assert_eq!(signals(&res), None);
        let issue =
            json!({"source_id": "router", "entity_id": "wan", "action": "press", "args": null});
        let res = send("POST", "/v4/commands", OPERATOR, issue).await.unwrap();
        assert!(res.status().is_success());
        let res = send("PUT", "/v4/live/router", AGENT, json!({"cpu": 1}))
            .await
            .unwrap();
        assert_eq!(
            signals(&res).as_deref(),
            Some(r#"[{"t":"ring","source_id":"router"}]"#)
        );
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"watched": false})
        );
        let claim = "/v4/commands/claim?source=router";
        let res = send("GET", claim, AGENT, Value::Null).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(signals(&res), None, "komut alındı, zil biter");
        let res = send("GET", claim, AGENT, Value::Null).await.unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(signals(&res), None);
    }

    #[tokio::test]
    async fn a_long_poll_wakes_on_a_command_and_times_out_empty() {
        let dir = tempdir().unwrap();
        let app = router(test_app(dir.path()));
        let wait = |ms: u32| {
            app.clone().oneshot(request(
                "GET",
                &format!("/v4/signals?wait={ms}"),
                AGENT,
                false,
                vec![],
            ))
        };
        let signals = |res: &Response| {
            res.headers()
                .get(geri_binding_http::SIGNALS)
                .map(|v| v.to_str().unwrap().to_owned())
        };
        let decl = serde_json::to_vec(&declaration()).unwrap();
        let res = app
            .clone()
            .oneshot(request("PUT", "/v4/sources/router", AGENT, false, decl))
            .await
            .unwrap();
        assert!(res.status().is_success());

        let started = std::time::Instant::now();
        let res = wait(200).await.unwrap();
        assert!(started.elapsed() >= std::time::Duration::from_millis(200));
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert_eq!(signals(&res), None);

        let started = std::time::Instant::now();
        let pending = tokio::spawn(wait(10_000));
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let issue =
            json!({"source_id": "router", "entity_id": "wan", "action": "press", "args": null});
        let res = app
            .clone()
            .oneshot(request(
                "POST",
                "/v4/commands",
                OPERATOR,
                false,
                serde_json::to_vec(&issue).unwrap(),
            ))
            .await
            .unwrap();
        assert!(res.status().is_success());
        let res = pending.await.unwrap().unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        let ring = Some(r#"[{"t":"ring","source_id":"router"}]"#.to_owned());
        assert_eq!(signals(&res), ring);

        let started = std::time::Instant::now();
        assert_eq!(signals(&wait(10_000).await.unwrap()), ring);
        assert!(started.elapsed() < std::time::Duration::from_secs(1));

        let res = app
            .clone()
            .oneshot(request(
                "GET",
                "/v4/signals?wait=10",
                "Bearer nope-nope-nope-nope",
                false,
                vec![],
            ))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn live_needs_a_declared_writable_source() {
        let dir = tempdir().unwrap();
        let app = router(test_app(dir.path()));
        let body = || serde_json::to_vec(&json!({"cpu": 1})).unwrap();
        let status = |res: Response| res.status();
        let res = app
            .clone()
            .oneshot(request("PUT", "/v4/live/router", AGENT, false, body()))
            .await
            .unwrap();
        assert_eq!(status(res), StatusCode::NOT_FOUND);
        let res = app
            .clone()
            .oneshot(request("PUT", "/v4/live/router", OPERATOR, false, body()))
            .await
            .unwrap();
        assert_eq!(status(res), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn viewers_receive_live_values_and_make_the_source_watched() {
        let dir = tempdir().unwrap();
        let shared = test_app(dir.path());
        let app = router(shared.clone());
        let decl = serde_json::to_vec(&declaration()).unwrap();
        app.clone()
            .oneshot(request("PUT", "/v4/sources/router", AGENT, false, decl))
            .await
            .unwrap();
        let mut signals = shared.signals.subscribe();

        let viewer = app
            .clone()
            .oneshot(request("GET", "/v4/live/router", OPERATOR, false, vec![]))
            .await
            .unwrap();
        assert_eq!(viewer.status(), StatusCode::OK);
        assert!(matches!(signals.recv().await, Ok(Signal::Pace(s, LIVE_PACE_MS)) if s == "router"));

        let res = app
            .clone()
            .oneshot(request(
                "PUT",
                "/v4/live/router",
                AGENT,
                false,
                serde_json::to_vec(&json!({"cpu": 42})).unwrap(),
            ))
            .await
            .unwrap();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], br#"{"watched":true}"#);

        let mut stream = viewer.into_body();
        let frame = stream.frame().await.unwrap().unwrap().into_data().unwrap();
        let text = String::from_utf8(frame.to_vec()).unwrap();
        assert!(
            text.contains("event: live") && text.contains(r#"{"cpu":42}"#),
            "{text}"
        );

        drop(stream);
        assert!(matches!(signals.recv().await, Ok(Signal::Pace(s, 0)) if s == "router"));
        assert!(!shared.signals.watched("router"));
    }

    type Client = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn connect(shared: Shared, auth: &str) -> Result<Client, tungstenite::Error> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router(shared)).await.unwrap() });
        let mut req = format!("ws://{addr}/v4/ws").into_client_request().unwrap();
        req.headers_mut()
            .insert("authorization", auth.parse().unwrap());
        Ok(tokio_tungstenite::connect_async(req).await?.0)
    }

    async fn call(ws: &mut Client, frame: Frame<ToHub>, cbor: bool) -> Frame<ToSource> {
        let codec = if cbor {
            geri_binding_stream::Codec::Cbor
        } else {
            geri_binding_stream::Codec::Json
        };
        let bytes = geri_binding_stream::encode(&frame, codec).unwrap();
        let msg = if cbor {
            tungstenite::Message::Binary(bytes.into())
        } else {
            tungstenite::Message::Text(String::from_utf8(bytes).unwrap().into())
        };
        ws.send(msg).await.unwrap();
        next(ws).await
    }

    async fn next(ws: &mut Client) -> Frame<ToSource> {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
            .await
            .expect("hub said nothing")
            .unwrap()
            .unwrap();
        geri_binding_stream::decode(&msg.into_data()).unwrap()
    }

    #[tokio::test]
    async fn a_source_talks_over_websocket_and_gets_pace_and_ring_pushed() {
        let dir = tempdir().unwrap();
        let shared = test_app(dir.path());
        assert!(connect(shared.clone(), "Bearer nope").await.is_err());
        let mut ws = connect(shared.clone(), AGENT).await.unwrap();

        let hello = Frame {
            id: Some(1),
            msg: ToHub::Hello {
                v: VERSION,
                auth: None,
            },
        };
        let welcome = call(&mut ws, hello, false).await;
        assert_eq!(welcome.id, Some(1));
        assert!(matches!(welcome.msg, ToSource::Welcome(w) if w.agent_id == "router"));

        let declare = Frame {
            id: Some(2),
            msg: ToHub::Declare(declaration()),
        };
        assert_eq!(
            call(&mut ws, declare, true).await.msg,
            ToSource::Ok {
                watched: None,
                next_sequence: Some(1),
            }
        );

        let record = geri_proto::v4::Record {
            source_id: "router".into(),
            entity_id: "wan".into(),
            record_id: "router-1".into(),
            sequence: 1,
            kind: geri_proto::v4::RecordKind::State,
            event_time_ms: None,
            content_type: "application/json".into(),
            data: Some(json!({"up": true, "note": null})),
            blob: None,
            indexed: None,
        };
        let batch = Frame {
            id: Some(3),
            msg: ToHub::Batch(geri_proto::v4::Batch {
                v: VERSION,
                records: vec![record],
            }),
        };
        let receipt = call(&mut ws, batch, true).await;
        assert!(matches!(&receipt.msg, ToSource::Receipt(r) if r.records[0].cursor.is_some()));

        let (viewer, _values) = Viewer::join(shared.clone(), "router".into()).unwrap();
        let pace = next(&mut ws).await;
        assert_eq!(pace.id, None);
        assert_eq!(
            pace.msg,
            ToSource::Pace {
                source_id: "router".into(),
                ms: LIVE_PACE_MS
            }
        );
        drop(viewer);
        assert_eq!(
            next(&mut ws).await.msg,
            ToSource::Pace {
                source_id: "router".into(),
                ms: 0
            }
        );

        let issue = request(
            "POST",
            "/v4/commands",
            OPERATOR,
            false,
            serde_json::to_vec(&json!({"source_id": "router", "entity_id": "wan", "action": "press", "args": null})).unwrap(),
        );
        assert!(
            router(shared.clone())
                .oneshot(issue)
                .await
                .unwrap()
                .status()
                .is_success()
        );
        assert_eq!(
            next(&mut ws).await.msg,
            ToSource::Ring {
                source_id: "router".into()
            }
        );
        let claim = Frame {
            id: Some(4),
            msg: ToHub::Claim {
                source_id: "router".into(),
            },
        };
        let command = call(&mut ws, claim, false).await;
        let ToSource::Command(command) = command.msg else {
            panic!("{command:?}")
        };
        let result = Frame {
            id: Some(5),
            msg: ToHub::Result {
                source_id: "router".into(),
                result: geri_proto::v4::CommandResult {
                    command_id: command.command_id,
                    ok: true,
                    result: Value::Null,
                },
            },
        };
        assert_eq!(
            call(&mut ws, result, false).await.msg,
            ToSource::Ok {
                watched: None,
                next_sequence: None
            }
        );

        let ping = Frame {
            id: Some(6),
            msg: ToHub::Ping,
        };
        assert_eq!(call(&mut ws, ping, false).await.msg, ToSource::Pong);
        ws.send(tungstenite::Message::Text("not json".into()))
            .await
            .unwrap();
        let bad = next(&mut ws).await;
        assert!(matches!(bad.msg, ToSource::Error { status: 400, .. }));
    }

    #[tokio::test]
    async fn a_signed_upgrade_opens_the_socket_once() {
        let dir = tempdir().unwrap();
        let shared = test_app(dir.path());
        let signer = geri_sign::Signer::new("router", crate::api::TEST_SEED).unwrap();
        let auth = signer.authorization("GET", "/v4/ws", b"");
        let mut ws = connect(shared.clone(), &auth).await.unwrap();
        let hello = Frame {
            id: Some(1),
            msg: ToHub::Hello {
                v: VERSION,
                auth: None,
            },
        };
        let welcome = call(&mut ws, hello, false).await;
        assert!(matches!(welcome.msg, ToSource::Welcome(w) if w.agent_id == "router"));
        match connect(shared, &auth).await {
            Err(tungstenite::Error::Http(r)) => {
                assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
                assert_eq!(r.headers()[geri_sign::COUNTER_HEADER], "1");
            }
            other => panic!("replay accepted: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_source_that_connects_while_watched_is_told_at_once() {
        let dir = tempdir().unwrap();
        let shared = test_app(dir.path());
        let (_viewer, _values) = Viewer::join(shared.clone(), "router".into()).unwrap();
        let mut ws = connect(shared, AGENT).await.unwrap();
        let hello = Frame {
            id: Some(1),
            msg: ToHub::Hello {
                v: VERSION,
                auth: None,
            },
        };
        assert!(matches!(
            call(&mut ws, hello, false).await.msg,
            ToSource::Welcome(_)
        ));
        assert_eq!(
            next(&mut ws).await.msg,
            ToSource::Pace {
                source_id: "router".into(),
                ms: LIVE_PACE_MS
            }
        );
    }

    #[tokio::test]
    async fn viewers_per_source_are_limited() {
        let dir = tempdir().unwrap();
        let shared = test_app_with(dir.path(), Signals::new(2, 4));
        let app = router(shared.clone());
        let decl = serde_json::to_vec(&declaration()).unwrap();
        app.clone()
            .oneshot(request("PUT", "/v4/sources/router", AGENT, false, decl))
            .await
            .unwrap();
        let view = || request("GET", "/v4/live/router", OPERATOR, false, vec![]);
        let first = app.clone().oneshot(view()).await.unwrap();
        let second = app.clone().oneshot(view()).await.unwrap();
        assert!(first.status().is_success() && second.status().is_success());
        let third = app.clone().oneshot(view()).await.unwrap();
        assert_eq!(third.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(third.headers()["retry-after"], "30");
        let body = third.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains(r#""code":"busy""#));
        drop(first);
        assert!(
            app.clone()
                .oneshot(view())
                .await
                .unwrap()
                .status()
                .is_success()
        );
    }

    #[tokio::test]
    async fn websockets_per_agent_are_limited() {
        let dir = tempdir().unwrap();
        let shared = test_app_with(dir.path(), Signals::new(32, 2));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router(shared)).await.unwrap() });
        let open = |auth: &'static str| async move {
            let mut req = format!("ws://{addr}/v4/ws").into_client_request().unwrap();
            req.headers_mut()
                .insert("authorization", auth.parse().unwrap());
            tokio_tungstenite::connect_async(req)
                .await
                .map(|(ws, _)| ws)
        };
        let a = open(AGENT).await.unwrap();
        let _b = open(AGENT).await.unwrap();
        match open(AGENT).await {
            Err(tungstenite::Error::Http(res)) => {
                assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
            }
            other => panic!("third socket should be refused: {other:?}"),
        }
        assert!(open(OPERATOR).await.is_ok());
        drop(a);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(open(AGENT).await.is_ok());
    }
}
