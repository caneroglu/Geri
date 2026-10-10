use crate::msg::{self, Signals};
use crate::store::{Store, StoreError};
use crate::{quic, tcp};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{get, post, put},
};
use futures_util::stream;
use geri_core::v4::{AgentPolicy, Role};
use geri_proto::v4::{self, ApiError, CommandRequest, Visibility};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;

#[derive(Clone, Deserialize)]
pub(crate) struct Credential {
    id: String,
    #[serde(default)]
    token: String,
    #[serde(default, deserialize_with = "public_key")]
    public_key: Option<[u8; 32]>,
    #[serde(default)]
    sources: Vec<String>,
    #[serde(default)]
    role: String,
}

fn public_key<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<[u8; 32]>, D::Error> {
    match Option::<String>::deserialize(d)? {
        None => Ok(None),
        Some(hex) => geri_sign::parse_key(&hex)
            .map(Some)
            .ok_or_else(|| serde::de::Error::custom("public_key must be 64 hex characters")),
    }
}
pub(crate) struct App {
    pub(crate) hud: crate::ui::Hud,
    pub(crate) store: Store,
    credentials: Vec<Credential>,
    signing_seed: Option<[u8; 32]>,
    backup_root: Option<PathBuf>,
    pub(crate) signals: Signals,
}
pub(crate) type Shared = Arc<App>;

pub(crate) async fn blocking<T: Send + 'static>(
    app: &Shared,
    f: impl FnOnce(&App) -> T + Send + 'static,
) -> T {
    let app = app.clone();
    match tokio::task::spawn_blocking(move || f(&app)).await {
        Ok(value) => value,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}
const MAX_BODY: usize = 16 * 1024 * 1024;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub(crate) fn error(status: StatusCode, code: &str, message: &str) -> Response {
    no_store(
        (
            status,
            axum::Json(ApiError {
                code: code.into(),
                message: message.into(),
                earliest_cursor: None,
                counter: None,
            }),
        )
            .into_response(),
    )
}
pub(crate) fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    response
}
const VERIFIED: &str = "x-geri-verified";

pub(crate) fn auth(headers: &HeaderMap, app: &App) -> Result<AgentPolicy, Response> {
    let unauthorized = || {
        error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid bearer token",
        )
    };
    if let Some(agent) = headers.get(VERIFIED).and_then(|h| h.to_str().ok()) {
        return app.agent(agent).ok_or_else(unauthorized);
    }
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    app.by_token(token).ok_or_else(unauthorized)
}

#[derive(Debug)]
pub(crate) enum SignatureError {
    Invalid(&'static str),
    Replay(u64),
    Store(StoreError),
}

fn role(name: &str) -> Option<Role> {
    match name {
        "" | "agent" => Some(Role::Agent),
        "operator" => Some(Role::Operator),
        "developer" => Some(Role::Developer),
        _ => None,
    }
}

fn policy(cred: &Credential) -> AgentPolicy {
    AgentPolicy {
        agent_id: cred.id.clone(),
        writable_sources: cred.sources.iter().cloned().collect(),
        role: role(&cred.role).unwrap_or(Role::Agent),
    }
}

impl App {
    pub(crate) fn agent(&self, id: &str) -> Option<AgentPolicy> {
        self.credentials.iter().find(|c| c.id == id).map(policy)
    }

    pub(crate) fn by_token(&self, token: &str) -> Option<AgentPolicy> {
        if token.is_empty() {
            return None;
        }
        let received = Sha256::digest(token.as_bytes());
        self.credentials
            .iter()
            .find(|c| bool::from(Sha256::digest(c.token.as_bytes()).ct_eq(&received)))
            .map(policy)
    }

    pub(crate) fn by_noise_key(&self, remote: &[u8; 32]) -> Option<AgentPolicy> {
        self.credentials
            .iter()
            .find(|c| {
                c.public_key
                    .is_some_and(|k| geri_noise::matches(remote, &k))
            })
            .map(policy)
    }

    pub(crate) fn signing_seed(&self) -> Option<&[u8; 32]> {
        self.signing_seed.as_ref()
    }

    pub(crate) fn signer_known(&self, value: &str) -> Result<(), SignatureError> {
        let signature = geri_sign::Authorization::parse(value)
            .ok_or(SignatureError::Invalid("malformed signature header"))?;
        self.credentials
            .iter()
            .find(|c| c.id == signature.agent)
            .and_then(|c| c.public_key)
            .map(|_| ())
            .ok_or(SignatureError::Invalid("no public key for this agent"))
    }

    pub(crate) fn check_signature(
        &self,
        value: &str,
        method: &str,
        target: &str,
        body: &[u8],
    ) -> Result<String, SignatureError> {
        let signature = geri_sign::Authorization::parse(value)
            .ok_or(SignatureError::Invalid("malformed signature header"))?;
        let key = self
            .credentials
            .iter()
            .find(|c| c.id == signature.agent)
            .and_then(|c| c.public_key)
            .ok_or(SignatureError::Invalid("no public key for this agent"))?;
        if !signature.verify(&key, method, target, body) {
            return Err(SignatureError::Invalid("invalid signature"));
        }
        match self.store.accept_counter(signature.agent, signature.n) {
            Ok(Ok(())) => Ok(signature.agent.to_owned()),
            Ok(Err(max)) => Err(SignatureError::Replay(max)),
            Err(e) => Err(SignatureError::Store(e)),
        }
    }
}

pub(crate) fn replay_error(max: u64) -> ApiError {
    ApiError {
        code: "replay".into(),
        message: "counter already used; continue after Geri-Counter".into(),
        earliest_cursor: None,
        counter: Some(max),
    }
}

async fn signed(State(app): State<Shared>, request: Request, next: Next) -> Response {
    let (mut parts, body) = request.into_parts();
    parts.headers.remove(VERIFIED);
    let Some(value) = parts
        .headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .filter(|v| v.starts_with(geri_sign::SCHEME))
    else {
        return next.run(Request::from_parts(parts, body)).await;
    };
    if let Err(SignatureError::Invalid(message)) = app.signer_known(value) {
        return error(StatusCode::UNAUTHORIZED, "unauthorized", message);
    }
    let value = value.to_owned();
    let body = match axum::body::to_bytes(body, MAX_BODY).await {
        Ok(body) => body,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "too_large", "body too large"),
    };
    let target = parts
        .uri
        .path_and_query()
        .map_or("/", |p| p.as_str())
        .to_owned();
    let method = parts.method.as_str().to_owned();
    let checked = body.clone();
    let agent = match blocking(&app, move |app| {
        app.check_signature(&value, &method, &target, &checked)
    })
    .await
    {
        Ok(agent) => agent,
        Err(SignatureError::Invalid(message)) => {
            return error(StatusCode::UNAUTHORIZED, "unauthorized", message);
        }
        Err(SignatureError::Replay(max)) => {
            let mut response =
                no_store((StatusCode::UNAUTHORIZED, axum::Json(replay_error(max))).into_response());
            response
                .headers_mut()
                .insert(geri_sign::COUNTER_HEADER, HeaderValue::from(max));
            return response;
        }
        Err(SignatureError::Store(e)) => return db_error(e),
    };
    if let Ok(agent) = HeaderValue::from_str(&agent) {
        parts.headers.insert(VERIFIED, agent);
    }
    next.run(Request::from_parts(parts, Body::from(body))).await
}

pub(crate) fn failure(e: StoreError) -> (StatusCode, ApiError) {
    let api = |code: &str, message: &str| ApiError {
        code: code.into(),
        message: message.into(),
        earliest_cursor: None,
        counter: None,
    };
    match e {
        StoreError::Invalid(r) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            api("invalid", &format!("{r:?}")),
        ),
        StoreError::Forbidden => (
            StatusCode::FORBIDDEN,
            api("forbidden", "source write denied"),
        ),
        StoreError::MissingBlob => (
            StatusCode::UNPROCESSABLE_ENTITY,
            api("missing_blob", "upload blob before batch"),
        ),
        StoreError::SequenceExhausted => (
            StatusCode::CONFLICT,
            api("sequence_exhausted", "source sequence is exhausted"),
        ),
        StoreError::Json(e) => (StatusCode::BAD_REQUEST, api("invalid_json", &e.to_string())),
        StoreError::Expired(earliest_cursor) => (
            StatusCode::GONE,
            ApiError {
                earliest_cursor: Some(earliest_cursor),
                counter: None,
                ..api("cursor_expired", "cursor passed retention window")
            },
        ),
        other => {
            eprintln!("storage error: {other:?}");
            (
                StatusCode::INSUFFICIENT_STORAGE,
                api("storage_failure", "durable storage unavailable"),
            )
        }
    }
}
fn db_error(e: StoreError) -> Response {
    let (status, body) = failure(e);
    no_store((status, axum::Json(body)).into_response())
}
fn encoded<T: Serialize>(headers: &HeaderMap, value: &T) -> Response {
    no_store(
        if headers
            .get("accept")
            .and_then(|x| x.to_str().ok())
            .is_some_and(|s| s.contains("application/cbor"))
        {
            match geri_proto::cbor::to_vec(value) {
                Ok(bytes) => ([("content-type", "application/cbor")], bytes).into_response(),
                Err(_) => error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "encode",
                    "CBOR encoding failed",
                ),
            }
        } else {
            axum::Json(value).into_response()
        },
    )
}
async fn sources(State(app): State<Shared>, headers: HeaderMap) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        match app.store.sources(&p) {
            Ok(v) => encoded(&headers, &v),
            Err(e) => db_error(e),
        }
    })
    .await
}
pub(crate) fn can_read(app: &App, p: &AgentPolicy, source: &str) -> bool {
    p.can_write(source)
        || app
            .store
            .visibility(source)
            .ok()
            .flatten()
            .is_some_and(|v| p.can_read(v))
}
async fn state(
    State(app): State<Shared>,
    Path(source): Path<String>,
    headers: HeaderMap,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if !can_read(app, &p, &source) {
            return error(StatusCode::FORBIDDEN, "forbidden", "source read denied");
        }
        match app.store.state(&source) {
            Ok(v) => encoded(&headers, &v),
            Err(e) => db_error(e),
        }
    })
    .await
}
#[derive(Deserialize, Default, Clone)]
struct Search {
    source: Option<String>,
    cursor: Option<u64>,
    from_ms: Option<u64>,
    to_ms: Option<u64>,
    q: Option<String>,
    limit: Option<usize>,
}
fn search(app: &App, p: &AgentPolicy, q: &Search) -> Result<v4::RecordPage, Response> {
    let source = q.source.as_deref();
    if let Some(source) = source {
        if !can_read(app, p, source) {
            return Err(error(
                StatusCode::FORBIDDEN,
                "forbidden",
                "source read denied",
            ));
        }
    } else if p.role == Role::Agent {
        return Err(error(
            StatusCode::FORBIDDEN,
            "source_required",
            "agent queries need a source",
        ));
    }
    app.store
        .records(
            source,
            q.cursor.unwrap_or(0),
            q.from_ms,
            q.to_ms,
            q.q.as_deref(),
            q.limit.unwrap_or(100),
        )
        .map_err(db_error)
}
async fn records(
    State(app): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<Search>,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        match search(app, &p, &q) {
            Ok(v) => encoded(&headers, &v),
            Err(e) => e,
        }
    })
    .await
}
#[derive(Deserialize, Default)]
struct DigestQuery {
    after_cursor: Option<u64>,
    limit: Option<usize>,
}
async fn digest(
    State(app): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<DigestQuery>,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if p.role == Role::Agent {
            return error(
                StatusCode::FORBIDDEN,
                "forbidden",
                "developer or operator required",
            );
        }
        let Some(seed) = app.signing_seed else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "signing_unconfigured",
                "GERI_SIGNING_SEED required",
            );
        };
        let after = q.after_cursor.unwrap_or(0);
        match app.store.records(
            None,
            after,
            None,
            None,
            None,
            q.limit.unwrap_or(500).clamp(1, 500),
        ) {
            Ok(page) => encoded(
                &headers,
                &geri_core::v4::sign_summary(after, &page.records, seed),
            ),
            Err(e) => db_error(e),
        }
    })
    .await
}
async fn backup(State(app): State<Shared>, headers: HeaderMap) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if !p.can_command() {
            return error(StatusCode::FORBIDDEN, "forbidden", "operator required");
        }
        let Some(root) = &app.backup_root else {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "backup_unconfigured",
                "GERI_BACKUP_DIR required",
            );
        };
        let path = root.join(format!("backup-{}", now_ms()));
        match app.store.backup(&path) {
            Ok(()) => encoded(&headers, &serde_json::json!({"path":path})),
            Err(e) => db_error(e),
        }
    })
    .await
}
async fn record_stream(
    State(app): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<Search>,
) -> Response {
    let p = match auth(&headers, &app) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let (p2, q2) = (p.clone(), q.clone());
    if let Err(e) = blocking(&app, move |app| search(app, &p2, &q2)).await {
        return e;
    }
    let stream = stream::unfold(
        (
            app,
            p,
            q,
            Some(tokio::time::interval(std::time::Duration::from_secs(1))),
        ),
        |(app, p, mut q, timer)| async move {
            let mut timer = timer?;
            timer.tick().await;
            let (p2, q2) = (p.clone(), q.clone());
            let (event, timer) = match blocking(&app, move |app| search(app, &p2, &q2)).await {
                Ok(page) => {
                    if let Some(c) = page.next_cursor {
                        q.cursor = Some(c);
                    }
                    let event = Event::default()
                        .event("records")
                        .data(serde_json::to_string(&page).unwrap_or_default());
                    (event, Some(timer))
                }
                Err(response) => {
                    let code = match response.status() {
                        StatusCode::GONE => "cursor_expired",
                        StatusCode::FORBIDDEN => "forbidden",
                        _ => "storage_failure",
                    };
                    (Event::default().event("error").data(code), None)
                }
            };
            Some((
                Ok::<Event, std::convert::Infallible>(event),
                (app, p, q, timer),
            ))
        },
    );
    no_store(Sse::new(stream).into_response())
}
#[derive(Deserialize)]
struct VisibilityBody {
    visibility: Visibility,
}
async fn visibility(
    State(app): State<Shared>,
    Path(source): Path<String>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<VisibilityBody>,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if p.role != Role::Operator {
            return error(StatusCode::FORBIDDEN, "forbidden", "operator required");
        }
        match app.store.set_visibility(&source, body.visibility) {
            Ok(true) => {
                app.hud.reload(app, &source);
                StatusCode::NO_CONTENT.into_response()
            }
            Ok(false) => error(StatusCode::NOT_FOUND, "not_found", "source not found"),
            Err(e) => db_error(e),
        }
    })
    .await
}
async fn blob(
    State(app): State<Shared>,
    Path(digest): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if p.writable_sources.is_empty() {
            return error(StatusCode::FORBIDDEN, "forbidden", "write grant required");
        }
        if body.len() > 16 * 1024 * 1024 {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "too_large",
                "blob over 16 MiB",
            );
        }
        match app.store.put_blob(&digest, &body) {
            Ok(v) => encoded(&headers, &v),
            Err(e) => db_error(e),
        }
    })
    .await
}
async fn get_blob(
    State(app): State<Shared>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if p.role != Role::Operator {
            return error(StatusCode::FORBIDDEN, "forbidden", "operator required");
        }
        match app.store.get_blob(&digest) {
            Ok(Some(bytes)) => {
                no_store(([("content-type", "application/octet-stream")], bytes).into_response())
            }
            Ok(None) => error(StatusCode::NOT_FOUND, "not_found", "blob not found"),
            Err(e) => db_error(e),
        }
    })
    .await
}
async fn webhook(
    State(app): State<Shared>,
    Path((source, entity)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let Some(id) = headers.get("x-geri-event-id").and_then(|v| v.to_str().ok()) else {
            return error(
                StatusCode::BAD_REQUEST,
                "missing_event_id",
                "X-Geri-Event-ID required",
            );
        };
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream");
        match app
            .store
            .ingest_webhook(&p, &source, &entity, id, content_type, &body, now_ms())
        {
            Ok(receipt) => {
                app.hud.reload(app, &source);
                encoded(&headers, &receipt)
            }
            Err(e) => db_error(e),
        }
    })
    .await
}
async fn issue_command(
    State(app): State<Shared>,
    headers: HeaderMap,
    axum::Json(req): axum::Json<CommandRequest>,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if !p.can_command() {
            return error(StatusCode::FORBIDDEN, "forbidden", "operator required");
        }
        match app.store.issue_command(&p.agent_id, &req, now_ms()) {
            Ok(Some(command)) => {
                app.hud.reload(app, &command.source_id);
                app.signals.ring(&command.source_id);
                encoded(&headers, &command)
            }
            Ok(None) => error(StatusCode::NOT_FOUND, "not_found", "entity not found"),
            Err(e) => db_error(e),
        }
    })
    .await
}
#[derive(Deserialize, Default)]
struct AuditQuery {
    source: Option<String>,
    limit: Option<usize>,
}
async fn command_audit(
    State(app): State<Shared>,
    headers: HeaderMap,
    Query(q): Query<AuditQuery>,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match auth(&headers, app) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if !p.can_command() {
            return error(StatusCode::FORBIDDEN, "forbidden", "operator required");
        }
        match app
            .store
            .command_audit(q.source.as_deref(), q.limit.unwrap_or(100))
        {
            Ok(rows) => encoded(&headers, &rows),
            Err(e) => db_error(e),
        }
    })
    .await
}
pub(crate) fn router(app: Shared) -> Router {
    Router::new()
        .route("/", get(crate::ui::fleet))
        .route("/odn", get(crate::ui::fleet))
        .route("/v4/ui", get(crate::ui::fleet))
        .route("/n/{source}", get(crate::ui::source))
        .route("/odn/n/{source}", get(crate::ui::source))
        .route("/akis", get(crate::ui::fleet_stream))
        .route("/odn/akis", get(crate::ui::fleet_stream))
        .route("/akis/{source}", get(crate::ui::source_stream))
        .route("/hud/session", post(crate::ui::session))
        .route("/hud/logout", get(crate::ui::logout))
        .route("/hud/commands", post(crate::ui::command))
        .route(geri_view::DATASTAR_PATH, get(crate::ui::datastar))
        .route(geri_view::WEBTUI_PATH, get(crate::ui::webtui))
        .route(geri_view::AUGMENTED_PATH, get(crate::ui::augmented))
        .route("/v4/welcome", get(msg::http))
        .route("/v4/batch", post(msg::http))
        .route("/v4/sources", get(sources))
        .route("/v4/sources/{source}", put(msg::http))
        .route("/v4/sources/{source}/visibility", put(visibility))
        .route("/v4/state/{source}", get(state))
        .route("/v4/records", get(records))
        .route("/v4/digest", get(digest))
        .route("/v4/admin/backup", post(backup))
        .route("/v4/stream", get(record_stream))
        .route("/v4/blobs/{digest}", put(blob).get(get_blob))
        .route("/v4/webhook/{source}/{entity}", post(webhook))
        .route("/v4/commands", get(command_audit).post(issue_command))
        .route("/v4/commands/claim", get(msg::http))
        .route("/v4/commands/result", post(msg::http))
        .route("/v4/live/{source}", put(msg::http).get(msg::live_stream))
        .route("/v4/signals", get(msg::wait))
        .route("/v4/ws", get(msg::ws))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(middleware::from_fn_with_state(app.clone(), signed))
        .with_state(app)
}
fn limit(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    match env::var(name) {
        Err(_) => Ok(default),
        Ok(v) => match v.parse::<usize>() {
            Ok(n) if n > 0 => Ok(n),
            _ => Err(format!("{name} must be a positive number").into()),
        },
    }
}
fn tombstone_ms() -> Result<u64, Box<dyn std::error::Error>> {
    match env::var("GERI_TOMBSTONE_DAYS") {
        Err(_) => Ok(crate::store::DEFAULT_TOMBSTONE_MS),
        Ok(v) => v
            .parse::<u64>()
            .map(|days| days.saturating_mul(24 * 60 * 60 * 1000))
            .map_err(|_| "GERI_TOMBSTONE_DAYS must be a whole number of days".into()),
    }
}
pub async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let db = env::var("GERI_DB").unwrap_or_else(|_| "geri.sqlite".into());
    let blobs = env::var("GERI_BLOBS").unwrap_or_else(|_| "geri-blobs".into());
    let credentials: Vec<Credential> = serde_json::from_str(&env::var("GERI_AGENTS")?)?;
    if credentials.is_empty()
        || credentials.iter().any(|c| {
            !geri_sign::valid_agent(&c.id)
                || (c.token.is_empty() && c.public_key.is_none())
                || (!c.token.is_empty() && c.token.len() < 16)
                || role(&c.role).is_none()
        })
    {
        return Err(
            "GERI_AGENTS requires valid agent IDs, a role of agent/operator/developer, and a token of at least 16 characters or a public_key"
                .into(),
        );
    }
    let signing_seed = env::var("GERI_SIGNING_SEED")
        .ok()
        .map(|s| {
            geri_core::v4::parse_signing_seed(&s)
                .ok_or("GERI_SIGNING_SEED must be 64 lowercase hex characters")
        })
        .transpose()?;
    if env::var("GERI_TCP_LISTEN").is_ok() && signing_seed.is_none() {
        return Err("GERI_TCP_LISTEN (Noise) needs GERI_SIGNING_SEED: it is the hub's key".into());
    }
    let backup_root = env::var("GERI_BACKUP_DIR").ok().map(PathBuf::from);
    let app = Arc::new(App {
        hud: crate::ui::Hud::default(),
        store: Store::open(&PathBuf::from(db), &PathBuf::from(blobs))?
            .with_tombstones(tombstone_ms()?),
        credentials,
        signing_seed,
        backup_root,
        signals: Signals::new(
            limit("GERI_MAX_VIEWERS", msg::MAX_VIEWERS)?,
            limit("GERI_MAX_WS_PER_AGENT", msg::MAX_SOCKETS_PER_AGENT)?,
        ),
    });
    let cleanup = app.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop {
            tick.tick().await;
            loop {
                match blocking(&cleanup, |app| app.store.prune(now_ms())).await {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(e) => {
                        eprintln!("retention: {e:?}");
                        break;
                    }
                }
            }
            blocking(&cleanup, |app| app.hud.refresh(app)).await;
        }
    });
    if let Some(seed) = app.signing_seed() {
        eprintln!(
            "hub public key: {}",
            hex::encode(geri_sign::public_key(seed))
        );
    }
    if let Ok(addr) = env::var("GERI_TCP_LISTEN") {
        let listener = tokio::net::TcpListener::bind(addr.parse::<SocketAddr>()?).await?;
        tokio::spawn(tcp::listen(app.clone(), listener, tcp::Mode::Noise));
    }
    if let Ok(addr) = env::var("GERI_TCP_PLAIN_LISTEN") {
        eprintln!("plain tcp on {addr}: unencrypted, only the first hello is authenticated");
        let listener = tokio::net::TcpListener::bind(addr.parse::<SocketAddr>()?).await?;
        tokio::spawn(tcp::listen(app.clone(), listener, tcp::Mode::Plain));
    }
    let _quic = match env::var("GERI_QUIC_LISTEN") {
        Ok(addr) => {
            let cert = env::var("GERI_QUIC_CERT").ok();
            let key = env::var("GERI_QUIC_KEY").ok();
            let config = quic::config(app.signing_seed(), cert.as_deref(), key.as_deref())?;
            Some(quic::listen(app.clone(), config, addr.parse()?)?)
        }
        Err(_) => None,
    };
    let addr: SocketAddr = env::var("GERI_LISTEN")
        .unwrap_or_else(|_| "127.0.0.1:8787".into())
        .parse()?;
    axum::serve(tokio::net::TcpListener::bind(addr).await?, router(app)).await?;
    Ok(())
}

#[cfg(test)]
pub(crate) const TEST_SEED: [u8; 32] = [7; 32];

#[cfg(test)]
pub(crate) fn test_app(dir: &std::path::Path) -> Shared {
    test_app_with(dir, Signals::default())
}

#[cfg(test)]
pub(crate) const TEST_HUB_SEED: [u8; 32] = [5; 32];

#[cfg(test)]
pub(crate) fn test_app_with_key(dir: &std::path::Path, signals: Signals) -> Shared {
    let mut app = Arc::into_inner(test_app_with(dir, signals)).unwrap();
    app.signing_seed = Some(TEST_HUB_SEED);
    Arc::new(app)
}

#[cfg(test)]
pub(crate) fn test_app_with(dir: &std::path::Path, signals: Signals) -> Shared {
    Arc::new(App {
        hud: crate::ui::Hud::default(),
        store: Store::memory(&dir.join("blobs")).unwrap(),
        credentials: vec![
            Credential {
                id: "router".into(),
                token: "router-token-123456".into(),
                public_key: Some(geri_sign::public_key(&TEST_SEED)),
                sources: vec!["router".into()],
                role: "agent".into(),
            },
            Credential {
                id: "operator".into(),
                token: "operator-token-123456".into(),
                public_key: None,
                sources: vec![],
                role: "operator".into(),
            },
        ],
        signing_seed: None,
        backup_root: None,
        signals,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use base64::Engine;
    use geri_proto::v4::CommandStatus;
    use http_body_util::BodyExt;
    use tempfile::tempdir;
    use tower::ServiceExt;

    #[test]
    fn cbor_keeps_json_null_as_null() {
        let value = serde_json::json!({"args": null, "list": []});
        let bytes = geri_proto::cbor::to_vec(&value).unwrap();
        assert!(bytes.contains(&0xf6), "CBOR null expected: {bytes:02x?}");
        let back: serde_json::Value = geri_proto::cbor::from_slice(&bytes).unwrap();
        assert_eq!(back, value);
    }

    #[tokio::test]
    async fn http_json_cbor_auth_webhook_and_one_shot_command() {
        let dir = tempdir().unwrap();
        let app = router(Arc::new(App {
            hud: crate::ui::Hud::default(),
            store: Store::memory(&dir.path().join("blobs")).unwrap(),
            signing_seed: Some([1; 32]),
            backup_root: Some(dir.path().to_path_buf()),
            signals: Signals::default(),
            credentials: vec![
                Credential {
                    id: "agent".into(),
                    token: "agent-secret-123456".into(),
                    public_key: None,
                    sources: vec!["router".into()],
                    role: "agent".into(),
                },
                Credential {
                    id: "other".into(),
                    token: "other-secret-123456".into(),
                    public_key: None,
                    sources: vec!["other".into()],
                    role: "agent".into(),
                },
                Credential {
                    id: "op".into(),
                    token: "operator-secret-123456".into(),
                    public_key: None,
                    sources: vec![],
                    role: "operator".into(),
                },
            ],
        }));
        let send = |path: &str, token: &str, content_type: &str, body: Vec<u8>| {
            Request::builder()
                .method("POST")
                .uri(path)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap()
        };
        let batch = v4::Batch {
            v: 4,
            records: vec![v4::Record {
                source_id: "router".into(),
                entity_id: "cpu".into(),
                record_id: "r-1".into(),
                sequence: 1,
                kind: v4::RecordKind::State,
                event_time_ms: None,
                content_type: "application/json".into(),
                data: Some(serde_json::json!({"pct":42})),
                indexed: None,
                blob: None,
            }],
        };
        let unauthorized = app
            .clone()
            .oneshot(send(
                "/v4/batch",
                "bad",
                "application/json",
                serde_json::to_vec(&batch).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let accepted = app
            .clone()
            .oneshot(send(
                "/v4/batch",
                "agent-secret-123456",
                "application/cbor",
                geri_proto::cbor::to_vec(&batch).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let receipt: v4::Receipt =
            serde_json::from_slice(&accepted.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(receipt.records[0].status, v4::ReceiptStatus::Accepted);
        let private = Request::builder()
            .uri("/v4/state/router")
            .header("authorization", "Bearer other-secret-123456")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(private).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        let duplicate = app
            .clone()
            .oneshot(send(
                "/v4/batch",
                "agent-secret-123456",
                "application/json",
                serde_json::to_vec(&batch).unwrap(),
            ))
            .await
            .unwrap();
        let receipt: v4::Receipt =
            serde_json::from_slice(&duplicate.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(receipt.records[0].status, v4::ReceiptStatus::Duplicate);
        let mut changed = batch.clone();
        changed.records[0].data = Some(serde_json::json!({"pct":99}));
        let changed = app
            .clone()
            .oneshot(send(
                "/v4/batch",
                "agent-secret-123456",
                "application/json",
                serde_json::to_vec(&changed).unwrap(),
            ))
            .await
            .unwrap();
        let changed: v4::Receipt =
            serde_json::from_slice(&changed.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(changed.records[0].status, v4::ReceiptStatus::Conflict);
        let req = Request::builder()
            .method("POST")
            .uri("/v4/webhook/router/alert")
            .header("authorization", "Bearer agent-secret-123456")
            .header("x-geri-event-id", "alert-1")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"message":"hi"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );
        let command = v4::CommandRequest {
            source_id: "router".into(),
            entity_id: "cpu".into(),
            action: "press".into(),
            args: serde_json::Value::Null,
        };
        assert_eq!(
            app.clone()
                .oneshot(send(
                    "/v4/commands",
                    "operator-secret-123456",
                    "application/json",
                    serde_json::to_vec(&command).unwrap()
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let declaration = v4::SourceDeclaration {
            source_id: "router".into(),
            label: "Router".into(),
            kind: "router".into(),
            entities: vec![v4::Entity {
                group: None,
                source_id: "router".into(),
                entity_id: "cpu".into(),
                label: "CPU".into(),
                kind: "sensor".into(),
                control: Some(geri_proto::Control::new(
                    geri_proto::Shape::Trigger,
                    geri_proto::Danger::Safe,
                )),
            }],
        };
        let declared = Request::builder()
            .method("PUT")
            .uri("/v4/sources/router")
            .header("authorization", "Bearer agent-secret-123456")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&declaration).unwrap()))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(declared).await.unwrap().status(),
            StatusCode::OK
        );
        let mut wrong = command.clone();
        wrong.action = "set".into();
        assert_eq!(
            app.clone()
                .oneshot(send(
                    "/v4/commands",
                    "operator-secret-123456",
                    "application/json",
                    serde_json::to_vec(&wrong).unwrap()
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let issued = app
            .clone()
            .oneshot(send(
                "/v4/commands",
                "operator-secret-123456",
                "application/json",
                serde_json::to_vec(&command).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(issued.status(), StatusCode::OK);
        let claim = || {
            Request::builder()
                .uri("/v4/commands/claim?source=router")
                .header("authorization", "Bearer agent-secret-123456")
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(claim()).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone().oneshot(claim()).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
        let get = |path: &str| {
            Request::builder()
                .uri(path)
                .header("authorization", "Bearer operator-secret-123456")
                .body(Body::empty())
                .unwrap()
        };
        let audit = app
            .clone()
            .oneshot(get("/v4/commands?source=router"))
            .await
            .unwrap();
        assert_eq!(audit.status(), StatusCode::OK);
        let audits: Vec<v4::CommandAudit> =
            serde_json::from_slice(&audit.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].status, CommandStatus::Dispatched);
        assert_eq!(audits[0].command.action, "press");
        let result = v4::CommandResult {
            command_id: audits[0].command.command_id.clone(),
            ok: true,
            result: serde_json::json!({"done":true}),
        };
        assert_eq!(
            app.clone()
                .oneshot(send(
                    "/v4/commands/result?source=router",
                    "agent-secret-123456",
                    "application/json",
                    serde_json::to_vec(&result).unwrap()
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::NO_CONTENT
        );
        let audit = app
            .clone()
            .oneshot(get("/v4/commands?source=router"))
            .await
            .unwrap();
        let audits: Vec<v4::CommandAudit> =
            serde_json::from_slice(&audit.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(audits[0].status, CommandStatus::Done);
        assert_eq!(audits[0].result.as_ref().unwrap()["ok"], true);
        let records = app
            .clone()
            .oneshot(get("/v4/records?limit=10"))
            .await
            .unwrap();
        let page: v4::RecordPage =
            serde_json::from_slice(&records.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let event = page
            .records
            .iter()
            .find(|r| r.record.entity_id == "alert")
            .unwrap();
        let raw = event.record.data.as_ref().unwrap()["raw_base64"]
            .as_str()
            .unwrap();
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(raw)
                .unwrap(),
            br#"{"message":"hi"}"#
        );
        let digest = app
            .clone()
            .oneshot(get("/v4/digest?limit=10"))
            .await
            .unwrap();
        let summary: v4::SignedSummary =
            serde_json::from_slice(&digest.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(geri_core::v4::verify_summary(&summary, &page.records));
        let backup = app
            .oneshot(send(
                "/v4/admin/backup",
                "operator-secret-123456",
                "application/json",
                Vec::new(),
            ))
            .await
            .unwrap();
        assert_eq!(backup.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&backup.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let path = PathBuf::from(body["path"].as_str().unwrap());
        Store::verify_backup(&path).unwrap();
        let restored = Store::open(&path.join("db.sqlite"), &path.join("blobs")).unwrap();
        assert_eq!(
            restored
                .records(None, 0, None, None, None, 10)
                .unwrap()
                .records
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn signed_requests_stand_in_for_the_token_and_cannot_be_replayed() {
        let dir = tempdir().unwrap();
        let app = router(test_app(dir.path()));
        let signer = geri_sign::Signer::new("router", TEST_SEED).unwrap();
        let call = |method: &str, uri: &str, auth: &str, body: &[u8]| {
            let mut request = Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json");
            if !auth.is_empty() {
                request = request.header("authorization", auth);
            }
            let request = request.body(Body::from(body.to_vec())).unwrap();
            let app = app.clone();
            async move { app.oneshot(request).await.unwrap() }
        };
        let code = |r: Response| async move {
            let body = r.into_body().collect().await.unwrap().to_bytes();
            serde_json::from_slice::<ApiError>(&body).unwrap().code
        };

        let first = signer.authorization("GET", "/v4/sources", b"");
        assert_eq!(
            call("GET", "/v4/sources", &first, b"").await.status(),
            StatusCode::OK
        );
        let replay = call("GET", "/v4/sources", &first, b"").await;
        assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(replay.headers()[geri_sign::COUNTER_HEADER], "1");
        assert_eq!(code(replay).await, "replay");

        let second = signer.authorization("GET", "/v4/sources", b"");
        let moved = call("GET", "/v4/sources?x=1", &second, b"").await;
        assert_eq!(moved.status(), StatusCode::UNAUTHORIZED);
        assert!(moved.headers().get(geri_sign::COUNTER_HEADER).is_none());
        assert_eq!(
            call("GET", "/v4/sources", &second, b"").await.status(),
            StatusCode::OK
        );

        let batch = serde_json::to_vec(&serde_json::json!({"v": 4, "records": [{
            "source_id": "router", "entity_id": "cpu", "record_id": "r-1", "sequence": 1,
            "kind": "state", "content_type": "application/json", "data": {"pct": 1}
        }]}))
        .unwrap();
        let declare = serde_json::to_vec(&serde_json::json!({
            "source_id": "router", "label": "Router", "kind": "router", "entities": []
        }))
        .unwrap();
        let auth = signer.authorization("PUT", "/v4/sources/router", &declare);
        assert!(
            call("PUT", "/v4/sources/router", &auth, &declare)
                .await
                .status()
                .is_success()
        );
        let auth = signer.authorization("POST", "/v4/batch", &batch);
        let tampered = String::from_utf8(batch.clone())
            .unwrap()
            .replace(r#""pct":1"#, r#""pct":9"#)
            .into_bytes();
        assert_ne!(tampered, batch);
        assert_eq!(
            call("POST", "/v4/batch", &auth, &tampered).await.status(),
            StatusCode::UNAUTHORIZED
        );
        let receipt = call("POST", "/v4/batch", &auth, &batch).await;
        assert_eq!(receipt.status(), StatusCode::OK);
        let body = receipt.into_body().collect().await.unwrap().to_bytes();
        assert!(
            String::from_utf8_lossy(&body).contains("accepted"),
            "{body:?}"
        );

        let rebooted = geri_sign::Signer::new("router", TEST_SEED).unwrap();
        let stale = call(
            "GET",
            "/v4/sources",
            &rebooted.authorization("GET", "/v4/sources", b""),
            b"",
        )
        .await;
        assert_eq!(stale.status(), StatusCode::UNAUTHORIZED);
        let counter =
            geri_sign::counter(stale.headers()[geri_sign::COUNTER_HEADER].to_str().unwrap());
        rebooted.learn(counter.unwrap());
        let again = rebooted.authorization("GET", "/v4/sources", b"");
        assert_eq!(
            call("GET", "/v4/sources", &again, b"").await.status(),
            StatusCode::OK
        );

        let spoofed = Request::builder()
            .uri("/v4/sources")
            .header("x-geri-verified", "operator")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(spoofed).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let operator = geri_sign::Signer::new("operator", TEST_SEED).unwrap();
        let no_key = call(
            "GET",
            "/v4/sources",
            &operator.authorization("GET", "/v4/sources", b""),
            b"",
        )
        .await;
        assert_eq!(no_key.status(), StatusCode::UNAUTHORIZED);
        let huge = vec![b'x'; MAX_BODY + 1];
        let unknown = call(
            "POST",
            "/v4/batch",
            &operator.authorization("POST", "/v4/batch", &huge),
            &huge,
        )
        .await;
        assert_eq!(unknown.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            call("GET", "/v4/sources", "Bearer router-token-123456", b"")
                .await
                .status(),
            StatusCode::OK
        );
    }

    #[test]
    fn a_misspelled_role_is_a_configuration_error() {
        assert_eq!(role(""), Some(Role::Agent));
        assert_eq!(role("operator"), Some(Role::Operator));
        assert_eq!(role("operatr"), None);
    }

    #[test]
    fn the_replay_window_survives_a_hub_restart() {
        let dir = tempdir().unwrap();
        let db = dir.path().join("geri.sqlite");
        let store = Store::open(&db, &dir.path().join("blobs")).unwrap();
        assert_eq!(store.accept_counter("router", 5).unwrap(), Ok(()));
        assert_eq!(store.accept_counter("router", 3).unwrap(), Ok(()));
        assert_eq!(
            store.accept_counter("other", 5).unwrap(),
            Ok(()),
            "agent başına"
        );
        assert_eq!(store.accept_counter("router", 5 + 63).unwrap(), Ok(()));
        drop(store);
        let store = Store::open(&db, &dir.path().join("blobs")).unwrap();
        assert_eq!(store.accept_counter("router", 5).unwrap(), Err(68));
        assert_eq!(store.accept_counter("router", 68).unwrap(), Err(68));
        assert_eq!(store.accept_counter("router", 4).unwrap(), Err(68));
        assert_eq!(store.accept_counter("router", 6).unwrap(), Ok(()));
        assert_eq!(store.accept_counter("router", u64::MAX).unwrap(), Ok(()));
        assert_eq!(
            store.accept_counter("router", u64::MAX).unwrap(),
            Err(u64::MAX)
        );
    }
}
