use crate::api::{App, Shared, auth, blocking, can_read, error, no_store, now_ms};
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{
        Html, IntoResponse, Redirect, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use datastar::prelude::{PatchElements, PatchSignals};
use geri_core::v4::AgentPolicy;
use geri_proto::v4::CommandRequest;
use geri_proto::{Plain, Values};
use geri_view as view;
use maud::html;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    sync::Mutex,
};
use tokio::sync::broadcast;

const SESSION_MS: u64 = 12 * 60 * 60 * 1000;
const MAX_SESSIONS: usize = 1024;

pub(crate) struct Hud {
    sources: Mutex<BTreeMap<String, view::SourceView>>,
    changed: broadcast::Sender<String>,
    sessions: Mutex<HashMap<[u8; 32], (String, u64)>>,
}
impl Default for Hud {
    fn default() -> Self {
        Self {
            sources: Mutex::default(),
            changed: broadcast::channel(256).0,
            sessions: Mutex::default(),
        }
    }
}
impl Hud {
    pub(crate) fn reload(&self, app: &App, source: &str) {
        match app.store.hud_data(source) {
            Ok(Some(data)) => {
                let mut sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
                let previous = sources.remove(source);
                sources.insert(source.into(), data.apply(previous));
            }
            Ok(None) => {}
            Err(e) => eprintln!("HUD {source}: {e}"),
        }
        let _ = self.changed.send(source.into());
    }
    fn open_session(&self, agent: &str) -> Option<String> {
        let mut id = [0u8; 32];
        getrandom::fill(&mut id).ok()?;
        let now = now_ms();
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if sessions.len() >= MAX_SESSIONS {
            sessions.retain(|_, (_, until)| *until > now);
        }
        if sessions.len() >= MAX_SESSIONS
            && let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, (_, until))| *until)
                .map(|(k, _)| *k)
        {
            sessions.remove(&oldest);
        }
        sessions.insert(Sha256::digest(id).into(), (agent.into(), now + SESSION_MS));
        Some(hex::encode(id))
    }
    fn session(&self, id: &str) -> Option<String> {
        let id = hex::decode(id).ok()?;
        let key: [u8; 32] = Sha256::digest(&id).into();
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        match sessions.get(&key) {
            Some((agent, until)) if *until > now_ms() => Some(agent.clone()),
            Some(_) => {
                sessions.remove(&key);
                None
            }
            None => None,
        }
    }
    fn close_session(&self, id: &str) {
        if let Ok(id) = hex::decode(id) {
            let key: [u8; 32] = Sha256::digest(&id).into();
            self.sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
        }
    }
    pub(crate) fn refresh(&self, app: &App) {
        let ids: Vec<_> = self
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        for id in ids {
            self.reload(app, &id);
        }
    }
    pub(crate) fn live(&self, source: &str, values: &serde_json::Map<String, serde_json::Value>) {
        let mut sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(view) = sources.get_mut(source) {
            let values: Values = values
                .iter()
                .filter_map(|(id, v)| {
                    serde_json::from_value::<Plain>(v.clone())
                        .ok()
                        .map(|p| (id.clone(), p.0))
                })
                .collect();
            if view.live(values, now_ms()) {
                let _ = self.changed.send(source.into());
            }
        }
    }
    pub(crate) fn notify(&self, source: &str) {
        let _ = self.changed.send(source.into());
    }
    pub(crate) fn disconnected(&self, source: &str) {
        if let Some(view) = self
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(source)
        {
            view.clear_live();
        }
        self.notify(source);
    }
    fn ensure(&self, app: &App, p: &AgentPolicy) -> Result<Vec<String>, Box<Response>> {
        let list = app.store.sources(p).map_err(|_| {
            Box::new(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "store",
                "kaynaklar okunamadı",
            ))
        })?;
        let ids: Vec<_> = list.into_iter().map(|s| s.source_id).collect();
        for id in &ids {
            if !self
                .sources
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(id)
            {
                self.reload(app, id);
            }
        }
        Ok(ids)
    }
}
fn cookie(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.split(';')
                .find_map(|v| v.trim().strip_prefix("geri_hud="))
        })
}
fn policy(headers: &HeaderMap, app: &App) -> Result<AgentPolicy, Box<Response>> {
    if headers.contains_key("authorization") {
        return auth(headers, app).map_err(Box::new);
    }
    cookie(headers)
        .and_then(|id| app.hud.session(id))
        .and_then(|agent| app.agent(&agent))
        .ok_or_else(|| {
            Box::new(error(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "giriş gerekli",
            ))
        })
}
fn same_origin(headers: &HeaderMap) -> bool {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    match header("origin") {
        Some(origin) => origin.split("://").nth(1) == header("host"),
        None => header("sec-fetch-site").is_none_or(|site| site == "same-origin" || site == "none"),
    }
}
#[derive(serde::Deserialize)]
pub(crate) struct Login {
    token: String,
}
pub(crate) async fn session(
    State(app): State<Shared>,
    headers: HeaderMap,
    Json(login): Json<Login>,
) -> Response {
    if !same_origin(&headers) {
        return error(StatusCode::FORBIDDEN, "origin", "yanlış köken");
    }
    let mut supplied = HeaderMap::new();
    let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", login.token)) else {
        return error(StatusCode::UNAUTHORIZED, "unauthorized", "giriş reddedildi");
    };
    supplied.insert("authorization", value);
    let p = match auth(&supplied, &app) {
        Ok(p) => p,
        Err(response) => return response,
    };
    let Some(id) = app.hud.open_session(&p.agent_id) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "random",
            "oturum açılamadı",
        );
    };
    let secure = if headers
        .get("x-forwarded-proto")
        .is_some_and(|v| v == "https")
    {
        "; Secure"
    } else {
        ""
    };
    no_store(
        (
            [(
                "set-cookie",
                format!(
                    "geri_hud={id}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{secure}",
                    SESSION_MS / 1000
                ),
            )],
            StatusCode::NO_CONTENT,
        )
            .into_response(),
    )
}
pub(crate) async fn logout(State(app): State<Shared>, headers: HeaderMap) -> Response {
    if let Some(id) = cookie(&headers) {
        app.hud.close_session(id);
    }
    no_store(
        (
            [(
                "set-cookie",
                "geri_hud=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
            )],
            Redirect::to("/"),
        )
            .into_response(),
    )
}
fn presence(app: &App, source: &view::SourceView) -> view::Presence {
    if let Some(seconds) = app.signals.connected(&source.declaration.source_id) {
        return view::Presence::Up(Some(seconds));
    }
    let ago = now_ms().saturating_sub(source.last_seen_ms) / 1000;
    if !source.disconnected && ago < 360 {
        view::Presence::Up(None)
    } else {
        view::Presence::Down(ago)
    }
}
fn commander<'a>(
    app: &App,
    source: &view::SourceView,
    p: &AgentPolicy,
) -> Option<view::Commander<'a>> {
    p.can_command().then_some(view::Commander {
        endpoint: "/hud/commands",
        live: matches!(presence(app, source), view::Presence::Up(_)),
    })
}
fn log(source: &view::SourceView) -> maud::Markup {
    let commands = source.commands(now_ms());
    let rows: Vec<_> = source
        .commands
        .iter()
        .zip(&commands)
        .map(|(c, row)| view::LogRow {
            ago: row.ago,
            entity: row.name,
            desired: row.desired,
            by: &c.audit.command.issued_by,
            state: row.state,
        })
        .collect();
    view::log_panel(&rows)
}
fn stats(app: &App, source: &view::SourceView, p: &AgentPolicy) -> maud::Markup {
    html! { (source.stats(commander(app, source, p),app.signals.connected(&source.declaration.source_id),now_ms())) (view::label_el(&source.declaration.label)) (source.events(now_ms())) @if p.can_command() {(log(source))} }
}
fn account(p: &AgentPolicy) -> view::Account<'_> {
    view::Account::Admin {
        who: &p.agent_id,
        logout: "/hud/logout",
    }
}
pub(crate) async fn fleet(State(app): State<Shared>, headers: HeaderMap) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match policy(&headers, app) {
            Ok(p) => p,
            Err(_) => return no_store(Html(view::login_page().into_string()).into_response()),
        };
        let ids = match app.hud.ensure(app, &p) {
            Ok(ids) => ids,
            Err(e) => return *e,
        };
        let sources = app.hud.sources.lock().unwrap_or_else(|e| e.into_inner());
        let tiles: Vec<_> = ids
            .iter()
            .filter_map(|id| sources.get(id))
            .map(|s| s.tile(presence(app, s), true, true))
            .collect();
        no_store(Html(view::fleet_page(&tiles, "/akis", account(&p)).into_string()).into_response())
    })
    .await
}
pub(crate) async fn source(
    State(app): State<Shared>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    blocking(&app.clone(), move |app| {
        let p = match policy(&headers, app) {
            Ok(p) => p,
            Err(_) => return no_store(Redirect::to("/").into_response()),
        };
        if !can_read(app, &p, &id) {
            return error(
                StatusCode::FORBIDDEN,
                "forbidden",
                "kaynak erişimi reddedildi",
            );
        }
        if let Err(e) = app.hud.ensure(app, &p) {
            return *e;
        }
        let sources = app.hud.sources.lock().unwrap_or_else(|e| e.into_inner());
        let Some(source) = sources.get(&id) else {
            return error(StatusCode::NOT_FOUND, "not_found", "kaynak bulunamadı");
        };
        let commands = source.commands(now_ms());
        let sheet = view::Sheet {
            entities: &source.declaration.entities,
            values: &source.values,
            commands: &commands,
            commander: commander(app, source, &p),
            presenter: None,
        };
        let stream = format!("/akis/{id}");
        no_store(
            Html(
                view::node_page(
                    &source.declaration.label,
                    &sheet,
                    app.signals.connected(&id),
                    false,
                    &stream,
                    view::Nav {
                        back: Some("/"),
                        account: account(&p),
                    },
                    Some(html! {(source.events(now_ms())) @if p.can_command() {(log(source))}}),
                )
                .into_string(),
            )
            .into_response(),
        )
    })
    .await
}
fn elements(html: maud::Markup) -> Event {
    PatchElements::new(html.into_string()).write_as_axum_sse_event()
}
fn signals(json: String) -> Event {
    PatchSignals::new(json).write_as_axum_sse_event()
}
pub(crate) async fn command(
    State(app): State<Shared>,
    headers: HeaderMap,
    Json(command): Json<CommandRequest>,
) -> Response {
    if !same_origin(&headers) {
        return error(StatusCode::FORBIDDEN, "origin", "yanlış köken");
    }
    let p = match policy(&headers, &app) {
        Ok(p) => p,
        Err(e) => return *e,
    };
    let message = if !p.can_command() {
        "operatör yetkisi gerekli".into()
    } else {
        let request = command.clone();
        blocking(&app, move |app| {
            match app.store.issue_command(&p.agent_id, &request, now_ms()) {
                Ok(Some(issued)) => {
                    app.hud.reload(app, &issued.source_id);
                    app.signals.ring(&issued.source_id);
                    String::new()
                }
                Ok(None) => "entity bulunamadı".into(),
                Err(e) => format!("{e}"),
            }
        })
        .await
    };
    no_store(
        Sse::new(futures_util::stream::once(async move {
            Ok::<_, Infallible>(signals(view::error_signals(
                &command.source_id,
                &command.entity_id,
                &message,
            )))
        }))
        .into_response(),
    )
}
pub(crate) async fn fleet_stream(State(app): State<Shared>, headers: HeaderMap) -> Response {
    stream(app, headers, None).await
}
pub(crate) async fn source_stream(
    State(app): State<Shared>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    stream(app, headers, Some(id)).await
}
struct StreamState {
    app: Shared,
    policy: AgentPolicy,
    source: Option<String>,
    viewers: BTreeMap<String, crate::msg::Viewer>,
    rx: broadcast::Receiver<String>,
    timer: tokio::time::Interval,
    initial: bool,
    initial_signals: bool,
}
async fn stream(app: Shared, headers: HeaderMap, source: Option<String>) -> Response {
    let policy = match policy(&headers, &app) {
        Ok(p) => p,
        Err(e) => return *e,
    };
    let (p2, id) = (policy.clone(), source.clone());
    let readable = blocking(&app, move |app| id.is_none_or(|id| can_read(app, &p2, &id))).await;
    if !readable {
        return error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "kaynak erişimi reddedildi",
        );
    }
    let rx = app.hud.changed.subscribe();
    let mut state = StreamState {
        app,
        policy,
        source,
        viewers: BTreeMap::new(),
        rx,
        timer: tokio::time::interval(std::time::Duration::from_secs(30)),
        initial: true,
        initial_signals: true,
    };
    let policy = state.policy.clone();
    if let Err(e) = blocking(&state.app, move |app| app.hud.ensure(app, &policy)).await {
        return *e;
    }
    if let Err(e) = state.sync() {
        return *e;
    }
    let events = futures_util::stream::unfold(state, |mut state| async move {
        loop {
            if state.initial_signals {
                state.initial_signals = false;
                return Some((
                    Ok::<_, Infallible>(signals("{\"offline\":false,\"busy\":false}".into())),
                    state,
                ));
            }
            let changed = if state.initial {
                state.initial = false;
                None
            } else {
                tokio::select! {
                    value=state.rx.recv()=>match value {Ok(id)=>Some(id),Err(broadcast::error::RecvError::Lagged(_))=>None,Err(_)=>return None},
                    _=state.timer.tick()=>None,
                }
            };
            if changed
                .as_ref()
                .is_some_and(|id| state.source.as_ref().is_some_and(|source| source != id))
            {
                continue;
            }
            let previous = state.viewers.len();
            if state.sync().is_err() {
                return None;
            }
            let sources = state
                .app
                .hud
                .sources
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let markup = if let Some(id) = &state.source {
                if !state.viewers.contains_key(id) {
                    return None;
                }
                let source = sources.get(id)?;
                stats(&state.app, source, &state.policy)
            } else if let Some(id) = changed.as_ref().filter(|_| previous == state.viewers.len()) {
                let Some(source) = sources.get(id).filter(|_| state.viewers.contains_key(id))
                else {
                    continue;
                };
                let total = state.viewers.len();
                let up = state
                    .viewers
                    .keys()
                    .filter_map(|id| sources.get(id))
                    .filter(|s| matches!(presence(&state.app, s), view::Presence::Up(_)))
                    .count();
                html! { (view::tile(&source.tile(presence(&state.app, source), true, true))) (view::fleet_presence(up,total)) }
            } else {
                let tiles: Vec<_> = state
                    .viewers
                    .keys()
                    .filter_map(|id| sources.get(id))
                    .map(|s| s.tile(presence(&state.app, s), true, true))
                    .collect();
                view::fleet(&tiles)
            };
            drop(sources);
            return Some((Ok::<_, Infallible>(elements(markup)), state));
        }
    });
    no_store(
        Sse::new(events)
            .keep_alive(KeepAlive::default())
            .into_response(),
    )
}
impl StreamState {
    fn sync(&mut self) -> Result<(), Box<Response>> {
        let ids: Vec<_> = self
            .app
            .hud
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|s| {
                self.policy.can_write(&s.declaration.source_id)
                    || self.policy.can_read(s.visibility)
            })
            .map(|s| s.declaration.source_id.clone())
            .collect();
        let ids: Vec<_> = ids
            .into_iter()
            .filter(|id| self.source.as_ref().is_none_or(|s| s == id))
            .collect();
        self.viewers.retain(|id, _| ids.contains(id));
        for id in ids {
            if !self.viewers.contains_key(&id) {
                let Some((viewer, _)) = crate::msg::Viewer::join(self.app.clone(), id.clone())
                else {
                    return Err(Box::new(error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "busy",
                        "çok fazla izleyici",
                    )));
                };
                self.viewers.insert(id, viewer);
            }
        }
        Ok(())
    }
}
fn asset(bytes: &'static [u8], kind: &str, gzip: bool) -> Response {
    let mut response = (
        [
            ("content-type", kind),
            ("cache-control", "public, max-age=31536000, immutable"),
        ],
        bytes,
    )
        .into_response();
    if gzip {
        response
            .headers_mut()
            .insert("content-encoding", HeaderValue::from_static("gzip"));
    }
    response
}
pub(crate) async fn datastar() -> Response {
    asset(
        view::DATASTAR_JS.as_bytes(),
        "text/javascript; charset=utf-8",
        false,
    )
}
pub(crate) async fn webtui() -> Response {
    asset(
        view::WEBTUI_CSS.as_bytes(),
        "text/css; charset=utf-8",
        false,
    )
}
pub(crate) async fn augmented() -> Response {
    asset(view::AUGMENTED_CSS_GZ, "text/css; charset=utf-8", true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn call(
        app: &Shared,
        method: &str,
        uri: &str,
        cookie: Option<&str>,
        token: Option<&str>,
        body: Value,
    ) -> Response {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(cookie) = cookie {
            request = request.header("cookie", cookie);
        }
        if let Some(token) = token {
            request = request.header("authorization", format!("Bearer {token}"));
        }
        crate::api::router(app.clone())
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap()
    }
    async fn text(response: Response) -> String {
        String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap()
    }
    async fn seed(app: &Shared) {
        let response = call(app,"PUT","/v4/sources/router",None,Some("router-token-123456"),json!({
            "source_id":"router","label":"Test router","kind":"router",
            "entities":[
                {"source_id":"router","entity_id":"z.last","label":"First declared","kind":"metric","group":"hardware"},
                {"source_id":"router","entity_id":"a.first","label":"Second declared","kind":"metric","group":"hardware"},
                {"source_id":"router","entity_id":"Fan.Power","label":"Fan","kind":"switch","control":{"shape":{"type":"bool"}}}
            ]
        })).await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = call(app,"POST","/v4/batch",None,Some("router-token-123456"),json!({"v":4,"records":[
            {"source_id":"router","entity_id":"z.last","record_id":"r1","sequence":1,"kind":"state","content_type":"application/json","data":10},
            {"source_id":"router","entity_id":"a.first","record_id":"r2","sequence":2,"kind":"state","content_type":"application/json","data":20},
            {"source_id":"router","entity_id":"Fan.Power","record_id":"r3","sequence":3,"kind":"state","content_type":"application/json","data":false},
            {"source_id":"router","entity_id":"boot","record_id":"r4","sequence":0,"kind":"event","content_type":"application/json","data":"boot-event <safe>"}
        ]})).await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    #[tokio::test]
    async fn session_is_required_and_cookie_respects_read_and_command_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::api::test_app(dir.path());
        seed(&app).await;
        assert!(
            text(call(&app, "GET", "/", None, None, json!(null)).await)
                .await
                .contains("id=\"login\"")
        );
        assert_eq!(
            call(
                &app,
                "POST",
                "/hud/session",
                None,
                None,
                json!({"token":"wrong"})
            )
            .await
            .status(),
            StatusCode::UNAUTHORIZED
        );
        let login = call(
            &app,
            "POST",
            "/hud/session",
            None,
            None,
            json!({"token":"operator-token-123456"}),
        )
        .await;
        assert_eq!(login.status(), StatusCode::NO_CONTENT);
        let cookie = login.headers()["set-cookie"].to_str().unwrap().to_owned();
        assert!(cookie.contains("HttpOnly; SameSite=Strict; Max-Age=43200"));
        assert!(!cookie.contains("operator-token") && !cookie.contains("b3BlcmF0b3I"));
        let html =
            text(call(&app, "GET", "/n/router", Some(&cookie), None, json!(null)).await).await;
        assert!(html.contains("id=\"ctl-Fan.Power\"") && html.contains("/hud/commands"));
        assert!(
            html.contains("hardware") && html.find("First declared") < html.find("Second declared")
        );
        assert!(html.contains("boot-event &lt;safe&gt;") && html.contains("sırasız"));
        let reader = text(
            call(
                &app,
                "GET",
                "/n/router",
                None,
                Some("router-token-123456"),
                json!(null),
            )
            .await,
        )
        .await;
        assert!(!reader.contains("id=\"ctl-Fan.Power\""));
        assert_eq!(
            call(
                &app,
                "GET",
                "/n/other",
                None,
                Some("router-token-123456"),
                json!(null)
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
        let request =
            json!({"source_id":"router","entity_id":"Fan.Power","action":"set","args":true});
        let denied = text(
            call(
                &app,
                "POST",
                "/hud/commands",
                None,
                Some("router-token-123456"),
                request.clone(),
            )
            .await,
        )
        .await;
        assert!(denied.contains("operatör yetkisi gerekli"));
        assert!(
            app.store
                .command_audit(Some("router"), 10)
                .unwrap()
                .is_empty()
        );
        let allowed =
            text(call(&app, "POST", "/hud/commands", Some(&cookie), None, request).await).await;
        assert!(allowed.contains("datastar-patch-signals"));
        assert_eq!(
            app.store.command_audit(Some("router"), 10).unwrap().len(),
            1
        );
        let logout = call(&app, "GET", "/hud/logout", Some(&cookie), None, json!(null)).await;
        assert!(
            logout.headers()["set-cookie"]
                .to_str()
                .unwrap()
                .contains("Max-Age=0")
        );
        assert!(
            text(call(&app, "GET", "/", Some(&cookie), None, json!(null)).await)
                .await
                .contains("id=\"login\"")
        );
    }
    #[tokio::test]
    async fn hud_stream_tracks_viewers_and_keeps_live_values_across_command_writes() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::api::test_app(dir.path());
        seed(&app).await;
        let response = call(
            &app,
            "GET",
            "/akis/router",
            None,
            Some("operator-token-123456"),
            json!(null),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(app.signals.watched("router"));
        let mut body = response.into_body();
        let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert!(
            String::from_utf8(first.to_vec())
                .unwrap()
                .contains("datastar-patch-signals")
        );
        let live = call(
            &app,
            "PUT",
            "/v4/live/router",
            None,
            Some("router-token-123456"),
            json!({"z.last":73}),
        )
        .await;
        assert!(text(live).await.contains("true"));
        call(
            &app,
            "POST",
            "/hud/commands",
            None,
            Some("operator-token-123456"),
            json!({"source_id":"router","entity_id":"Fan.Power","action":"set","args":true}),
        )
        .await;
        let html = text(
            call(
                &app,
                "GET",
                "/n/router",
                None,
                Some("operator-token-123456"),
                json!(null),
            )
            .await,
        )
        .await;
        assert!(
            html.contains(">73<") && html.contains("GÖNDERİLDİ"),
            "{html}"
        );
        let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
        let patch = String::from_utf8(frame.to_vec()).unwrap();
        assert!(patch.contains("datastar-patch-elements") && patch.contains(">73<"));
        drop(body);
        assert!(!app.signals.watched("router"));
    }

    #[tokio::test]
    async fn last_socket_close_restores_durable_values_and_updates_fleet_presence() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::api::test_app(dir.path());
        seed(&app).await;
        let socket = crate::msg::Socket::open(app.clone(), "router".into()).unwrap();
        let mut stream = call(
            &app,
            "GET",
            "/akis",
            None,
            Some("operator-token-123456"),
            json!(null),
        )
        .await
        .into_body();
        stream.frame().await.unwrap().unwrap();
        stream.frame().await.unwrap().unwrap();
        call(
            &app,
            "PUT",
            "/v4/live/router",
            None,
            Some("router-token-123456"),
            json!({"z.last":73}),
        )
        .await;
        assert_eq!(
            app.hud.sources.lock().unwrap()["router"].values["z.last"].as_f64(),
            Some(73.0)
        );
        drop(socket);
        assert!(app.signals.connected("router").is_none());
        assert_eq!(
            app.hud.sources.lock().unwrap()["router"].values["z.last"].as_f64(),
            Some(10.0)
        );
        let patch = String::from_utf8(
            stream
                .frame()
                .await
                .unwrap()
                .unwrap()
                .into_data()
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(
            patch.contains("id=\"fleet-up\"") && patch.contains("0/1 UP"),
            "{patch}"
        );
        let html = text(
            call(
                &app,
                "GET",
                "/n/router",
                None,
                Some("operator-token-123456"),
                json!(null),
            )
            .await,
        )
        .await;
        assert!(html.contains("düğüm canlı değil") && html.contains("disabled"));
    }

    #[tokio::test]
    async fn redeclaration_preserves_its_order_before_unlisted_entities() {
        let dir = tempfile::tempdir().unwrap();
        let app = crate::api::test_app(dir.path());
        seed(&app).await;
        let response = call(&app,"PUT","/v4/sources/router",None,Some("router-token-123456"),json!({
            "source_id":"router","label":"Test router","kind":"router","entities":[
                {"source_id":"router","entity_id":"a.first","label":"Second declared","kind":"metric","group":"hardware"},
                {"source_id":"router","entity_id":"z.last","label":"First declared","kind":"metric","group":"hardware"}
            ]
        })).await;
        assert_eq!(response.status(), StatusCode::OK);
        let sources = app.hud.sources.lock().unwrap();
        let entities: Vec<_> = sources["router"]
            .declaration
            .entities
            .iter()
            .map(|e| e.entity_id.as_str())
            .collect();
        assert_eq!(entities, ["a.first", "z.last", "Fan.Power", "boot"]);
        assert!(
            sources["router"].declaration.entities[2].control.is_none(),
            "geçmiş entity kalır ama kaldırılan kontrolün yetkisi geri çekilir"
        );
    }
}
