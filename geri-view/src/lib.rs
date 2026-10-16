// Testler Claude tarafından yazılmıştır.

mod render;
mod source;
mod widget;

use geri_proto::Value;
use maud::{DOCTYPE, Markup, PreEscaped, html};

pub use render::{
    Cmd, CmdState, Commander, Footprint, IMAGE, Layout, MAX_COLS, PresentationState, PresentedBody,
    Presenter, Sheet, UPTIME, columns, is_image,
};

pub use geri_proto::Values;
pub use source::{CommandView, EventView, SourceView};

pub const DATASTAR_PATH: &str = "/static/datastar-1.0.4.js";
pub const DATASTAR_JS: &str = include_str!("../assets/datastar.js");
pub const WEBTUI_PATH: &str = "/static/webtui-0.1.10.css";
pub const WEBTUI_CSS: &str = include_str!("../assets/webtui-0.1.10.css");
pub const AUGMENTED_PATH: &str = "/static/augmented-ui-2.0.0.min.css";
pub const AUGMENTED_CSS_GZ: &[u8] = include_bytes!("../assets/augmented-ui-2.0.0.min.css.gz");
const CSS: &str = include_str!("../assets/style.css");
const ACTION_JS: &str = include_str!("../assets/actions.js");
const HL_JS: &str = include_str!("../assets/speed-highlight-2.1.0.js");
const CODE_JS: &str = include_str!("../assets/code.js");

pub const STREAM_PATH: &str = "/akis";
pub const NODE_PATH: &str = "/n/";
pub const ADMIN_PATH: &str = "/odn";
pub const LOGOUT_PATH: &str = "/cdn-cgi/access/logout";

fn connect(stream: &str) -> String {
    format!("@get('{stream}', {{retry: 'always', retryMaxCount: Infinity}})")
}

const ON_FETCH: &str =
    "['error', 'retrying', 'retries-failed'].includes(evt.detail.type) && ($offline = true)";

pub fn page(label: &str, sheet: &Sheet, busy: bool) -> Markup {
    node_page(label, sheet, None, busy, STREAM_PATH, Nav::default(), None)
}

#[derive(Clone, Copy, Default)]
pub struct Nav<'a> {
    pub back: Option<&'a str>,
    pub account: Account<'a>,
}

#[derive(Clone, Copy, Default, Debug, PartialEq)]
pub enum Account<'a> {
    #[default]
    None,
    Guest {
        login: &'a str,
    },
    Admin {
        who: &'a str,
        logout: &'a str,
    },
}

impl Account<'_> {
    fn admin(&self) -> bool {
        matches!(self, Self::Admin { .. })
    }

    fn home(&self) -> &'static str {
        if self.admin() { ADMIN_PATH } else { "/" }
    }
}

fn account_el(account: Account) -> Markup {
    html! {
        @match account {
            Account::None => {}
            Account::Guest { login } => {
                a class="account login" href=(login) { "GİRİŞ" }
            }
            Account::Admin { who, logout } => {
                span class="account" {
                    span class="who" title=(who) { (clip_chars(who.into(), 32)) }
                    a class="logout" href=(logout) { "ÇIKIŞ" }
                }
            }
        }
    }
}

fn masthead(account: Account, right: Markup) -> Markup {
    html! {
        div class="row" {
            h1 {
                a class="brand-link" href=(account.home()) { "GERI" }
                small { @if account.admin() { " // admin" } @else { " // sentinel" } }
            }
            div class="top-right" { (right) (account_el(account)) }
        }
    }
}

pub fn node_page(
    label: &str,
    sheet: &Sheet,
    connected: Option<u64>,
    busy: bool,
    stream: &str,
    nav: Nav,
    extra: Option<Markup>,
) -> Markup {
    shell(
        busy,
        stream,
        html! {
            header class="top" {
                (masthead(nav.account, uptime_el(sheet.values, connected, sheet.presenter)))
                (label_el(label))
            }
            (stats_grid(sheet))
            @if let Some(extra) = extra { (extra) }
            (footer(nav))
        },
    )
}

pub fn login_page() -> Markup {
    shell(
        false,
        "",
        html! {
            header class="top" { (masthead(Account::None, html! {})) }
            section class="panel accent-slate ok" "box-"="square" {
                div class="panel-head" { span class="title" { "GİRİŞ" } }
                form id="login" {
                    label for="token" { "Operatör veya geliştirici tokenı" }
                    input id="token" type="password" autocomplete="off" required;
                    button type="submit" { "BAĞLAN" }
                    div id="login-error" class="state err" role="alert" {}
                }
            }
            script { (PreEscaped(r#"document.getElementById('login').addEventListener('submit', async e => {
            e.preventDefault();
            const input = document.getElementById('token');
            try {
                const response = await fetch('/hud/session', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({token:input.value})});
                input.value = '';
                if (!response.ok) throw Error('Giriş reddedildi');
                location.assign('/');
            } catch(error) { document.getElementById('login-error').textContent = error.message; }
        });"#)) }
        },
    )
}

fn shell(busy: bool, stream: &str, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="tr" data-webtui-theme="dark" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "GERI // sentinel" }
                link rel="icon" href="data:,";
                link rel="stylesheet" href=(WEBTUI_PATH);
                link rel="stylesheet" href=(AUGMENTED_PATH);
                style { (PreEscaped(CSS)) }
                script type="module" src=(DATASTAR_PATH) data-cfasync="false" {}
                script { (PreEscaped(ACTION_JS)) }
                script { (PreEscaped(HL_JS)) }
                script { (PreEscaped(CODE_JS)) }
            }
            body data-signals:offline="false" data-signals:busy=(busy) data-init=[(!stream.is_empty()).then(|| connect(stream))] data-on:datastar-fetch=(ON_FETCH) {
                main class="chassis" data-augmented-ui="tl-clip tr-clip br-clip bl-clip b-clip-x border inlay" {
                    (body)
                }
            }
        }
    }
}

fn footer(nav: Nav) -> Markup {
    html! {
        footer class="foot" {
            span class="nav" {
                @if let Some(href) = nav.back {
                    a class="nav-link back" href=(href) { "← GERI" }
                }
            }
            span class="brand" { "CAN EROĞLU // FREKI FORGE" }
        }
    }
}

pub fn signal_key(source: &str, entity: &str) -> String {
    use std::fmt::Write;
    let mut key = String::with_capacity((source.len() + entity.len()) * 2 + 1);
    for byte in source.bytes() {
        let _ = write!(key, "{byte:02x}");
    }
    key.push('_');
    for byte in entity.bytes() {
        let _ = write!(key, "{byte:02x}");
    }
    key
}

pub fn error_signals(source: &str, entity: &str, message: &str) -> String {
    let key = format!("_e_{}", signal_key(source, entity));
    format!("{{{}: {}}}", widget::js_str(&key), widget::js_str(message))
}

pub struct LogRow<'a> {
    pub ago: u64,
    pub entity: &'a str,
    pub desired: &'a Value,
    pub by: &'a str,
    pub state: CmdState<'a>,
}

pub fn log_panel(rows: &[LogRow]) -> Markup {
    html! {
        section id="log" class="panel accent-slate ok log" "box-"="square" "shear-"="top" {
            div class="panel-head" {
                span class="title" "is-"="badge" { "komut defteri" }
                span class="pct" "is-"="badge" { (rows.len()) }
            }
            @if rows.is_empty() { div class="sub" { "henüz komut yok" } }
            dl class="lines" {
                @for r in rows {
                    dt { (ago_text(r.ago)) }
                    dd {
                        (r.entity)
                        @if !matches!(r.desired, Value::Null) { " = " (widget::short(r.desired)) }
                        " · " span class="dim" { (r.by) } " · "
                        @let (class, text) = widget::phase(r.state, r.desired, None);
                        span class={ "state " (class) } { (text) }
                    }
                }
            }
        }
    }
}

pub fn stats(sheet: &Sheet) -> Markup {
    node_stats(sheet, None)
}

pub fn node_stats(sheet: &Sheet, connected: Option<u64>) -> Markup {
    html! { (stats_grid(sheet)) (uptime_el(sheet.values, connected, sheet.presenter)) }
}

pub fn stats_cached(sheet: &Sheet, layout: &mut Layout) -> Markup {
    node_stats_cached(sheet, None, layout)
}
pub fn node_stats_cached(sheet: &Sheet, connected: Option<u64>, layout: &mut Layout) -> Markup {
    html! {(stats_grid_cached(sheet, Some(layout))) (uptime_el(sheet.values, connected, sheet.presenter))}
}
fn stats_grid(sheet: &Sheet) -> Markup {
    stats_grid_cached(sheet, None)
}
fn stats_grid_cached(sheet: &Sheet, layout: Option<&mut Layout>) -> Markup {
    let cells = render::cells(sheet);
    if cells.is_empty() {
        return render::grid_cached(
            "stats",
            &[render::Cell {
                markup: html! {
                    section class="panel accent-slate ok" "box-"="square" "shear-"="top" {
                        div class="panel-head" { span class="title" "is-"="badge" { "düğüm" } }
                        div class="value off" { "henüz değer yok" }
                    }
                },
                size: Footprint { w: 1, h: 1 },
            }],
            layout,
        );
    }
    render::grid_cached("stats", &cells, layout)
}

pub fn label_el(label: &str) -> Markup {
    html! { div id="node" class="meta" { (label) } }
}

fn uptime_el(values: &Values, connected: Option<u64>, presenter: Option<&dyn Presenter>) -> Markup {
    let own = values
        .get(UPTIME)
        .and_then(|v| presenter.and_then(|p| p.seconds(v)));
    let up = match (own, connected) {
        (Some(s), _) => format!("UP {}", uptime(s)).to_uppercase(),
        (_, Some(s)) => format!("UP {}", coarse(s)).to_uppercase(),
        _ => "UP —".into(),
    };
    live_el("uptime", &up)
}

fn live_el(id: &str, text: &str) -> Markup {
    html! {
        span id=(id) class="live" data-class:offline="$offline" data-class:busy="$busy" {
            span class="live-online" { (text) }
            span class="live-offline" data-text="$busy ? 'KALABALIK · SIRADASIN' : 'BAĞLANTI YOK · YENİDEN DENENİYOR'" {
                "BAĞLANTI YOK · YENİDEN DENENİYOR"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Presence {
    Up(Option<u64>),
    Down(u64),
    Asleep(u64),
}

pub struct Tile<'a> {
    pub id: &'a str,
    pub label: &'a str,
    pub presence: Presence,
    pub values: Vec<(&'a str, &'a Value)>,
    pub presenter: Option<&'a dyn Presenter>,
    pub masked: bool,
    pub admin: bool,
}

impl Tile<'_> {
    fn href(&self) -> String {
        if self.admin || self.masked {
            format!("{ADMIN_PATH}{NODE_PATH}{}", self.id)
        } else {
            format!("{NODE_PATH}{}", self.id)
        }
    }
}

const BRIEF_MAX: usize = 6;
const BRIEF_CHARS: usize = 12;

pub fn fleet_page(tiles: &[Tile], stream: &str, account: Account) -> Markup {
    shell(
        false,
        stream,
        html! {
            header class="top" {
                (masthead(account, fleet_up(tiles)))
                (fleet_meta(tiles))
            }
            (fleet_grid(tiles))
            (footer(Nav::default()))
        },
    )
}

pub fn fleet(tiles: &[Tile]) -> Markup {
    html! { (fleet_grid(tiles)) (fleet_up(tiles)) (fleet_meta(tiles)) }
}

fn fleet_up(tiles: &[Tile]) -> Markup {
    let up = tiles
        .iter()
        .filter(|t| matches!(t.presence, Presence::Up(_)))
        .count();
    fleet_presence(up, tiles.len())
}

pub fn fleet_presence(up: usize, total: usize) -> Markup {
    live_el("fleet-up", &format!("{up}/{total} UP"))
}

fn fleet_meta(tiles: &[Tile]) -> Markup {
    html! { div id="fleet-meta" class="meta" { "filo · " (tiles.len()) " düğüm" } }
}

fn fleet_grid(tiles: &[Tile]) -> Markup {
    const ONE: Footprint = Footprint { w: 1, h: 1 };
    let mut cells: Vec<_> = tiles
        .iter()
        .map(|t| render::Cell {
            markup: tile(t),
            size: ONE,
        })
        .collect();
    if cells.is_empty() {
        cells.push(render::Cell {
            markup: html! {
                section class="panel accent-slate ok" "box-"="square" "shear-"="top" {
                    div class="panel-head" { span class="title" "is-"="badge" { "filo" } }
                    div class="value off" { "henüz düğüm yok" }
                }
            },
            size: ONE,
        });
    }
    render::grid("fleet", &cells)
}

pub fn tile(t: &Tile) -> Markup {
    let mut briefs: Vec<(&str, String, Level)> = Vec::new();
    let mut lists: Vec<(&str, &[Value])> = Vec::new();
    for (name, v) in t.values.iter().copied() {
        match v {
            Value::List(l) => lists.push((name, l.as_slice())),
            _ => {
                if let Some((text, level)) = brief_with(v, t.presenter) {
                    briefs.push((name, text, level));
                }
            }
        }
    }

    let level = briefs.iter().map(|b| b.2).max().unwrap_or(Level::Ok);
    let (class, text) = match t.presence {
        Presence::Up(connected) => {
            let own = t.values.iter().find_map(|(name, v)| {
                (*name == UPTIME)
                    .then(|| t.presenter.and_then(|p| p.seconds(v)))
                    .flatten()
            });
            let secs = own.or(connected);
            (
                "up",
                secs.map_or("UP".into(), |s| format!("UP {}", coarse(s))),
            )
        }
        Presence::Down(ago) => ("down", format!("DOWN · {}", ago_text(ago))),
        Presence::Asleep(ago) => ("asleep", format!("UYKUDA · {}", ago_text(ago))),
    };
    let accent = if class == "up" && !t.masked {
        "accent-cyan"
    } else {
        "accent-slate"
    };
    let masked = if t.masked { " masked" } else { "" };

    html! {
        a id={ "tile-" (t.id) } class={ "tile " (class) (masked) } href=(t.href()) {
            section class={ "panel " (accent) " " (level.class()) } "box-"="square" "shear-"="top" {
                div class="panel-head" {
                    span class="title" "is-"="badge" { (t.id) }
                    span class={ "presence " (class) } "is-"="badge" { i {} (text.to_uppercase()) }
                }
                div class="sub" { (clip_chars(t.label.into(), 48)) }
                div class="tile-body" {
                    @if t.masked {
                        div class="sealed" { "🔒 GİRİŞ GEREKLİ" }
                    }
                    @if !briefs.is_empty() && !t.masked {
                        div class="briefs" {
                            @for (name, text, level) in briefs.iter().take(BRIEF_MAX) {
                                div class={ "brief " (level.class()) } {
                                    span class="k" { (name) }
                                    span class="v" { (text) }
                                }
                            }
                        }
                        @if briefs.len() > BRIEF_MAX {
                            div class="sub more" { "+" (briefs.len() - BRIEF_MAX) " değer" }
                        }
                    }
                    @for (_, list) in lists.iter().take(usize::from(!t.masked)) {
                        ul class="tile-list" {
                            @for item in list.iter().take(5) {
                                @let full_title = render::item_title(item);
                                li class="tile-item" title=(full_title) {
                                    span class="bullet" { "› " }
                                    span class="text" { (clip_chars(full_title, 64)) }
                                }
                            }
                        }
                        @if list.len() > 5 {
                            div class="sub more" { "+" (list.len() - 5) " daha →" }
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn brief_with(v: &Value, presenter: Option<&dyn Presenter>) -> Option<(String, Level)> {
    if let Some(result) = presenter.and_then(|p| p.brief(v)) {
        return result;
    }
    if v.get("t").and_then(Value::as_str) == Some("gauge")
        && let Some(number) = v.get("v").and_then(Value::as_f64)
    {
        let unit = v.get("unit").and_then(Value::as_str).unwrap_or("");
        let level = v
            .get("max")
            .and_then(Value::as_f64)
            .filter(|m| *m > 0.0)
            .map_or(Level::Ok, |m| gauge_level(v, (number / m * 100.0) as f32));
        let text = match v.get("brief").and_then(Value::as_str) {
            Some(brief) => brief.to_owned(),
            None => format!("{number:.1}{unit}"),
        };
        return Some((clip_chars(text.trim_end().into(), BRIEF_CHARS), level));
    }
    let text = match v {
        Value::Bool(b) => (if *b { "açık" } else { "kapalı" }).into(),
        Value::Text(t) => t.clone(),
        Value::List(l) => format!("{} öğe", l.len()),
        v => show(v),
    };
    Some((clip_chars(text, BRIEF_CHARS), Level::Ok))
}

pub(crate) fn gauge_level(v: &Value, pct: f32) -> Level {
    if v.get("good").and_then(Value::as_str) == Some("high") {
        Level::of(100.0 - pct, 80.0, 95.0)
    } else {
        Level::of(pct, 80.0, 95.0)
    }
}

fn coarse(secs: u64) -> String {
    let (d, h, m) = (secs / 86_400, secs % 86_400 / 3_600, secs % 3_600 / 60);
    match (d, h) {
        (0, 0) if m == 0 => "<1d".into(),
        (0, 0) => format!("{m}d"),
        (0, _) => format!("{h}s {m}d"),
        _ => format!("{d}g {h}s {m}d"),
    }
}

fn ago_text(secs: u64) -> String {
    match secs {
        0..60 => format!("{secs}sn önce"),
        60..3_600 => format!("{}d önce", secs / 60),
        3_600..86_400 => format!("{}s önce", secs / 3_600),
        _ => format!("{}g önce", secs / 86_400),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Ok,
    Warn,
    Crit,
}

impl Level {
    pub fn of(pct: f32, warn: f32, crit: f32) -> Self {
        if pct >= crit {
            Self::Crit
        } else if pct >= warn {
            Self::Warn
        } else {
            Self::Ok
        }
    }
    pub(crate) fn class(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Crit => "crit",
        }
    }
}

pub(crate) const SHOW_MAX: usize = 160;

pub(crate) fn show(v: &Value) -> String {
    match v {
        Value::Null => "—".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Text(t) => format!("{t:?}"),
        Value::Bytes(b) => format!("{} bayt", b.len()),
        Value::List(l) => format!("[{}]", l.iter().map(show).collect::<Vec<_>>().join(", ")),
        Value::Map(m) => format!(
            "{{{}}}",
            m.iter()
                .map(|(k, v)| format!("{k}: {}", show(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub(crate) fn clip_chars(s: String, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => format!("{}…", &s[..i]),
        None => s,
    }
}

pub(crate) fn value(num: &str, unit: &str) -> Markup {
    html! { div class="value" { (num) @if !unit.is_empty() { small { " " (unit) } } } }
}

pub fn uptime(secs: u64) -> String {
    let (d, h, m, s) = (
        secs / 86_400,
        secs % 86_400 / 3_600,
        secs % 3_600 / 60,
        secs % 60,
    );
    match (d, h) {
        (0, 0) => format!("{m}d {s}sn"),
        (0, _) => format!("{h}s {m}d {s}sn"),
        _ => format!("{d}g {h}s {m}d {s}sn"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geri_proto::v4::Entity;
    use geri_proto::{Danger, Shape};
    use std::collections::BTreeMap;

    fn body(sheet: &Sheet) -> String {
        stats(sheet).into_string()
    }

    #[test]
    fn gauge_brief_and_direction_come_from_the_source() {
        let map = |kv: &[(&str, Value)]| {
            Value::Map(kv.iter().map(|(k, v)| ((*k).into(), v.clone())).collect())
        };
        let free = |v: f64, good: Option<&str>| {
            let mut kv = vec![
                ("t", Value::Text("gauge".into())),
                ("v", Value::Float(v)),
                ("max", Value::Float(100.0)),
                ("unit", Value::Text("MiB".into())),
                ("brief", Value::Text(format!("{v:.0}% boş"))),
            ];
            if let Some(g) = good {
                kv.push(("good", Value::Text(g.into())));
            }
            map(&kv)
        };
        let (text, level) = brief_with(&free(90.0, Some("high")), None).unwrap();
        assert_eq!(text, "90% boş");
        assert_eq!(level, Level::Ok);
        assert_eq!(
            brief_with(&free(3.0, Some("high")), None).unwrap().1,
            Level::Crit
        );
        assert_eq!(brief_with(&free(90.0, None), None).unwrap().1, Level::Warn);
    }

    #[test]
    fn gauge_sub_lines_come_from_the_source() {
        let map = |kv: &[(&str, Value)]| {
            Value::Map(kv.iter().map(|(k, v)| ((*k).into(), v.clone())).collect())
        };
        let gauge = |sub: Option<Value>| {
            let mut kv = vec![
                ("t", Value::Text("gauge".into())),
                ("v", Value::Float(60.0)),
                ("max", Value::Float(233.4)),
                ("unit", Value::Text("MiB".into())),
            ];
            if let Some(s) = sub {
                kv.push(("sub", s));
            }
            map(&kv)
        };
        let page = |v: Value| {
            let e = [Entity::new("router", "mem")];
            let values = Values::from([("mem".into(), v)]);
            stats(&Sheet::read_only(&e, &values)).into_string()
        };
        assert!(page(gauge(None)).contains("/ 233.4 MiB"));
        let one = page(gauge(Some(Value::Text("kullanılan 170 MiB".into()))));
        assert!(one.contains("kullanılan 170 MiB") && !one.contains("/ 233.4"));
        let two = page(gauge(Some(Value::List(vec![
            Value::Text("a satırı".into()),
            Value::Text("b satırı".into()),
        ]))));
        assert!(two.contains("a satırı") && two.contains("b satırı"));
    }

    #[test]
    fn unknown_entities_get_generic_panels() {
        let map = |kv: &[(&str, Value)]| {
            Value::Map(kv.iter().map(|(k, v)| ((*k).into(), v.clone())).collect())
        };
        let v = Values::from([
            (
                "queue".into(),
                map(&[
                    ("t", Value::Text("gauge".into())),
                    ("v", Value::Float(45.0)),
                    ("max", Value::Uint(50)),
                    ("unit", Value::Text("iş".into())),
                ]),
            ),
            ("ups".into(), Value::Bool(true)),
            (
                "future".into(),
                map(&[
                    (
                        "custom",
                        Value::List(vec![Value::Uint(1), Value::Float(2.5)]),
                    ),
                    ("raw", Value::Bytes(vec![0; 3])),
                    ("fix", Value::Null),
                ]),
            ),
            ("blob".into(), Value::Text("<b>".repeat(100))),
        ]);
        let e: Vec<_> = ["queue", "ups", "future", "blob"]
            .map(|id| Entity::new("router", id))
            .into();
        let body = body(&Sheet::read_only(&e, &v));
        assert!(body.contains(">queue<") && body.contains("45.0") && body.contains("90%"));
        assert!(body.contains(">ups<") && body.contains("açık"));
        assert!(body.contains("{custom: [1, 2.5], fix: —, raw: 3 bayt}"));
        assert!(body.contains("&lt;b&gt;") && !body.contains("<b>"));
    }

    #[test]
    fn huge_values_are_clipped() {
        let e = [Entity::new("router", "x")];
        let v = Values::from([("x".into(), Value::Text("y".repeat(6_000)))]);
        let body = body(&Sheet::read_only(&e, &v));
        assert!(body.contains('…'), "{body}");
        assert!(body.len() < 3_000, "{}", body.len());

        let big_list = Value::List(vec![Value::Uint(7); 1000]);
        let t = Tile {
            masked: false,
            admin: false,
            id: "hn",
            label: "Big Feed",
            presence: Presence::Up(Some(10)),
            values: vec![("x", &big_list)],
            presenter: None,
        };
        let tile_html = tile(&t).into_string();
        assert!(tile_html.contains("+995 daha →"));
        assert!(tile_html.len() < 3_000);
    }

    #[test]
    fn groups_share_a_panel_and_footprint_follows_value_kind() {
        let e = [
            Entity::new("router", "clients").group("wifi"),
            Entity::new("router", "band").group("wifi").label("bant"),
            Entity::new("router", "feed"),
        ];
        let v = Values::from([
            ("clients".into(), Value::Uint(3)),
            ("band".into(), Value::Text("5 GHz".into())),
            (
                "feed".into(),
                Value::List(vec![Value::Map(
                    [("title".into(), Value::Text("ilk".into()))].into(),
                )]),
            ),
        ]);
        let body = body(&Sheet::read_only(&e, &v));
        assert!(
            body.contains(">wifi<") && body.contains(r#"<div class="sub caption">clients</div>"#)
        );
        assert!(body.contains("<dt>bant</dt><dd>5 GHz</dd>"));
        assert!(body.contains(r#"class="cell w2 h2""#) && body.contains(">ilk<"));
        let long = Values::from([("clients".into(), Value::Text("x".repeat(500)))]);
        assert!(
            stats(&Sheet::read_only(&e[..1], &long))
                .into_string()
                .contains("cell w1 h1")
        );
    }

    #[test]
    fn empty_node_still_renders() {
        let body = page(
            "router bekleniyor",
            &Sheet::read_only(&[], &Values::new()),
            false,
        )
        .into_string();
        assert!(body.contains("UP —") && body.contains("router bekleniyor"));
        assert!(body.contains("henüz değer yok"));
    }

    fn controls() -> Vec<Entity> {
        vec![
            Entity::new("router", "tempo").group("geri").control(
                Shape::Range {
                    min: 250.0,
                    max: 60_000.0,
                    step: Some(250.0),
                    unit: Some("ms".into()),
                },
                Danger::Safe,
            ),
            Entity::new("router", "reboot")
                .group("geri")
                .label("yeniden başlat")
                .control(Shape::Trigger, Danger::Destructive),
            Entity::new("router", "wan")
                .group("geri")
                .control(Shape::Trigger, Danger::Risky),
            Entity::new("router", "guest")
                .label("misafir")
                .control(Shape::Bool, Danger::Safe),
            Entity::new("router", "band").control(Shape::choice(["2.4", "5"]), Danger::Safe),
            Entity::new("router", "led").control(Shape::Unknown, Danger::Safe),
        ]
    }

    fn control_values() -> Values {
        Values::from([
            ("tempo".into(), Value::Uint(1000)),
            ("guest".into(), Value::Bool(true)),
            ("band".into(), Value::Text("5".into())),
        ])
    }

    #[test]
    fn public_face_has_no_controls() {
        let (e, v) = (controls(), control_values());
        let body = body(&Sheet::read_only(&e, &v));
        assert!(!body.contains("@post") && !body.contains("<button") && !body.contains("<input"));
        assert!(!body.contains(">reboot<") && !body.contains("yeniden başlat"));
        assert!(body.contains("1000"), "değerler yine görünür");
    }

    #[test]
    fn commander_face_draws_widgets_from_shapes_and_gestures_from_danger() {
        let (e, v) = (controls(), control_values());
        let sheet = Sheet {
            entities: &e,
            values: &v,
            commands: &[],
            commander: Some(Commander {
                endpoint: "/odn/commands",
                live: true,
            }),
            presenter: None,
        };
        let body = body(&sheet);
        assert!(body.contains(r#"type="number" min="250" max="60000" step="250""#));
        let key = signal_key("router", "tempo");
        assert!(body.contains(&format!("data-bind=\"_c_{key}\"")) && body.contains(">ms<"));
        assert!(body.contains(&format!("$_c_{key} != 1000")));
        assert!(
            body.contains("@post(&quot;/odn/commands&quot;")
                && body.contains(&format!("args: $_c_{key}"))
                && body.contains("entity_id: &quot;tempo&quot;")
        );
        assert!(
            body.contains("yeniden başlat · BASILI TUT") && body.contains("data-on:pointerdown")
        );
        assert!(body.contains("GERİ AL · wan") && body.contains("3000"));
        assert!(body.contains("entity_id: &quot;guest&quot;") && body.contains("args: false"));
        assert!(body.contains(">KAPAT<") && body.contains(">AÇIK<"));
        assert!(body.contains(r#"<span class="seg on">5</span>"#));
        assert!(
            body.contains(r#"{payload: {value: \"2.4\"}}"#.replace('\\', "").as_str())
                || body.contains("&quot;2.4&quot;")
        );
        assert!(body.contains("salt okunur") && !body.contains("/c/led"));
        let at = |s: &str| body.find(s).unwrap();
        assert!(at("danger-rule") < at("BASILI TUT") && at("GERİ AL · wan") < at("danger-rule"));
    }

    #[test]
    fn code_control_is_a_highlighted_textarea_without_inline_diff() {
        let e = [Entity::new("freki", "models.config")
            .label("model ayarı")
            .control(Shape::code(2048, Some("toml")), Danger::Risky)];
        let v = Values::from([(
            "models.config".into(),
            Value::Text("[roles]\nevolve = \"abonelik\"\n".into()),
        )]);
        let sheet = Sheet {
            entities: &e,
            values: &v,
            commands: &[],
            commander: Some(Commander {
                endpoint: "/odn/commands",
                live: true,
            }),
            presenter: None,
        };
        let body = body(&sheet);
        let key = signal_key("freki", "models.config");
        assert!(body.contains(r#"<geri-code data-lang="toml">"#));
        // Datastar morph'u boyanmış katmanı silmesin.
        assert!(body.contains(r#"<pre data-ignore-morph aria-hidden="true"><code></code></pre>"#));
        assert!(
            body.contains(&format!(r#"data-bind="_c_{key}""#))
                && body.contains(r#"maxlength="2048""#)
        );
        assert!(body.contains("<textarea") && body.contains(r#"spellcheck="false""#));
        // Kutu bildirilen değerle dolu açılır.
        assert!(
            body.contains(r#"[roles]\nevolve = \&quot;abonelik\&quot;"#),
            "{body}"
        );
        // Çok satırlı değerde satır içi "eski → yeni" farkı gösterilmez.
        assert!(!body.contains("' → '"));
        assert!(body.contains(">KAYDET<"));
        assert!(CODE_JS.contains("customElements.define('geri-code'") && HL_JS.contains("GeriHL"));
    }

    #[test]
    fn risky_and_destructive_widgets_arm_through_element_lifecycle() {
        let (e, v) = (controls(), control_values());
        let sheet = Sheet {
            entities: &e,
            values: &v,
            commands: &[],
            commander: Some(Commander {
                endpoint: "/odn/commands",
                live: true,
            }),
            presenter: None,
        };
        let body = body(&sheet);
        // Kurma/iptal elementin timer/pointer durumunda yaşar; sinyal timer'ı yok.
        assert!(body.contains(r#"<geri-action data-danger="risky" data-delay="3000""#));
        assert!(body.contains(r#"<geri-action data-danger="destructive" data-delay="1500""#));
        assert!(body.contains("arm(evt,") && body.contains("data-on:geri-confirm"));
        assert!(!body.contains("clearTimeout") && !body.contains("setTimeout"));
        assert!(!body.contains("_u_") && !body.contains("_h_"));
        // VAZGEÇ element timer'ını da iptal eder.
        assert!(body.contains("querySelector('geri-action')?.cancel()"));
        // Yüklenen yardımcı betik nesil sayacıyla eski callback'i etkisiz kılar.
        assert!(
            ACTION_JS.contains("#generation")
                && ACTION_JS.contains("JSON.parse(JSON.stringify(payload))")
        );
    }

    #[test]
    fn controls_lock_with_a_reason_and_show_the_command_state() {
        let (e, v) = (controls(), control_values());
        let desired = Value::Uint(500);
        let cmds = [Cmd {
            name: "tempo",
            desired: &desired,
            state: CmdState::Applying,
            ago: 2,
        }];
        let sheet = |live| Sheet {
            entities: &e,
            values: &v,
            commands: &cmds,
            commander: Some(Commander {
                endpoint: "/odn/commands",
                live,
            }),
            presenter: None,
        };
        let body = stats(&sheet(true)).into_string();
        assert!(body.contains("⧗ UYGULANIYOR · düğüm henüz 1000 bildiriyor"));
        assert!(body.contains("ctl safe pending locked"));
        let down = stats(&sheet(false)).into_string();
        assert!(down.contains("düğüm canlı değil") && down.contains("disabled"));
        let tempo = down
            .split("id=\"ctl-tempo\"")
            .nth(1)
            .unwrap()
            .split("id=\"ctl-")
            .next()
            .unwrap();
        assert!(
            tempo.contains("düğüm canlı değil"),
            "sonuç satırı kilit nedenini gizlemesin"
        );
        assert!(!down.contains("ctl safe locked\"") || down.contains("locked"));
    }

    #[test]
    fn item_controls_render_one_row_per_item() {
        let e = [Entity::new("router", "todo").control(Shape::item(Shape::Bool), Danger::Safe)];
        let item = |id: &str, title: &str, done| {
            Value::Map(
                [
                    ("id".into(), Value::Text(id.into())),
                    ("title".into(), Value::Text(title.into())),
                    ("value".into(), Value::Bool(done)),
                ]
                .into(),
            )
        };
        let v = Values::from([(
            "todo".into(),
            Value::List(vec![item("t1", "süt al", false), item("t2", "kod", true)]),
        )]);
        let sheet = Sheet {
            entities: &e,
            values: &v,
            commands: &[],
            commander: Some(Commander {
                endpoint: "/odn/commands",
                live: true,
            }),
            presenter: None,
        };
        let body = body(&sheet);
        assert!(body.contains("süt al") && body.contains("item done"));
        assert!(body.contains("{id: &quot;t1&quot;, value: true}"), "{body}");
        assert_eq!(body.matches("<li>").count(), 2, "liste bir kez çizilir");
    }

    #[test]
    fn masked_tile_shows_only_identity_and_presence() {
        let secret = Value::Text("dişçi 14:00".into());
        let mut t = Tile {
            masked: true,
            admin: false,
            id: "cal",
            label: "Takvim",
            presence: Presence::Up(Some(60)),
            values: vec![("next", &secret)],
            presenter: None,
        };
        let body = tile(&t).into_string();
        assert!(
            body.contains("Takvim") && body.contains(">UP 1D<") && body.contains("GİRİŞ GEREKLİ")
        );
        assert!(body.contains(r#"href="/odn/n/cal""#) && body.contains("tile up masked"));
        assert!(!body.contains("dişçi"), "{body}");
        t.masked = false;
        t.admin = true;
        let body = tile(&t).into_string();
        assert!(body.contains("dişçi") && body.contains(r#"href="/odn/n/cal""#));
        assert!(!body.contains("GİRİŞ GEREKLİ"));
    }

    #[test]
    fn account_corner_follows_the_face() {
        let admin = fleet_page(
            &[],
            "/odn/akis",
            Account::Admin {
                who: "can@x",
                logout: LOGOUT_PATH,
            },
        )
        .into_string();
        assert!(admin.contains("can@x") && admin.contains(r#"href="/cdn-cgi/access/logout""#));
        assert!(admin.contains("// admin") && admin.contains(r#"class="brand-link" href="/odn""#));
        assert!(admin.contains("@get('/odn/akis'") && !admin.contains("GİRİŞ"));
        let guest = fleet_page(&[], STREAM_PATH, Account::Guest { login: "/odn" }).into_string();
        assert!(guest.contains(r#"href="/odn">GİRİŞ<"#) && !guest.contains("ÇIKIŞ"));
        assert!(
            !page("x", &Sheet::read_only(&[], &Values::new()), false)
                .into_string()
                .contains("GİRİŞ")
        );
    }

    #[test]
    fn empty_fleet_renders() {
        let body = fleet_page(&[], STREAM_PATH, Account::Guest { login: ADMIN_PATH }).into_string();
        assert!(body.contains("0/0 UP") && body.contains("henüz düğüm yok"));
    }

    #[test]
    fn node_page_streams_its_node() {
        let e = [Entity::new("router", "co2")];
        let v = Values::from([("co2".into(), Value::Uint(612))]);
        let sheet = Sheet::read_only(&e, &v);
        let nav = Nav {
            back: Some("/"),
            account: Account::Guest {
                login: "/odn/n/router",
            },
        };
        let body =
            node_page("OpenWrt", &sheet, None, false, "/akis/router", nav, None).into_string();
        assert!(body.contains(r#"<a class="account login" href="/odn/n/router">GİRİŞ</a>"#));
        assert!(body.contains("data-init=\"@get('/akis/router'"));
        assert!(body.contains(r#"href="/""#) && body.contains(r#"id="stats""#));
        assert!(body.contains("← GERI") && body.contains("// sentinel"));
        assert!(
            !page("OpenWrt", &sheet, false)
                .into_string()
                .contains("← GERI")
        );
    }

    #[test]
    fn non_router_node_page() {
        let e = [Entity::new("router", "co2")];
        let v = Values::from([("co2".into(), Value::Uint(612))]);
        let body = node_page(
            "ESP32",
            &Sheet::read_only(&e, &v),
            Some(125),
            false,
            "/akis/esp",
            Nav::default(),
            None,
        )
        .into_string();
        assert!(body.contains(">co2<") && body.contains("UP 2D"));
        assert!(!body.contains("okunamadı") && !body.contains(">ram<"));
        assert!(body.contains("--cols:1"));
    }

    #[test]
    fn uptime_format() {
        assert_eq!(uptime(59), "0d 59sn");
        assert_eq!(uptime(3_661), "1s 1d 1sn");
        assert_eq!(uptime(90_061), "1g 1s 1d 1sn");
    }

    #[test]
    fn list_brief_and_clickable_links() {
        let item1 = Value::Map(BTreeMap::from([
            ("title".into(), Value::Text("Haber 1".into())),
            ("link".into(), Value::Text("https://example.com/1".into())),
        ]));
        let item2 = Value::Map(BTreeMap::from([(
            "title".into(),
            Value::Text("Haber 2".into()),
        )]));
        let list = Value::List(vec![item1, item2]);
        assert_eq!(brief_with(&list, None), Some(("2 öğe".into(), Level::Ok)));

        let e = [Entity::new("router", "feed")];
        let v = Values::from([("feed".into(), list)]);
        let html = node_page(
            "News",
            &Sheet::read_only(&e, &v),
            None,
            false,
            "/akis/news",
            Nav::default(),
            None,
        )
        .into_string();
        assert!(html.contains(r#"href="https://example.com/1""#));
        assert!(html.contains(r#"target="_blank""#));
        assert!(html.contains(r#"class="item-link""#));
        assert!(html.contains(">Haber 1<"));
        assert!(html.contains(">Haber 2<"));

        let t = Tile {
            masked: false,
            admin: false,
            id: "hn",
            label: "Hacker News",
            presence: Presence::Up(Some(10)),
            values: vec![("feed", &v["feed"])],
            presenter: None,
        };
        let tile_html = tile(&t).into_string();
        assert!(tile_html.contains(r#"class="tile-list""#));
        assert!(tile_html.contains(r#"class="tile-item""#));
        assert!(tile_html.contains(">Haber 1<"));
        assert!(html.contains(r#"title="Haber 1""#));
        assert!(html.contains(r#"class="item-uri""#));
        assert!(html.contains(r#"title="https://example.com/1""#));
        assert!(html.contains("↗ example.com"));
    }
}
