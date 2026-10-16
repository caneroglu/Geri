use geri_proto::v4::Entity;
use geri_proto::{Control, Danger, Shape, Value};
use maud::{Markup, html};

use crate::render::{CmdState, Commander, MAX_ITEMS, Sheet, item_title};
use crate::show;

const UNDO_MS: u32 = 3_000;
const HOLD_MS: u32 = 1_500;
const RESULT_SECS: u64 = 120;
const MAX_SEGMENTS: usize = 4;

pub(crate) fn control(e: &Entity, c: &Control, sheet: &Sheet, face: Commander) -> (Danger, Markup) {
    let s = Sig::new(&e.source_id, &e.entity_id);
    let reported = sheet.values.get(&e.entity_id);
    let cmd = sheet.command(&e.entity_id);
    let pending = cmd.is_some_and(|c| c.state.pending());
    let lock = if !face.live {
        Some("düğüm canlı değil")
    } else if pending {
        Some("komut sürüyor")
    } else {
        None
    };
    let send = |payload: &str| {
        format!(
            "${e} = ''; @post({endpoint}, {{payload: {{source_id: {source}, entity_id: {entity}, action: {action}, args: {payload}}}}})",
            e = s.err,
            endpoint = js_str(face.endpoint),
            source = js_str(&e.source_id),
            entity = js_str(&e.entity_id),
            action = js_str(c.shape.action()),
        )
    };
    let current = reported.map_or("null".into(), js);
    let init = match (&c.shape, reported) {
        (Shape::Range { min, .. }, None) => format!("{min}"),
        (Shape::Text { .. } | Shape::Code { .. }, None) => "''".into(),
        (Shape::Choice { options }, None) => options.first().map_or("''".into(), |o| js_str(o)),
        _ => current.clone(),
    };
    let signals = format!("{{{}: {init}, {}: ''}}", s.value, s.err);
    let label = e.label.as_str();
    let changed = format!("${} != {current}", s.value);
    let widget = match &c.shape {
        Shape::Trigger => act(label, "null", &send, c.danger, lock),
        Shape::Bool => {
            let on = matches!(reported, Some(Value::Bool(true)));
            html! {
                span class={ "switch " (if on { "on" } else { "off" }) } { (if on { "AÇIK" } else { "KAPALI" }) }
                (act(if on { "KAPAT" } else { "AÇ" }, if on { "false" } else { "true" }, &send, c.danger, lock))
            }
        }
        Shape::Range {
            min,
            max,
            step,
            unit,
        } => html! {
            input type="number" min=(min) max=(max) step=(step.map_or("any".into(), |s| s.to_string()))
                data-bind=(s.value) disabled[lock.is_some()];
            @if let Some(u) = unit { span class="unit" { (u) } }
            (save(&s, &changed, &current, &send, c.danger, lock))
        },
        Shape::Choice { options } if options.len() <= MAX_SEGMENTS => html! {
            span class="segments" {
                @for o in options {
                    @let here = matches!(reported, Some(Value::Text(t)) if t == o);
                    @if here {
                        span class="seg on" { (o) }
                    } @else {
                        (act(o, &js_str(o), &send, c.danger, lock))
                    }
                }
            }
        },
        Shape::Choice { options } => html! {
            select data-bind=(s.value) disabled[lock.is_some()] {
                @for o in options { option value=(o) { (o) } }
            }
            (save(&s, &changed, &current, &send, c.danger, lock))
        },
        Shape::Text { max } => html! {
            input type="text" maxlength=(max) data-bind=(s.value) disabled[lock.is_some()];
            (save(&s, &changed, &current, &send, c.danger, lock))
        },
        Shape::Code { max, lang } => html! {
            geri-code data-lang=[lang.as_deref()] {
                pre data-ignore-morph aria-hidden="true" { code {} }
                textarea maxlength=(max) rows="1" spellcheck="false" autocapitalize="off" autocomplete="off"
                    aria-label=(label) data-bind=(s.value) disabled[lock.is_some()] {}
            }
            (save_quiet(&s, &changed, &current, &send, c.danger, lock))
        },
        Shape::Item { of } => items(reported, of, c.danger, lock, &send),
        Shape::Unknown => html! { span class="dim" { "salt okunur" } },
    };
    let state = cmd
        .filter(|c| c.state.pending() || c.ago <= RESULT_SECS)
        .map(|c| phase(c.state, c.desired, reported));
    let classes = format!(
        "ctl {}{}{}",
        danger_class(c.danger),
        if pending { " pending" } else { "" },
        if lock.is_some() { " locked" } else { "" }
    );
    let markup = html! {
        div id={ "ctl-" (e.entity_id) } class=(classes) data-signals__ifmissing=(signals) {
            div class="ctl-row" {
                @if !matches!(c.shape, Shape::Trigger | Shape::Item { .. }) { span class="k" { (label) } }
                span class={ "w" @if matches!(c.shape, Shape::Code { .. }) { " block" } } { (widget) }
            }
            @if let Some((class, text)) = state { div class={ "state " (class) } { (text) } }
            @if let Some(why) = lock.filter(|_| !face.live) { div class="state dim" { (why) } }
            div class="state err" data-show={ "$" (s.err) } data-text={ "$" (s.err) } {}
        }
    };
    (c.danger, markup)
}

struct Sig {
    value: String,
    err: String,
}

impl Sig {
    fn new(source: &str, name: &str) -> Self {
        let n = crate::signal_key(source, name);
        Self {
            value: format!("_c_{n}"),
            err: format!("_e_{n}"),
        }
    }
}

fn danger_class(d: Danger) -> &'static str {
    match d {
        Danger::Safe => "safe",
        Danger::Risky => "risky",
        Danger::Destructive => "destructive",
    }
}

fn act(
    label: &str,
    payload: &str,
    send: &dyn Fn(&str) -> String,
    danger: Danger,
    lock: Option<&str>,
) -> Markup {
    let off = lock.is_some();
    match danger {
        Danger::Safe => html! {
            button class="act safe" disabled[off] data-on:click=(send(payload)) { (label) }
        },
        Danger::Risky => {
            let arm = format!("el.closest('geri-action').arm(evt, {payload})");
            html! {
                geri-action data-danger="risky" data-delay=(UNDO_MS) data-on:geri-confirm=(send("evt.detail")) {
                    button class="act risky" disabled[off] data-on:click=(arm) { (label) }
                    button class="act undo" style="display:none" data-on:click="el.closest('geri-action').cancel()" {
                        "GERİ AL · " (label)
                    }
                }
            }
        }
        Danger::Destructive => {
            let arm = format!("el.closest('geri-action').arm(evt, {payload})");
            html! {
                geri-action data-danger="destructive" data-delay=(HOLD_MS) data-on:geri-confirm=(send("evt.detail")) {
                    button class="act destructive" disabled[off] style={ "--hold:" (HOLD_MS) "ms" }
                        data-on:pointerdown=(arm)
                        data-on:pointerleave="el.closest('geri-action').release(evt)"
                        data-on:contextmenu="evt.preventDefault()" {
                        span { (label) " · BASILI TUT" }
                    }
                }
            }
        }
    }
}

fn save(
    s: &Sig,
    changed: &str,
    current: &str,
    send: &dyn Fn(&str) -> String,
    danger: Danger,
    lock: Option<&str>,
) -> Markup {
    let diff = format!("{current} + ' → ' + ${}", s.value);
    save_with(s, changed, current, send, danger, lock, Some(&diff))
}

/// Çok satırlı değerde "eski → yeni" satıra sığmaz; yalnız düğmeler.
fn save_quiet(
    s: &Sig,
    changed: &str,
    current: &str,
    send: &dyn Fn(&str) -> String,
    danger: Danger,
    lock: Option<&str>,
) -> Markup {
    save_with(s, changed, current, send, danger, lock, None)
}

fn save_with(
    s: &Sig,
    changed: &str,
    current: &str,
    send: &dyn Fn(&str) -> String,
    danger: Danger,
    lock: Option<&str>,
    diff: Option<&str>,
) -> Markup {
    html! {
        span class="save" data-show=(changed) style="display:none" {
            @if let Some(diff) = diff { span class="diff" data-text=(diff) {} }
            (act("KAYDET", &format!("${}", s.value), send, danger, lock))
            button class="act" data-on:click={ "el.closest('.save').querySelector('geri-action')?.cancel(); $" (s.value) " = " (current) } { "VAZGEÇ" }
        }
    }
}

fn items(
    reported: Option<&Value>,
    of: &Shape,
    danger: Danger,
    lock: Option<&str>,
    send: &dyn Fn(&str) -> String,
) -> Markup {
    let list: &[Value] = match reported {
        Some(Value::List(l)) => l,
        _ => &[],
    };
    html! {
        ul class="items" {
            @for item in list.iter().take(MAX_ITEMS) {
                @let id = item.get("id").and_then(Value::as_str);
                @let v = item.get("value");
                li {
                    @match (id, of) {
                        (Some(id), Shape::Bool) => {
                            @let on = matches!(v, Some(Value::Bool(true)));
                            @let payload = format!("{{id: {}, value: {}}}", js_str(id), !on);
                            (act(if on { "■" } else { "□" }, &payload, send, danger, lock))
                        }
                        (Some(id), Shape::Trigger) => {
                            @let payload = format!("{{id: {}}}", js_str(id));
                            (act("▸", &payload, send, danger, lock))
                        }
                        _ => {}
                    }
                    span class={ "item" @if matches!(v, Some(Value::Bool(true))) { " done" } } { (item_title(item)) }
                }
            }
        }
        @if list.len() > MAX_ITEMS { div class="sub" { "+" (list.len() - MAX_ITEMS) " öğe" } }
    }
}

pub(crate) fn phase(
    state: CmdState,
    desired: &Value,
    reported: Option<&Value>,
) -> (&'static str, String) {
    let now = reported.map_or("—".into(), short);
    match state {
        CmdState::Sent => ("wait", format!("⧗ GÖNDERİLDİ · {}", short(desired))),
        CmdState::Applying => (
            "wait",
            format!("⧗ UYGULANIYOR · düğüm henüz {now} bildiriyor"),
        ),
        CmdState::Done => ("ok", "✓ TAMAM".into()),
        CmdState::Failed { code, message } => ("bad", format!("✗ {message} ({code})")),
        CmdState::TimedOut => ("bad", "✗ ZAMAN AŞIMI · cevap gelmedi".into()),
        CmdState::Drift => ("bad", format!("✗ SAPMA · düğüm {now} bildiriyor")),
        CmdState::Lost => ("bad", "? BİLİNMİYOR · bağlantı koptu".into()),
    }
}

pub(crate) fn short(v: &Value) -> String {
    match v {
        Value::Null => "—".into(),
        Value::Text(t) => crate::clip_chars(t.clone(), 24),
        Value::Map(m) => m.get("value").map_or_else(|| show(v), short),
        v => crate::clip_chars(show(v), 24),
    }
}

fn js(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Text(t) => js_str(t),
        _ => "null".into(),
    }
}

pub(crate) fn js_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_else(|_| "''".into())
}
