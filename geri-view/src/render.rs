use std::collections::BTreeMap;

use geri_proto::v4::Entity;
use geri_proto::{Danger, Shape, Value};
use maud::{Markup, html};

use crate::{Level, SHOW_MAX, Values, clip_chars, show, widget};

pub const MAX_COLS: u8 = 4;
const MODULE_ROWS: u8 = 6;
pub(crate) const MAX_VISIBLE_CONTROLS: usize = 5;
/// Kod kutusu kartı uzun çizer.
const CODE_ROWS: u8 = 5;
pub(crate) const MAX_ITEMS: usize = 8;
pub const UPTIME: &str = "uptime";

pub struct Sheet<'a> {
    pub entities: &'a [Entity],
    pub values: &'a Values,
    pub commands: &'a [Cmd<'a>],
    pub commander: Option<Commander<'a>>,
    pub presenter: Option<&'a dyn Presenter>,
}

impl<'a> Sheet<'a> {
    pub fn read_only(entities: &'a [Entity], values: &'a Values) -> Self {
        Self {
            entities,
            values,
            commands: &[],
            commander: None,
            presenter: None,
        }
    }

    pub fn with_presenter(mut self, presenter: &'a dyn Presenter) -> Self {
        self.presenter = Some(presenter);
        self
    }

    pub(crate) fn command(&self, name: &str) -> Option<&Cmd<'a>> {
        self.commands.iter().find(|c| c.name == name)
    }
}

#[derive(Clone, Copy)]
pub struct Commander<'a> {
    pub endpoint: &'a str,
    pub live: bool,
}

pub struct Cmd<'a> {
    pub name: &'a str,
    pub desired: &'a Value,
    pub state: CmdState<'a>,
    pub ago: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CmdState<'a> {
    Sent,
    Applying,
    Done,
    Failed { code: &'a str, message: &'a str },
    TimedOut,
    Drift,
    Lost,
}

impl CmdState<'_> {
    pub fn pending(self) -> bool {
        matches!(self, Self::Sent | Self::Applying)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Footprint {
    pub w: u8,
    pub h: u8,
}

pub(crate) struct Cell {
    pub markup: Markup,
    pub size: Footprint,
}

pub struct PresentedBody {
    pub accent: &'static str,
    pub level: Level,
    pub badge: String,
    pub markup: Markup,
    pub rows: u8,
    pub wide: bool,
}

#[derive(Default)]
pub struct PresentationState {
    counters: BTreeMap<String, usize>,
}

impl PresentationState {
    pub fn next(&mut self, key: &str) -> usize {
        let counter = self.counters.entry(key.into()).or_default();
        let index = *counter;
        *counter += 1;
        index
    }
}

pub trait Presenter {
    fn body(
        &self,
        _entity: &Entity,
        _value: &Value,
        _state: &mut PresentationState,
    ) -> Option<PresentedBody> {
        None
    }

    fn brief(&self, _value: &Value) -> Option<Option<(String, Level)>> {
        None
    }

    fn seconds(&self, _value: &Value) -> Option<u64> {
        None
    }
}

pub(crate) fn cells(sheet: &Sheet) -> Vec<Cell> {
    let mut groups: Vec<(&str, Vec<&Entity>)> = Vec::new();
    let mut indices = BTreeMap::new();
    for e in sheet.entities {
        if e.entity_id == UPTIME
            && sheet
                .values
                .get(UPTIME)
                .and_then(|v| sheet.presenter.and_then(|p| p.seconds(v)))
                .is_some()
        {
            continue;
        }
        let shown = sheet.values.contains_key(&e.entity_id)
            || (sheet.commander.is_some() && e.control.is_some());
        if !shown {
            continue;
        }
        let Some(key) = e.group.as_deref() else {
            groups.push((&e.entity_id, vec![e]));
            continue;
        };
        if let Some(&index) = indices.get(key) {
            let group: &mut (&str, Vec<&Entity>) = &mut groups[index];
            group.1.push(e);
        } else {
            indices.insert(key, groups.len());
            groups.push((key, vec![e]));
        }
    }
    let mut state = PresentationState::default();
    groups
        .into_iter()
        .map(|(key, members)| panel(sheet, key, &members, &mut state))
        .collect()
}

fn title(e: &Entity) -> &str {
    e.label.as_str()
}

fn panel(sheet: &Sheet, key: &str, members: &[&Entity], state: &mut PresentationState) -> Cell {
    let grouped = members.len() > 1 || members[0].group.is_some();
    let primary = members
        .iter()
        .position(|e| sheet.values.contains_key(&e.entity_id))
        .unwrap_or(0);
    let head = members[primary];
    let body = sheet
        .values
        .get(&head.entity_id)
        .map(|v| {
            if is_image(head) {
                image_body(v, title(head))
            } else {
                sheet
                    .presenter
                    .and_then(|p| p.body(head, v, state))
                    .unwrap_or_else(|| body(v))
            }
        })
        .unwrap_or(PresentedBody {
            accent: "accent-slate",
            level: Level::Ok,
            badge: String::new(),
            markup: html! {},
            rows: 0,
            wide: false,
        });
    let primary_controlled = sheet.commander.is_some() && head.control.is_some();
    let panel_title = if grouped { key } else { title(head) };
    let mut rows = 1 + if primary_controlled {
        0
    } else {
        body.rows + u8::from(grouped)
    };
    let mut wide = body.wide;
    let mut controls = Vec::new();
    let mut lines = Vec::new();
    for (i, e) in members.iter().enumerate() {
        let controlled = sheet.commander.is_some() && e.control.is_some();
        if i != primary
            && !controlled
            && let Some(v) = sheet.values.get(&e.entity_id)
        {
            lines.push((title(e), v));
        }
        if let (Some(c), Some(face)) = (&e.control, sheet.commander) {
            if matches!(
                c.shape,
                Shape::Text { .. } | Shape::Code { .. } | Shape::Item { .. }
            ) {
                wide = true;
            }
            if matches!(c.shape, Shape::Code { .. }) {
                rows += CODE_ROWS;
            }
            controls.push(widget::control(e, c, sheet, face));
        }
    }
    rows += lines.len() as u8;
    let visible = controls.len().min(MAX_VISIBLE_CONTROLS);
    rows += visible as u8 + u8::from(controls.len() > visible);
    controls.sort_by_key(|(danger, _)| *danger);
    let size = Footprint {
        w: if wide { 2 } else { 1 },
        h: if rows > MODULE_ROWS { 2 } else { 1 },
    };
    let level = body.level;
    let markup = html! {
        section class={ "panel " (body.accent) " " (level.class()) } "box-"="square" "shear-"="top" {
            div class="panel-head" {
                span class="title" "is-"="badge" { (panel_title) }
                @if !body.badge.is_empty() { span class="pct" "is-"="badge" { (body.badge) } }
            }
            @if grouped && sheet.values.contains_key(&head.entity_id) && !primary_controlled {
                div class="sub caption" { (title(head)) }
            }
            @if !primary_controlled { (body.markup) }
            @if !lines.is_empty() {
                dl class="lines" {
                    @for (k, v) in &lines {
                        @let text = line(v, sheet.presenter);
                        dt { (k) } dd title=(text) { (text) }
                    }
                }
            }
            @if !controls.is_empty() {
                div class="controls" {
                    @for (i, (danger, m)) in controls.iter().enumerate().take(visible) {
                        @if *danger == Danger::Destructive && (i == 0 || controls[i - 1].0 != Danger::Destructive) {
                            hr class="danger-rule";
                        }
                        (m)
                    }
                    @if controls.len() > visible {
                        details class="more-controls" {
                            summary { "+" (controls.len() - visible) " ayar" }
                            @for (_, m) in controls.iter().skip(visible) { (m) }
                        }
                    }
                }
            }
        }
    };
    Cell { markup, size }
}

fn line(v: &Value, presenter: Option<&dyn Presenter>) -> String {
    crate::brief_clipped(v, presenter, SHOW_MAX).map_or_else(|| show(v), |(text, _)| text)
}

fn body(v: &Value) -> PresentedBody {
    let simple = |text: String, unit: &str| PresentedBody {
        accent: "accent-slate",
        level: Level::Ok,
        badge: String::new(),
        markup: crate::value(&clip_chars(text, SHOW_MAX), unit),
        rows: 2,
        wide: false,
    };
    if v.get("t").and_then(Value::as_str) == Some("gauge")
        && let Some(number) = v.get("v").and_then(Value::as_f64)
    {
        let max = v.get("max").and_then(Value::as_f64);
        let unit = v.get("unit").and_then(Value::as_str).unwrap_or("");
        let pct = max
            .filter(|m| *m > 0.0)
            .map(|m| (number / m * 100.0) as f32);
        return PresentedBody {
            accent: "accent-slate",
            level: pct.map_or(Level::Ok, |p| crate::gauge_level(v, p)),
            badge: pct.map_or(String::new(), |p| format!("{p:.0}%")),
            markup: html! {
                (crate::value(&clip_chars(format!("{number:.1}"), SHOW_MAX), unit))
                (bar(pct.unwrap_or(0.0)))
                @match v.get("sub") {
                    Some(Value::Text(t)) => div class="sub" { (t) },
                    Some(Value::List(lines)) => {
                        @for line in lines.iter().filter_map(Value::as_str) { div class="sub" { (line) } }
                    },
                    _ => div class="sub" { @if let Some(m) = max { "/ " (format!("{m:.1}")) @if !unit.is_empty() { " " (unit) } } },
                }
            },
            rows: 3,
            wide: false,
        };
    }
    match v {
        Value::Bool(b) => simple((if *b { "açık" } else { "kapalı" }).into(), ""),
        Value::Text(t) => simple(t.clone(), ""),
        Value::List(items) => list_body(items),
        Value::Map(m) => PresentedBody {
            wide: m.len() > 6,
            ..simple(show(v), "")
        },
        v => simple(show(v), ""),
    }
}

fn list_body(items: &[Value]) -> PresentedBody {
    PresentedBody {
        accent: "accent-cyan",
        level: Level::Ok,
        badge: format!("{}", items.len()),
        markup: html! {
            ul class="items" {
                @for item in items.iter().take(MAX_ITEMS) {
                    li class="item-row" {
                        @let title = item_title(item);
                        @if let Some(href) = item_link(item) {
                            a class="item-link" href=(href) title=(title) target="_blank" rel="noopener noreferrer" {
                                span class="item-title" { (title) }
                                @if let Some(host) = url_host(href) {
                                    span class="item-uri" title=(href) { "↗ " (host) }
                                }
                            }
                        } @else {
                            span class="item-title" title=(title) { (title) }
                        }
                    }
                }
            }
            @if items.len() > MAX_ITEMS { div class="sub" { "+" (items.len() - MAX_ITEMS) " öğe" } }
        },
        rows: MODULE_ROWS + 1,
        wide: true,
    }
}

pub(crate) fn url_host(url: &str) -> Option<&str> {
    let after_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let host = after_scheme.split(['/', '?', '#']).next()?;
    let host = host.strip_prefix("www.").unwrap_or(host);
    (!host.is_empty()).then_some(host)
}

pub const IMAGE: &str = "image";

pub fn is_image(e: &Entity) -> bool {
    e.kind == IMAGE
}

fn safe_url(url: &str) -> bool {
    url.starts_with("http://")
        || url.starts_with("https://")
        || (url.starts_with('/') && !url.starts_with("//"))
}

pub(crate) fn image_src(v: &Value) -> Option<(&str, Option<&str>)> {
    let (src, alt) = match v {
        Value::Text(t) => (t.as_str(), None),
        v => (
            v.get("src")?.as_str()?,
            v.get("alt").and_then(Value::as_str),
        ),
    };
    (safe_url(src) && !src.starts_with("http://")).then_some((src, alt))
}

fn image_body(v: &Value, fallback_alt: &str) -> PresentedBody {
    let markup = match image_src(v) {
        Some((src, alt)) => html! {
            figure class="image" {
                img src=(src) alt=(alt.unwrap_or(fallback_alt)) loading="lazy" decoding="async" referrerpolicy="no-referrer";
            }
        },
        None => html! { figure class="image" { div class="value off" { "resim yok" } } },
    };
    PresentedBody {
        accent: "accent-slate",
        level: Level::Ok,
        badge: String::new(),
        markup,
        rows: MODULE_ROWS - 1,
        wide: true,
    }
}

pub(crate) fn item_link(item: &Value) -> Option<&str> {
    let url = ["href", "url", "link"]
        .iter()
        .find_map(|k| item.get(k).and_then(Value::as_str))?;
    safe_url(url).then_some(url)
}

pub(crate) fn item_title(item: &Value) -> String {
    let text = ["title", "label", "name", "id"]
        .iter()
        .find_map(|k| item.get(k).and_then(Value::as_str))
        .map_or_else(|| show(item), str::to_owned);
    clip_chars(text, 120)
}

fn bar(pct: f32) -> Markup {
    html! {
        div "is-"="progress" style={ "--progress-value:" (format!("{:.1}", pct.clamp(0.0, 100.0))) } {}
    }
}

fn waste(sizes: &[Footprint], cols: u8) -> usize {
    let cols = usize::from(cols);
    let mut taken: Vec<u8> = Vec::new();
    let (mut row, mut col) = (0usize, 0usize);
    let mut area = 0;
    for s in sizes {
        let (w, h) = (usize::from(s.w).min(cols), usize::from(s.h));
        area += w * h;
        loop {
            if col + w > cols {
                row += 1;
                col = 0;
                continue;
            }
            let free = (row..row + h).all(|r| {
                taken
                    .get(r)
                    .is_none_or(|t| t & (((1 << w) - 1) << col) == 0)
            });
            if free {
                break;
            }
            col += 1;
        }
        for r in row..row + h {
            while taken.len() <= r {
                taken.push(0);
            }
            taken[r] |= ((1 << w) - 1) << col;
        }
        col += w;
    }
    taken.len() * cols - area
}

pub fn columns(sizes: &[Footprint]) -> u8 {
    let widest = sizes.iter().map(|s| s.w).max().unwrap_or(1);
    let total: usize = sizes.iter().map(|s| usize::from(s.w)).sum();
    let cap = (total.clamp(1, usize::from(MAX_COLS)) as u8).max(widest);
    let least = widest.max(cap.min(2));
    (least..=cap)
        .rev()
        .min_by_key(|c| waste(sizes, *c))
        .unwrap_or(1)
}

#[derive(Default)]
pub struct Layout {
    sizes: Vec<Footprint>,
    cols: u8,
}
impl Layout {
    fn columns(&mut self, sizes: &[Footprint]) -> u8 {
        if self.cols == 0 || self.sizes != sizes {
            self.cols = columns(sizes);
            self.sizes.clear();
            self.sizes.extend_from_slice(sizes);
        }
        self.cols
    }
}
pub(crate) fn grid_cached(id: &str, cells: &[Cell], layout: Option<&mut Layout>) -> Markup {
    let sizes: Vec<_> = cells.iter().map(|c| c.size).collect();
    let cols = layout.map_or_else(|| columns(&sizes), |layout| layout.columns(&sizes));
    html! { div id=(id) class="grid" style={"--cols:" (cols)} {
        @for c in cells { div class={"cell w" (c.size.w) " h" (c.size.h)} {(c.markup)} }
    } }
}
pub(crate) fn grid(id: &str, cells: &[Cell]) -> Markup {
    grid_cached(id, cells, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: Footprint = Footprint { w: 1, h: 1 };
    const W: Footprint = Footprint { w: 2, h: 1 };
    const T: Footprint = Footprint { w: 1, h: 2 };

    #[test]
    fn grid_simulation_counts_holes_like_css() {
        let panels = [S, S, W, S, S, S];
        assert_eq!(waste(&panels, 4), 1);
        assert_eq!(waste(&panels, 3), 2, "geniş panel ilk satıra sığmaz");
        assert_eq!(columns(&panels), 4);
        assert_eq!(columns(&[S, S, S, S]), 4);
        assert_eq!(columns(&[S, S, S, S, S]), 3, "5 kart: 4+1 yerine 3+2");
        assert_eq!(columns(&[S]), 1);
        assert_eq!(columns(&[W]), 2);
        assert_eq!(columns(&[]), 1);
        assert_eq!(waste(&[T, S, S], 2), 0);
    }

    #[test]
    fn cached_layout_matches_uncached_render_and_reacts_to_changed_shapes() {
        let entities = [
            Entity::new("test", "uptime"),
            Entity::new("test", "a"),
            Entity::new("test", "b"),
        ];
        let mut values = crate::Values::from([("a".into(), Value::Uint(5))]);
        let mut layout = Layout::default();
        for value in [
            Value::Uint(100),
            Value::List(vec![Value::Text("item".into()); 8]),
            Value::Uint(1),
        ] {
            values.insert("a".into(), value);
            let sheet = Sheet::read_only(&entities, &values);
            assert_eq!(
                crate::stats_cached(&sheet, &mut layout).into_string(),
                crate::stats(&sheet).into_string()
            );
        }
    }

    #[test]
    fn group_and_entity_with_the_same_name_are_separate_ordered_panels() {
        let entities = [
            Entity::new("test", "a").group("group"),
            Entity::new("test", "group"),
            Entity::new("test", "b").group("group"),
        ];
        let values = crate::Values::from([
            ("a".into(), Value::Uint(1)),
            ("group".into(), Value::Uint(2)),
            ("b".into(), Value::Uint(3)),
        ]);
        let panels = cells(&Sheet::read_only(&entities, &values));
        assert_eq!(panels.len(), 2);
        assert!(panels[0].markup.0.contains(">3<"));
        assert!(panels[1].markup.0.contains(">2<"));
    }
}

#[cfg(test)]
mod link_tests {
    use super::*;

    #[test]
    fn links_stay_on_http_or_this_site() {
        let item = |u: &str| Value::Map([("url".into(), Value::Text(u.into()))].into());
        assert_eq!(item_link(&item("/n/router")), Some("/n/router"));
        assert_eq!(item_link(&item("https://x.com")), Some("https://x.com"));
        for bad in ["//evil.com", "javascript:alert(1)", "data:text/html,x"] {
            assert_eq!(item_link(&item(bad)), None, "{bad}");
        }
    }
}

#[cfg(test)]
mod image_tests {
    use super::*;
    use geri_proto::Values;

    fn sheet_html(v: Value) -> (String, Footprint) {
        let e = [Entity {
            kind: IMAGE.into(),
            ..Entity::new("router", "cam").label("kapı kamerası")
        }];
        let values = Values::from([("cam".into(), v)]);
        let sheet = Sheet::read_only(&e, &values);
        let cell = cells(&sheet).remove(0);
        (cell.markup.into_string(), cell.size)
    }

    #[test]
    fn image_is_a_fixed_two_module_panel_from_safe_sources() {
        let (html, size) = sheet_html(Value::Text("https://x.com/a.jpg".into()));
        assert!(html.contains(r#"src="https://x.com/a.jpg""#), "{html}");
        assert!(html.contains(r#"alt="kapı kamerası""#) && html.contains("no-referrer"));
        assert_eq!(size, Footprint { w: 2, h: 1 });
        let map = Value::Map(
            [
                ("src".into(), Value::Text("/r2/cam.jpg".into())),
                ("alt".into(), Value::Text("ön kapı".into())),
            ]
            .into(),
        );
        let (html, _) = sheet_html(map);
        assert!(html.contains(r#"src="/r2/cam.jpg""#) && html.contains(r#"alt="ön kapı""#));
        for bad in [
            "http://x.com/a.jpg",
            "//evil.com/a.jpg",
            "javascript:alert(1)",
            "data:image/png;base64,AA",
        ] {
            let (html, size) = sheet_html(Value::Text(bad.into()));
            assert!(!html.contains("<img"), "{bad}");
            assert_eq!(size, Footprint { w: 2, h: 1 });
        }
    }
}
