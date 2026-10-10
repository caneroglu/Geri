use crate::{Cmd, CmdState, Presence, Tile};
use geri_proto::v4::{
    CommandAudit, CommandStatus, RecordKind, SourceDeclaration, StoredRecord, Visibility,
};
use geri_proto::{Plain, Value, Values};
use std::collections::BTreeMap;

pub struct SourceView {
    pub declaration: SourceDeclaration,
    pub visibility: Visibility,
    pub created_at_ms: u64,
    pub last_seen_ms: u64,
    pub disconnected: bool,
    pub values: Values,
    pub state: BTreeMap<String, StoredRecord>,
    pub commands: Vec<CommandView>,
    pub events: Vec<EventView>,
    pub layout: [std::cell::RefCell<crate::Layout>; 2],
}

impl SourceView {
    pub fn new(declaration: SourceDeclaration, visibility: Visibility, created_at_ms: u64) -> Self {
        Self {
            declaration,
            visibility,
            created_at_ms,
            last_seen_ms: created_at_ms,
            disconnected: false,
            values: Values::new(),
            state: BTreeMap::new(),
            commands: Vec::new(),
            events: Vec::new(),
            layout: Default::default(),
        }
    }

    pub fn state(&mut self, records: impl IntoIterator<Item = StoredRecord>) {
        let mut current = std::collections::BTreeSet::new();
        for stored in records {
            if stored.record.kind != RecordKind::State {
                continue;
            }
            let id = stored.record.entity_id.clone();
            current.insert(id.clone());
            self.last_seen_ms = self.last_seen_ms.max(stored.accepted_at_ms);
            if self
                .state
                .get(&id)
                .is_some_and(|old| old.cursor == stored.cursor)
            {
                continue;
            }
            self.disconnected = false;
            let value =
                stored
                    .record
                    .data
                    .as_ref()
                    .and_then(|v| serde_json::from_value::<Plain>(v.clone()).ok())
                    .map(|p| p.0)
                    .unwrap_or_else(|| {
                        Value::Text(
                            stored.record.blob.as_ref().map_or("—".into(), |b| {
                                format!("{} bayt · {}", b.size, b.sha256)
                            }),
                        )
                    });
            self.values.insert(id.clone(), value);
            self.state.insert(id, stored);
        }
        self.state.retain(|id, _| {
            if current.contains(id) {
                true
            } else {
                self.values.remove(id);
                false
            }
        });
        self.observe_commands();
    }

    pub fn live(&mut self, values: Values, now_ms: u64) -> bool {
        let presence_changed =
            self.disconnected || now_ms.saturating_sub(self.last_seen_ms) >= 360_000;
        self.last_seen_ms = now_ms;
        self.disconnected = false;
        let changed = values
            .iter()
            .any(|(id, value)| self.values.get(id) != Some(value));
        self.values.extend(values);
        self.observe_commands();
        changed || presence_changed
    }

    pub fn clear_live(&mut self) {
        self.disconnected = true;
        self.values.clear();
        for (id, record) in &self.state {
            let value =
                record
                    .record
                    .data
                    .as_ref()
                    .and_then(|v| serde_json::from_value::<Plain>(v.clone()).ok())
                    .map(|p| p.0)
                    .unwrap_or_else(|| {
                        Value::Text(
                            record.record.blob.as_ref().map_or("—".into(), |b| {
                                format!("{} bayt · {}", b.size, b.sha256)
                            }),
                        )
                    });
            self.values.insert(id.clone(), value);
        }
    }

    pub fn commands(&self, now_ms: u64) -> Vec<Cmd<'_>> {
        self.commands
            .iter()
            .map(|c| Cmd {
                name: &c.audit.command.entity_id,
                desired: &c.desired,
                state: c.state(&self.values),
                ago: now_ms.saturating_sub(c.audit.command.issued_at_ms) / 1000,
            })
            .collect()
    }

    pub fn command_audit(&mut self, audits: impl IntoIterator<Item = CommandAudit>) {
        self.commands = audits
            .into_iter()
            .map(|audit| {
                let confirmed = self.commands.iter().any(|c| {
                    c.audit.command.command_id == audit.command.command_id && c.confirmed.get()
                });
                let view = CommandView::from(audit);
                view.confirmed.set(confirmed);
                view
            })
            .collect();
        self.observe_commands();
    }
    fn observe_commands(&self) {
        for command in &self.commands {
            command.state(&self.values);
        }
    }

    pub fn events(&self, now_ms: u64) -> maud::Markup {
        use maud::html;
        html! { div id="events" {
            @if !self.events.is_empty() {
                section class="panel accent-slate ok log" "box-"="square" "shear-"="top" {
                    div class="panel-head" {span class="title" "is-"="badge" {"son olaylar"} span class="pct" "is-"="badge" {(self.events.len())}}
                    dl class="lines" {
                        @for event in &self.events {
                            @let r = &event.record;
                            @let time = r.record.event_time_ms.unwrap_or(r.accepted_at_ms);
                            dt title={"kabul: " (r.accepted_at_ms) " · olay: " (time)} {(crate::ago_text(now_ms.saturating_sub(time)/1000))}
                            dd {
                                (&r.record.entity_id) " · " (crate::clip_chars(crate::show(&event.value),120))
                                @if r.record.sequence == 0 {" · " span class="dim" {"sırasız"}}
                            }
                        }
                    }
                }
            }
        } }
    }

    pub fn stats(
        &self,
        commander: Option<crate::Commander<'_>>,
        connected: Option<u64>,
        now_ms: u64,
    ) -> maud::Markup {
        let commands = self.commands(now_ms);
        let sheet = crate::Sheet {
            entities: &self.declaration.entities,
            values: &self.values,
            commands: &commands,
            commander,
            presenter: None,
        };
        crate::node_stats_cached(
            &sheet,
            connected,
            &mut self.layout[usize::from(commander.is_some())].borrow_mut(),
        )
    }

    pub fn tile(&self, presence: Presence, admin: bool, public: bool) -> Tile<'_> {
        let masked = !admin && !public && self.visibility != Visibility::Public;
        Tile {
            id: &self.declaration.source_id,
            label: &self.declaration.label,
            presence,
            masked,
            admin,
            values: self
                .declaration
                .entities
                .iter()
                .filter(|e| !masked && !crate::is_image(e))
                .filter_map(|e| {
                    self.values
                        .get(&e.entity_id)
                        .map(|v| (e.entity_id.as_str(), v))
                })
                .collect(),
            presenter: None,
        }
    }
}

pub struct EventView {
    pub record: StoredRecord,
    value: Value,
}
impl From<StoredRecord> for EventView {
    fn from(record: StoredRecord) -> Self {
        let value = record
            .record
            .data
            .as_ref()
            .and_then(|v| serde_json::from_value::<Plain>(v.clone()).ok())
            .map(|p| p.0)
            .unwrap_or_else(|| {
                Value::Text(
                    record
                        .record
                        .blob
                        .as_ref()
                        .map_or("—".into(), |b| format!("{} bayt", b.size)),
                )
            });
        Self { record, value }
    }
}

pub struct CommandView {
    pub audit: CommandAudit,
    desired: Value,
    code: String,
    message: String,
    confirmed: std::cell::Cell<bool>,
}

impl From<CommandAudit> for CommandView {
    fn from(audit: CommandAudit) -> Self {
        let desired = serde_json::from_value::<Plain>(audit.command.args.clone())
            .map(|p| p.0)
            .unwrap_or(Value::Null);
        let detail = audit.result.as_ref().and_then(|v| v.get("result"));
        let code = detail
            .and_then(|v| v.get("code"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("failed")
            .into();
        let message = detail
            .and_then(|v| v.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("komut başarısız")
            .into();
        Self {
            audit,
            desired,
            code,
            message,
            confirmed: std::cell::Cell::new(false),
        }
    }
}

impl CommandView {
    fn state<'a>(&'a self, values: &Values) -> CmdState<'a> {
        match self.audit.status {
            CommandStatus::Queued => CmdState::Sent,
            CommandStatus::Dispatched => CmdState::Applying,
            CommandStatus::Cancelled => CmdState::Failed {
                code: "cancelled",
                message: "kontrol kaldırıldı veya değişti",
            },
            CommandStatus::Done => {
                match self
                    .audit
                    .result
                    .as_ref()
                    .and_then(|v| v.get("ok"))
                    .and_then(serde_json::Value::as_bool)
                {
                    Some(false) => CmdState::Failed {
                        code: &self.code,
                        message: &self.message,
                    },
                    Some(true) if self.audit.command.action == "press" => CmdState::Done,
                    Some(true)
                        if values
                            .get(&self.audit.command.entity_id)
                            .is_some_and(|v| reported(v, &self.desired)) =>
                    {
                        self.confirmed.set(true);
                        CmdState::Done
                    }
                    Some(true) if self.confirmed.get() => CmdState::Drift,
                    Some(true) => CmdState::Applying,
                    None => CmdState::Lost,
                }
            }
            CommandStatus::Unknown => CmdState::Lost,
        }
    }
}

fn reported(value: &Value, desired: &Value) -> bool {
    if let (Some(id), Some(wanted), Value::List(items)) =
        (desired.get("id"), desired.get("value"), value)
    {
        return items.iter().any(|item| {
            item.get("id") == Some(id) && item.get("value").is_some_and(|v| reported(v, wanted))
        });
    }
    value == desired
        || value
            .as_f64()
            .zip(desired.as_f64())
            .is_some_and(|(a, b)| a == b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use geri_proto::v4::{Command, Entity, Record};
    use serde_json::json;

    fn source() -> SourceView {
        SourceView::new(
            SourceDeclaration {
                source_id: "Router.Lab_1".into(),
                label: "router".into(),
                kind: "router".into(),
                entities: vec![Entity::new("Router.Lab_1", "CPU.Load")],
            },
            Visibility::Public,
            1,
        )
    }
    fn record(id: &str, cursor: u64, value: u64) -> StoredRecord {
        StoredRecord {
            record: Record {
                source_id: "Router.Lab_1".into(),
                entity_id: id.into(),
                record_id: format!("r-{cursor}"),
                sequence: cursor,
                kind: RecordKind::State,
                event_time_ms: None,
                content_type: "application/json".into(),
                data: Some(json!(value)),
                indexed: None,
                blob: None,
            },
            cursor,
            accepted_at_ms: cursor * 1000,
        }
    }
    #[test]
    fn live_survives_an_unrelated_write_and_replayed_state() {
        let mut source = source();
        source.state([record("CPU.Load", 1, 20)]);
        source.live(Values::from([("CPU.Load".into(), Value::Uint(73))]), 2000);
        source.state([record("CPU.Load", 1, 20), record("fan", 2, 1)]);
        assert_eq!(source.values["CPU.Load"], Value::Uint(73));
        assert_eq!(source.state["CPU.Load"].record.data, Some(json!(20)));
        source.state([record("CPU.Load", 3, 30), record("fan", 2, 1)]);
        assert_eq!(source.values["CPU.Load"].as_f64(), Some(30.0));
        source.live(Values::from([("CPU.Load".into(), Value::Uint(90))]), 4000);
        source.clear_live();
        assert_eq!(source.values["CPU.Load"].as_f64(), Some(30.0));
        assert!(source.disconnected);
        source.state([record("CPU.Load", 3, 30), record("fan", 2, 1)]);
        assert!(
            source.disconnected,
            "aynı kalıcı kayıt bağlantıyı yeniden açmaz"
        );
        assert!(
            source.live(Values::from([("CPU.Load".into(), Value::Uint(30))]), 5000),
            "aynı değerle geri gelen kaynak da izleyiciyi uyandırır"
        );
        assert!(!source.disconnected);
    }
    #[test]
    fn done_set_waits_for_the_reported_value_but_press_finishes() {
        let audit = CommandAudit {
            command: Command {
                command_id: "cmd-1".into(),
                source_id: "Router.Lab_1".into(),
                entity_id: "fan".into(),
                action: "set".into(),
                args: json!(true),
                issued_by: "operator".into(),
                issued_at_ms: 1000,
            },
            status: CommandStatus::Done,
            dispatched_at_ms: Some(2000),
            result: Some(json!({"ok":true,"result":{}})),
        };
        let mut source = source();
        source.commands.push(audit.into());
        assert_eq!(source.commands(3000)[0].state, CmdState::Applying);
        source.live(Values::from([("fan".into(), Value::Bool(true))]), 3000);
        assert_eq!(source.commands(3000)[0].state, CmdState::Done);
        let audit = source.commands[0].audit.clone();
        source.command_audit([audit]);
        source.live(Values::from([("fan".into(), Value::Bool(false))]), 4000);
        assert_eq!(
            source.commands(4000)[0].state,
            CmdState::Drift,
            "doğrulanmış değer daha sonra değişince kontrol kilitlenmez"
        );
        source.commands[0].audit.command.action = "press".into();
        source.values.clear();
        assert_eq!(source.commands(3000)[0].state, CmdState::Done);
        source.commands[0].audit.result = Some(json!({"ok":false,"result":{}}));
        assert!(matches!(
            source.commands(3000)[0].state,
            CmdState::Failed { .. }
        ));
        source.commands[0].audit.status = CommandStatus::Cancelled;
        source.commands[0].audit.result = None;
        assert_eq!(
            source.commands(4000)[0].state,
            CmdState::Failed {
                code: "cancelled",
                message: "kontrol kaldırıldı veya değişti",
            }
        );
    }
    #[test]
    fn v4_identifiers_have_distinct_script_keys() {
        let keys: std::collections::BTreeSet<_> = [
            ("Source", "a-b"),
            ("Source", "a_b"),
            ("Source", "a.b"),
            ("source", "a.b"),
            ("Other", "a.b"),
        ]
        .into_iter()
        .map(|(s, e)| crate::signal_key(s, e))
        .collect();
        assert_eq!(keys.len(), 5);
        assert!(
            keys.iter()
                .all(|k| k.bytes().all(|b| b.is_ascii_hexdigit() || b == b'_'))
        );
    }
}
