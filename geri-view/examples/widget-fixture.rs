//! F05 tarayıcı regresyonları için gerçek renderer çıktısı; çalışan hub gerekmez.
use geri_proto::v4::Entity;
use geri_proto::{Danger, Shape, Value, Values};
use geri_view::{Commander, Nav, Sheet, node_page};

fn main() {
    let entities = vec![
        Entity::new("fixture", "items").control(Shape::item(Shape::Trigger), Danger::Destructive),
        Entity::new("fixture", "level").control(
            Shape::Range {
                min: 0.0,
                max: 100.0,
                step: Some(1.0),
                unit: None,
            },
            Danger::Risky,
        ),
        Entity::new("fixture", "safe").control(Shape::Trigger, Danger::Safe),
        Entity::new("fixture", "cpu")
            .label("CPU kullanımı")
            .kind("gauge")
            .group("Sistem"),
        Entity::new("fixture", "ram")
            .label("RAM kullanımı")
            .kind("gauge")
            .group("Sistem"),
        Entity::new("fixture", "power")
            .label("güç")
            .kind("text")
            .group("Sistem"),
        Entity::new("fixture", "job").label("iş").kind("text"),
        Entity::new("fixture", "status").label("durum").kind("text").group("modeller"),
        Entity::new("fixture", "keys").label("anahtarlar").kind("text").group("modeller"),
        Entity::new("fixture", "config")
            .label("ayar")
            .group("modeller")
            .control(Shape::code(2048, Some("toml")), Danger::Risky),
    ];
    let values = Values::from([
        (
            "items".into(),
            Value::List(
                ["a", "b"]
                    .into_iter()
                    .map(|id| {
                        Value::Map(
                            [
                                ("id".into(), Value::Text(id.into())),
                                ("title".into(), Value::Text(id.into())),
                            ]
                            .into(),
                        )
                    })
                    .collect(),
            ),
        ),
        ("level".into(), Value::Uint(7)),
        ("cpu".into(), Value::Float(42.5)),
        ("ram".into(), Value::Float(71.0)),
        ("power".into(), Value::Text("running".into())),
        ("status".into(), Value::Text("✓ 6 rol · 4 API, 2 abonelik".into())),
        ("keys".into(), Value::Text("OpenRouter ✓ …1f47 · $0.22 / $50 · Anthropic yok".into())),
        ("job".into(), Value::Text("circle-packing · nesil 12/20 · en iyi 2.493".into())),
        (
            "config".into(),
            Value::Text(
                "# rol başına model\n[roles]\nevolve  = \"abonelik\"\nreflect = \"openrouter/openai/gpt-6-luna-pro\"\nbatch   = \"openrouter/meta/muse-spark-1.3-contributor\"\n\n[limits]\nmax_usd = 2.5\nfallback = true\n".into(),
            ),
        ),
    ]);
    let sheet = Sheet {
        entities: &entities,
        values: &values,
        commands: &[],
        commander: Some(Commander {
            endpoint: "/commands",
            live: true,
        }),
        presenter: None,
    };
    let page = node_page("F05", &sheet, None, false, "", Nav::default(), None).into_string();
    std::fs::write(std::env::args().nth(1).expect("çıktı HTML yolu"), page).unwrap();
}
