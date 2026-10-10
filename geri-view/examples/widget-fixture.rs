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
