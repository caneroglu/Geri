use geri_proto::v4::Entity;
use geri_proto::{Value, Values};
use geri_view::{Layout, Sheet, stats_cached};
use std::{hint::black_box, time::Instant};

fn main() {
    let entities: Vec<_> = (0..32)
        .map(|i| Entity::new("bench", format!("metric-{i}")))
        .collect();
    let values: Values = entities
        .iter()
        .map(|e| (e.entity_id.clone(), Value::Uint(42)))
        .collect();
    let sheet = Sheet::read_only(&entities, &values);
    let mut layout = Layout::default();
    if let Some(path) = std::env::args().nth(1) {
        std::fs::write(path, stats_cached(&sheet, &mut layout).into_string()).unwrap();
    }
    let start = Instant::now();
    let mut bytes = 0;
    for _ in 0..10_000 {
        bytes += black_box(stats_cached(black_box(&sheet), &mut layout).into_string()).len();
    }
    println!(
        "{} ns/render, {} bytes/render",
        start.elapsed().as_nanos() / 10_000,
        bytes / 10_000
    );
}
