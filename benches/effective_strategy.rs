//! Benchmark for `TableSchema::effective_strategy()` in the Pass 2 hot path.
//!
//! `effective_strategy()` is called once per JSON object during Pass 2 traversal/insert
//! (see `src/pass2/traversal.rs`, `src/pass2/insert.rs`). For `Flatten`/`NormalizeDynamicKeys`
//! overrides, the `prefix`/`id_column` payload is an `Arc<str>`, so building the returned
//! `Cow::Owned(InferredStrategy)` is a refcount bump, not a heap allocation — this benchmark
//! confirms the override scenarios cost the same as the no-override baseline.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use json2sql::schema::table_schema::{InferredStrategy, TableSchema, UserOverride};

fn schema_no_override() -> TableSchema {
    TableSchema::new("products".to_string(), vec!["products".to_string()], 0)
}

fn schema_ui_override_flatten() -> TableSchema {
    let mut s = TableSchema::new("products_nutrients".to_string(), vec!["products".to_string(), "nutrients".to_string()], 1);
    s.set_ui_override(Some(UserOverride::Flatten {
        prefix: "nutrients_".into(),
        max_depth: 1,
    }));
    s
}

fn schema_ui_override_normalize_dynamic_keys() -> TableSchema {
    let mut s = TableSchema::new("images".to_string(), vec!["images".to_string()], 1);
    s.set_ui_override(Some(UserOverride::NormalizeDynamicKeys {
        id_column: "image_id".into(),
    }));
    s
}

fn schema_toml_override_normalize_dynamic_keys() -> TableSchema {
    let mut s = TableSchema::new("images".to_string(), vec!["images".to_string()], 1);
    s.set_toml_override(Some(UserOverride::NormalizeDynamicKeys {
        id_column: "image_id".into(),
    }));
    s
}

/// Simulates the Pass 2 hot path: `effective_strategy()` called once per row.
fn call_per_row(schema: &TableSchema, rows: u64) {
    for _ in 0..rows {
        let eff = black_box(schema.effective_strategy());
        black_box(matches!(*eff, InferredStrategy::Ignore));
    }
}

fn bench_effective_strategy(c: &mut Criterion) {
    let mut group = c.benchmark_group("effective_strategy_1k_rows");

    let no_override = schema_no_override();
    group.bench_function("no_override_columns", |b| {
        b.iter(|| call_per_row(&no_override, 1_000));
    });

    let ui_flatten = schema_ui_override_flatten();
    group.bench_function("ui_override_flatten", |b| {
        b.iter(|| call_per_row(&ui_flatten, 1_000));
    });

    let ui_normalize = schema_ui_override_normalize_dynamic_keys();
    group.bench_function("ui_override_normalize_dynamic_keys", |b| {
        b.iter(|| call_per_row(&ui_normalize, 1_000));
    });

    let toml_normalize = schema_toml_override_normalize_dynamic_keys();
    group.bench_function("toml_override_normalize_dynamic_keys", |b| {
        b.iter(|| call_per_row(&toml_normalize, 1_000));
    });

    group.finish();
}

criterion_group!(benches, bench_effective_strategy);
criterion_main!(benches);
