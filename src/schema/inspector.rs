use super::naming::{NamingRegistry};
use super::observer::SchemaObserver;
use super::stats::ColumnStats;
use super::type_tracker::{InferredType, PgType, TypeTracker};

fn make_stat(table_name: &str, column_name: String, pg_type: PgType, tracker: &TypeTracker) -> ColumnStats {
    ColumnStats {
        table_name: table_name.to_string(),
        column_name,
        pg_type,
        total_count: tracker.total_count,
        null_count: tracker.null_count,
        type_histogram: type_histogram(tracker),
    }
}

/// Collect type distribution statistics for every data column (excluding j2s_ generated columns).
/// Call after `finalize()` — all table names must already be registered in `naming`.
fn collect_entry_stats(entry: &super::observer::TableEntry, table_name: &str) -> Vec<ColumnStats> {
    let mut stats = Vec::new();
    for (original_field, tracker) in &entry.columns {
        if tracker.is_object_field() || tracker.is_array_field() { continue; }
        stats.push(make_stat(table_name, NamingRegistry::column_name(original_field), tracker.to_pg_type(), tracker));
    }
    if let Some(ref tracker) = entry.scalar_tracker {
        stats.push(make_stat(table_name, "value".to_string(), tracker.to_pg_type(), tracker));
    }
    for (original_field, tracker) in &entry.array_columns {
        let pg_type = PgType::Array(Box::new(tracker.to_pg_type()));
        stats.push(make_stat(table_name, NamingRegistry::column_name(original_field), pg_type, tracker));
    }
    stats
}

pub fn collect_stats(observer: &SchemaObserver, naming: &mut NamingRegistry) -> Vec<ColumnStats> {
    let mut result = Vec::new();
    for entry in observer.tables.values() {
        let table_name = naming.table_name_from_dot_key(&entry.path_key);
        result.extend(collect_entry_stats(entry, &table_name));
    }
    result.sort_by(|a, b| a.table_name.cmp(&b.table_name).then(a.column_name.cmp(&b.column_name)));
    result
}

fn type_histogram(tracker: &TypeTracker) -> Vec<(String, u64)> {
    let mut hist: Vec<(String, u64)> = tracker
        .iter_types()
        .filter(|(t, _)| !matches!(t, InferredType::Object | InferredType::Array))
        .map(|(t, n)| (format!("{:?}", t), n))
        .collect();
    hist.sort_by_key(|b| std::cmp::Reverse(b.1));
    hist
}
