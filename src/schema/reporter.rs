use super::naming::{NamingRegistry};
use super::observer::SchemaObserver;
use super::stats::ColumnStats;
use super::type_tracker::{InferredType, PgType, TypeTracker};

/// Collect type distribution statistics for every data column (excluding j2s_ generated columns).
/// Call after `finalize()` — all table names must already be registered in `naming`.
pub fn collect_stats(observer: &SchemaObserver, naming: &mut NamingRegistry) -> Vec<ColumnStats> {
    let mut result = Vec::new();

    for entry in observer.tables.values() {
        let table_name = naming.table_name_from_dot_key(&entry.path_key);

        for (original_field, tracker) in &entry.columns {
            if tracker.is_object_field() || tracker.is_array_field() {
                continue;
            }
            let col_name = NamingRegistry::column_name(original_field);
            result.push(ColumnStats {
                table_name: table_name.clone(),
                column_name: col_name,
                pg_type: tracker.to_pg_type(),
                total_count: tracker.total_count,
                null_count: tracker.null_count,
                type_histogram: type_histogram(tracker),
            });
        }

        if let Some(ref tracker) = entry.scalar_tracker {
            result.push(ColumnStats {
                table_name: table_name.clone(),
                column_name: "value".to_string(),
                pg_type: tracker.to_pg_type(),
                total_count: tracker.total_count,
                null_count: tracker.null_count,
                type_histogram: type_histogram(tracker),
            });
        }

        for (original_field, tracker) in &entry.array_columns {
            let col_name = NamingRegistry::column_name(original_field);
            let elem_type = tracker.to_pg_type();
            result.push(ColumnStats {
                table_name: table_name.clone(),
                column_name: col_name,
                pg_type: PgType::Array(Box::new(elem_type)),
                total_count: tracker.total_count,
                null_count: tracker.null_count,
                type_histogram: type_histogram(tracker),
            });
        }
    }

    result.sort_by(|a, b| {
        a.table_name
            .cmp(&b.table_name)
            .then(a.column_name.cmp(&b.column_name))
    });
    result
}

/// Return anomaly info for reporting: (table_path_key, col_original_name, TypeTracker).
pub fn anomaly_iter(observer: &SchemaObserver) -> impl Iterator<Item = (&str, &str, &TypeTracker)> {
    observer.tables.values().flat_map(|entry| {
        entry.columns.iter().filter_map(|(field, tracker)| {
            if tracker.has_anomalies() {
                Some((entry.path_key.as_str(), field.as_str(), tracker))
            } else {
                None
            }
        })
    })
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
