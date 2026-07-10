//! Phase 3 — décision de la stratégie wide-table, par table.
//!
//! - fn `apply_wide_table_strategies` — applique la stratégie choisie à chaque table éligible.
//! - fn `apply_wide_strategy` — décide Columns/`StructuredPivot`/Pivot/Jsonb/`AutoSplit` selon le
//!   ratio de colonnes stables.
//! - fn `apply_non_autosplit_strategy` — branche `StructuredPivot`, Jsonb, ou le split
//!   identité/compagnon (Pivot).
//! - fn `apply_pivot_split` — split identité/compagnon à rétention zéro (toutes les clés
//!   migrent vers le compagnon `_pivot`), déclenché quand `apply_non_autosplit_strategy`
//!   choisirait Pivot — que la table soit `Object`- ou `ObjectArray`-parent.
//! - fn `unique_pivot_name` — nom du compagnon `_pivot`, unique contre toutes les tables déjà
//!   connues (fallback `_pivot_eav` sur auto-collision, `_pivot_2`/`_pivot_3`/... sur collision
//!   externe).
//! - fn `apply_autosplit_strategy` — construit la table EAV compagnon `_wide` (racine + vrai
//!   enfant `Object` — partition par fréquence, inchangé).
//! - fn `build_wide_pivot_schema` — construit la table EAV compagnon (réutilisée par
//!   `apply_autosplit_strategy` et `apply_pivot_split`, qui diffèrent seulement par la
//!   politique de rétention et le jeu de clés envoyé en compagnon).
//! - fn `collect_medium_keys` — sélectionne les clés de fréquence moyenne (`AutoSplit` racine
//!   uniquement).
//! - fn `infer_medium_value_type` — détermine le type de valeur commun d'un jeu de clés.
//! - fn `build_finalizer_config` — assemble la config figée passée à ces fonctions.
//! - struct `FinalizerConfig` — config figée (seuils + stratégies désactivées) pour cette phase.

use indexmap::IndexMap;

use super::super::naming::NamingRegistry;
use super::super::observer::TableEntry;
use super::super::strategies::StrategyName;
use super::super::suffix_detector::detect_suffix_schema;
use super::super::table_schema::{ChildKind, ColumnSchema, TableSchema, InferredStrategy};
use super::super::type_tracker::{widen_pg_types, PgType};
use super::super::wide_strategies::{apply_structured_pivot_columns, apply_wide_strategy_columns, suggest_wide_strategy};
use super::SchemaFinalizer;

/// Phase 3: apply per-table wide strategies (Pivot, Jsonb, `StructuredPivot`, `AutoSplit`)
/// to all remaining tables after fusion is complete.
pub(super) fn apply_wide_table_strategies(
    schemas: &mut Vec<TableSchema>,
    tables: &IndexMap<String, TableEntry>,
    naming: &NamingRegistry,
    config: &FinalizerConfig,
    tables_with_object_children: &std::collections::HashSet<String>,
) {
    let mut extra_schemas: Vec<TableSchema> = Vec::new();
    let schema_map: std::collections::HashMap<String, usize> =
        schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
    // Toutes les tables déjà nommées à ce stade — sert à garantir l'unicité des noms de
    // compagnon `_pivot` synthétiques (mis à jour au fil des créations pour couvrir aussi
    // les collisions entre deux compagnons créés dans le même passage).
    let mut existing_names: std::collections::HashSet<String> =
        schemas.iter().map(|s| s.name.clone()).collect();

    for (path_key, entry) in tables {
        let pg_name = naming.table_name_lookup_from_dot_key(path_key);
        if let Some(&idx) = schema_map.get(&pg_name) {
            // [issue #45, finding 6] `tables` liste les chemins PRÉ-fusion (Phase 1) ; un
            // chemin pré-fusion peut se résoudre, via NamingRegistry, vers une table que la
            // Phase 2 (finalize_cascading) a déjà fusionnée (SiblingCollapse/SiblingCollapseMulti/
            // ...). Retraiter cette table avec le TableEntry pré-fusion associé au chemin corrompt
            // la table déjà fusionnée (colonnes/medium_keys issus d'une autre observation) —
            // confirmé empiriquement en production (medium_keys peuplé de colonnes étrangères).
            if !matches!(schemas[idx].inferred_strategy, InferredStrategy::Columns) {
                continue;
            }
            if let Some(extra) = apply_wide_strategy(&mut schemas[idx], entry, config, tables_with_object_children, &existing_names) {
                existing_names.insert(extra.name.clone());
                extra_schemas.push(extra);
            }
        }
    }
    schemas.extend(extra_schemas);
}

pub(super) fn build_finalizer_config(finalizer: &SchemaFinalizer, text_threshold: u32) -> FinalizerConfig {
    FinalizerConfig {
        wide_column_threshold: finalizer.wide_column_threshold,
        stable_threshold: finalizer.stable_threshold,
        rare_threshold: finalizer.rare_threshold,
        text_threshold,
        disable_pivot: finalizer.disabled_strategies.contains(&StrategyName::Pivot),
        disable_structured_pivot: finalizer.disabled_strategies.contains(&StrategyName::StructuredPivot),
    }
}

pub(super) struct FinalizerConfig {
    wide_column_threshold: usize,
    stable_threshold: f64,
    rare_threshold: f64,
    text_threshold: u32,
    disable_pivot: bool,
    disable_structured_pivot: bool,
}

/// Apply a wide-table strategy to `schema` if the column count exceeds the threshold.
///
/// Eligible for Object and `ObjectArray` children (not `ScalarArray`, which only ever has
/// a single fixed `value` column and can never be "wide").
/// Returns a companion `_wide` table if the `AutoSplit` strategy is chosen.
fn apply_wide_strategy(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    tables_with_object_children: &std::collections::HashSet<String>,
    existing_names: &std::collections::HashSet<String>,
) -> Option<TableSchema> {
    let is_wide_eligible = matches!(entry.child_kind, Some(ChildKind::Object | ChildKind::ObjectArray) | None);
    let data_col_count = schema.data_columns().count();

    if !is_wide_eligible || data_col_count <= config.wide_column_threshold {
        return None;
    }

    let row_count = entry.row_count.max(1) as f64;
    let stable_count = entry
        .columns
        .values()
        .filter(|t| !t.is_object_field() && !t.is_array_field())
        .filter(|t| t.total_count as f64 / row_count >= config.stable_threshold)
        .count();
    let ratio_stable = stable_count as f64 / data_col_count as f64;

    let is_root = entry.parent_key.is_empty();
    let has_object_children = tables_with_object_children.contains(&entry.path_key);

    if ratio_stable > WIDE_TABLE_HIGH_STABLE_RATIO && entry.row_count >= 10 {
        eprintln!(
            "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: Columns \
            (high stable ratio — legitimate schema, not key explosion)",
            schema.name, data_col_count, ratio_stable * 100.0
        );
        return None;
    }
    if is_root && has_object_children {
        return Some(apply_autosplit_strategy(schema, entry, config, row_count, data_col_count, ratio_stable));
    }

    apply_non_autosplit_strategy(schema, entry, config, data_col_count, ratio_stable, existing_names)
}

/// Chooses `StructuredPivot`, Jsonb, or the identity/companion split (Pivot) for a table that
/// is not `is_root && has_object_children` — i.e. every wide-eligible table except the P5
/// `AutoSplit` root case, which `apply_autosplit_strategy` handles separately and unchanged.
fn apply_non_autosplit_strategy(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    data_col_count: usize,
    ratio_stable: f64,
    existing_names: &std::collections::HashSet<String>,
) -> Option<TableSchema> {
    let suffix_schema = if config.disable_structured_pivot {
        None
    } else {
        detect_suffix_schema(&entry.columns, SUFFIX_MIN_COVERAGE, config.text_threshold)
    };
    if let Some(suffix_schema) = suffix_schema {
        eprintln!(
            "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: StructuredPivot ({} suffixes)",
            schema.name, data_col_count, ratio_stable * 100.0, suffix_schema.suffix_cols.len()
        );
        apply_structured_pivot_columns(schema, suffix_schema);
        return None;
    }

    let strategy = if config.disable_pivot { InferredStrategy::Jsonb } else { suggest_wide_strategy(entry) };
    if strategy == InferredStrategy::Pivot {
        eprintln!(
            "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: Pivot \
            (split identité/compagnon)",
            schema.name, data_col_count, ratio_stable * 100.0
        );
        return Some(apply_pivot_split(schema, entry, config, existing_names));
    }

    eprintln!(
        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: {:?}",
        schema.name, data_col_count, ratio_stable * 100.0, strategy
    );
    apply_wide_strategy_columns(schema, strategy);
    None
}

/// Split a table that would otherwise be flat-Pivoted into an identity table (zero data
/// columns, unconditional retention, keeps the original name) and a companion `_pivot` EAV
/// table holding every key — no frequency-based retention or dropping, unlike P5 `AutoSplit`.
///
/// Applies uniformly whether `schema` is `Object`- or `ObjectArray`-parent: a single rule
/// avoids the conditional-split trap (detecting "does this table have a real child" is itself
/// a bug surface, cf. issue #45) and reuses the already-correct `AutoSplit` write path
/// (`recurse_children`) for any real children of `schema` instead of `insert_pivot_object`,
/// which never recursed into them.
fn apply_pivot_split(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    existing_names: &std::collections::HashSet<String>,
) -> TableSchema {
    let all_keys: std::collections::HashSet<String> =
        schema.data_columns().map(|c| c.original_name.clone()).collect();
    schema.columns.retain(|c| c.is_generated);
    let wide_name = unique_pivot_name(&schema.name, existing_names);
    let value_type = infer_medium_value_type(entry, &all_keys);
    build_wide_pivot_schema(schema, wide_name, value_type, all_keys, config)
}

/// Companion name for `apply_pivot_split`: `{base}_pivot`, falling back to `{base}_pivot_eav`
/// on self-collision (schema already named `..._pivot`) and to an incremental `_pivot_2`,
/// `_pivot_3`, ... on collision with any other already-known table name.
fn unique_pivot_name(schema_name: &str, existing_names: &std::collections::HashSet<String>) -> String {
    let base_name = schema_name.strip_suffix("_pivot").unwrap_or(schema_name);
    let candidate = format!("{base_name}_pivot");
    let mut candidate = if candidate == schema_name { format!("{base_name}_pivot_eav") } else { candidate };
    let mut suffix_n = 2;
    while existing_names.contains(&candidate) {
        candidate = format!("{base_name}_pivot_{suffix_n}");
        suffix_n += 1;
    }
    candidate
}

/// Apply the P5 `AutoSplit` strategy: retain stable columns on the main table,
/// build a companion `_wide` EAV table for medium-frequency keys.
fn collect_medium_keys(
    entry: &TableEntry,
    row_count: f64,
    rare_threshold: f64,
    stable_threshold: f64,
) -> std::collections::HashSet<String> {
    entry.columns.iter()
        .filter(|(_, t)| !t.is_object_field() && !t.is_array_field())
        .filter(|(_, t)| {
            let freq = t.total_count as f64 / row_count;
            freq >= rare_threshold && freq < stable_threshold
        })
        .map(|(k, _)| k.clone())
        .collect()
}

fn infer_medium_value_type(
    entry: &TableEntry,
    medium_keys: &std::collections::HashSet<String>,
) -> PgType {
    medium_keys.iter()
        .filter_map(|k| entry.columns.get(k))
        .fold(None::<PgType>, |acc, t| {
            Some(acc.map_or_else(|| t.to_pg_type(), |a| widen_pg_types(a, &t.to_pg_type())))
        })
        .unwrap_or(PgType::Text)
}

fn apply_autosplit_strategy(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    row_count: f64,
    data_col_count: usize,
    ratio_stable: f64,
) -> TableSchema {
    let medium_keys = collect_medium_keys(entry, row_count, config.rare_threshold, config.stable_threshold);
    schema.columns.retain(|c| {
        c.is_generated || entry.columns.get(&c.original_name)
            .is_some_and(|t| t.total_count as f64 / row_count >= config.stable_threshold)
    });
    let stable_col_count = schema.data_columns().count();
    let rare_count = data_col_count.saturating_sub(stable_col_count).saturating_sub(medium_keys.len());
    // Strip any existing `_wide` suffix to avoid `foo_wide_wide`; fall back to `_eav` on collision.
    let base_name = schema.name.strip_suffix("_wide").unwrap_or(&schema.name);
    let wide_candidate = format!("{base_name}_wide");
    let wide_name = if wide_candidate == schema.name { format!("{base_name}_eav") } else { wide_candidate };
    eprintln!(
        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: AutoSplit \
        ({} stable cols, {} medium → {}, {} rare dropped)",
        schema.name, data_col_count, ratio_stable * 100.0,
        stable_col_count, medium_keys.len(), wide_name, rare_count,
    );
    let value_type = infer_medium_value_type(entry, &medium_keys);
    build_wide_pivot_schema(schema, wide_name, value_type, medium_keys, config)
}

fn build_wide_pivot_schema(
    schema: &mut TableSchema,
    wide_name: String,
    value_type: PgType,
    medium_keys: std::collections::HashSet<String>,
    config: &FinalizerConfig,
) -> TableSchema {
    let mut wide_schema = TableSchema::new(wide_name.clone(), vec![wide_name.clone()], schema.depth + 1);
    wide_schema.parent_table = Some(schema.name.clone());
    wide_schema.child_kind = Some(ChildKind::Object);
    wide_schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    wide_schema.columns.push(ColumnSchema::parent_fk(&schema.name));
    wide_schema.columns.push(ColumnSchema {
        name: "key".to_string(), original_name: "key".to_string(),
        pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false,
    });
    wide_schema.columns.push(ColumnSchema {
        name: "value".to_string(), original_name: "value".to_string(),
        pg_type: value_type, not_null: false, is_generated: false, is_parent_fk: false,
    });
    wide_schema.inferred_strategy = InferredStrategy::Pivot;
    schema.inferred_strategy = InferredStrategy::AutoSplit {
        stable_threshold: config.stable_threshold,
        rare_threshold: config.rare_threshold,
        medium_keys,
        wide_table_name: wide_name,
    };
    wide_schema
}

/// A wide table with this fraction or more of stable columns is kept as-is (Columns strategy).
/// Below this, the table is split or pivoted.
const WIDE_TABLE_HIGH_STABLE_RATIO: f64 = 0.5;

/// Minimum fraction of columns that must share a common suffix pattern to trigger `StructuredPivot`.
const SUFFIX_MIN_COVERAGE: f64 = 0.3;

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use serde_json::Value;

    use crate::schema::naming::NamingRegistry;
    use crate::schema::observer::SchemaObserver;
    use crate::schema::table_schema::{KeyShape, SiblingSchema};
    use crate::schema::type_tracker::PgType;

    fn make_config() -> FinalizerConfig {
        FinalizerConfig {
            wide_column_threshold: 2,
            stable_threshold: 0.1,
            rare_threshold: 0.001,
            text_threshold: 256,
            disable_pivot: false,
            disable_structured_pivot: false,
        }
    }

    // ---------------------------------------------------------------------------
    // [issue #45] Phase 3 ne doit jamais retraiter une table déjà assignée par la Phase 2
    // (finding 6 de l'issue, confirmé empiriquement en production sur OpenFoodFacts :
    // medium_keys d'une table AutoSplit peuplé à partir des colonnes d'une TOUTE AUTRE table).
    //
    // apply_wide_table_strategies boucle sur `tables` — les chemins PRÉ-fusion de la Phase 1.
    // Un chemin pré-fusion peut toujours se résoudre, via NamingRegistry, vers une table que
    // la Phase 2 a déjà fusionnée (SiblingCollapse) — apply_wide_strategy ne doit alors JAMAIS
    // la retraiter avec ce TableEntry, quel qu'il soit.
    // ---------------------------------------------------------------------------
    #[test]
    fn already_assigned_schema_is_never_reprocessed() {
        let mut observer = SchemaObserver::new(256, false);
        let obj: serde_json::Map<String, Value> = serde_json::from_str(
            r#"{"id": 1, "foo": {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}}"#,
        ).unwrap();
        observer.observe_root("root", &obj);

        let mut naming = NamingRegistry::new();
        for path_key in observer.tables.keys() {
            naming.table_name_from_dot_key(path_key);
        }

        // Simule la Phase 2 ayant déjà fusionné "root_foo" en SiblingCollapse — une forme de
        // colonnes différente de ce que le TableEntry brut (5 colonnes scalaires) produirait.
        let mut fused = TableSchema::new("root_foo".to_string(), vec!["root".to_string(), "foo".to_string()], 1);
        fused.parent_table = Some("root".to_string());
        fused.inferred_strategy = InferredStrategy::SiblingCollapse(SiblingSchema {
            key_col_name: "key".to_string(), key_shape: KeyShape::Slug, array_children: false,
        });
        fused.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        fused.columns.push(ColumnSchema::parent_fk("root"));
        // 3 colonnes data > wide_column_threshold(2) — nécessaire pour que apply_wide_strategy
        // dépasse le premier seuil et atteigne réellement le chemin de retraitement.
        for col_name in ["key", "x1", "imgid"] {
            fused.columns.push(ColumnSchema {
                name: col_name.to_string(), original_name: col_name.to_string(),
                pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false,
            });
        }
        let mut schemas = vec![fused];

        apply_wide_table_strategies(
            &mut schemas, &observer.tables, &naming, &make_config(), &std::collections::HashSet::new(),
        );

        assert_eq!(schemas.len(), 1, "aucun compagnon ne doit être créé à partir d'une table déjà fusionnée");
        assert!(
            matches!(schemas[0].inferred_strategy, InferredStrategy::SiblingCollapse(_)),
            "le SiblingCollapse de la Phase 2 doit survivre intact — trouvé : {:?}", schemas[0].inferred_strategy
        );
        assert_eq!(schemas[0].data_columns().count(), 3, "colonnes inchangées (key, x1, imgid)");
    }
}
