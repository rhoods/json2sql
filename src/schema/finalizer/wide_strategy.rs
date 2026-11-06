//! Phase 3 — décision de la stratégie wide-table, par table.
//!
//! - fn `apply_wide_table_strategies` — applique la stratégie choisie à chaque table éligible.
//! - struct `WideStats` — mesures d'une table wide (lignes, colonnes scalaires, ratio stable).
//! - fn `WideStats::log` — journalise la stratégie choisie pour la table.
//! - fn `stable_ratio` — part des colonnes scalaires présentes dans assez de lignes.
//! - fn `apply_wide_strategy` — garde une table large et stable en Columns ; coupe la racine par fréquence
//!   (`AutoSplit`) et passe une table enfant à `apply_non_autosplit_strategy` (#200).
//! - fn `apply_non_autosplit_strategy` — split d'une table enfant : identité/compagnon (Pivot) si ses valeurs sont
//!   d'un seul type, sinon `AutoSplit` par fréquence. Plus de grille de suffixes automatique (#200, T8).
//! - fn `apply_structured_pivot_split` — la grille de suffixes du TOML (`suffix_columns`) part dans un compagnon
//!   `_pivot` structuré ; le reste (scalaires hors grille, tableaux, enfants) reste sur l'identité (#163).
//! - fn `apply_pivot_split` — split identité/compagnon à rétention zéro (toutes les clés
//!   migrent vers le compagnon `_pivot`), déclenché quand `apply_non_autosplit_strategy`
//!   choisirait Pivot — que la table soit `Object`- ou `ObjectArray`-parent.
//! - fn `unique_companion_name` — nom du compagnon `_pivot` ou `_wide`, unique contre toutes les tables déjà
//!   connues (repli `_{suffixe}_eav` sur auto-collision, `_{suffixe}_2`/`_3`/... sur collision externe).
//! - fn `apply_autosplit_strategy` — partition par fréquence : clés fréquentes en colonnes, les autres dans le
//!   compagnon EAV `_wide` ; pas de compagnon (la table reste Columns) quand toutes les clés sont fréquentes.
//! - fn `build_wide_pivot_schema` — construit la table EAV compagnon (réutilisée par
//!   `apply_autosplit_strategy` et `apply_pivot_split`, qui diffèrent seulement par la
//!   politique de rétention et le jeu de clés envoyé en compagnon).
//! - fn `is_pg_array` — vrai pour une colonne PG array, jamais envoyée en compagnon EAV (#48).
//! - fn `collect_medium_keys` — les clés scalaires sous le seuil de stabilité, toutes envoyées au compagnon
//!   (`AutoSplit` racine uniquement ; aucune n'est jetée, #200).
//! - fn `infer_medium_value_type` — détermine le type de valeur commun d'un jeu de clés.
//! - fn `build_finalizer_config` — assemble la config figée passée à ces fonctions.
//! - struct `FinalizerConfig` — config figée (seuils) pour cette phase.

use super::super::stats::{observed_keys_by_table, ColumnStats, ObservedKeys};
use super::super::table_schema::SuffixSchema;
use super::super::table_schema::{ChildKind, ColumnSchema, InferredStrategy, TableSchema};
use super::super::type_tracker::{widen_pg_types, PgType};
use super::super::wide_strategies::{structured_pivot_companion, suggest_wide_strategy};
use super::SchemaFinalizer;

/// Phase 3: apply per-table wide strategies (Pivot, Jsonb, `StructuredPivot`, `AutoSplit`)
/// to all remaining tables after fusion is complete. The keys of a table are read in the Pass 1
/// `stats`, which a snapshot keeps (#200): a table with no statistics under its name (a table the
/// cascade made) is never split.
pub(super) fn apply_wide_table_strategies(
    schemas: &mut Vec<TableSchema>,
    stats: &[ColumnStats],
    config: &FinalizerConfig,
) {
    let observed = observed_keys_by_table(stats);
    let mut extra_schemas: Vec<TableSchema> = Vec::new();
    // Toutes les tables déjà nommées à ce stade — sert à garantir l'unicité des noms de
    // compagnon `_pivot` synthétiques (mis à jour au fil des créations pour couvrir aussi
    // les collisions entre deux compagnons créés dans le même passage).
    let mut existing_names: std::collections::HashSet<String> =
        schemas.iter().map(|s| s.name.clone()).collect();

    for schema in schemas.iter_mut() {
        // [issue #45, finding 6] A table the cascade already fused (SiblingCollapse…) is never
        // split again: its statistics are those of the pre-fusion tables, not of its columns. Nor
        // is a table the TOML or the UI gave a strategy, `columns` included (#200).
        if !matches!(schema.inferred_strategy, InferredStrategy::Columns)
            || schema.toml_override().is_some()
            || schema.ui_override().is_some()
        {
            continue;
        }
        let Some(keys) = observed.get(schema.name.as_str()) else {
            continue;
        };
        if let Some(extra) = apply_wide_strategy(schema, keys, config, &existing_names) {
            existing_names.insert(extra.name.clone());
            extra_schemas.push(extra);
        }
    }
    schemas.extend(extra_schemas);
}

pub(super) const fn build_finalizer_config(finalizer: &SchemaFinalizer) -> FinalizerConfig {
    FinalizerConfig {
        wide_column_threshold: finalizer.wide_column_threshold,
        stable_threshold: finalizer.stable_threshold,
    }
}

pub(super) struct FinalizerConfig {
    wide_column_threshold: usize,
    stable_threshold: f64,
}

/// What `apply_wide_strategy` measured on a wide table, for the strategy branches.
struct WideStats {
    /// Rows of the table, at least 1.
    row_count: f64,
    scalar_col_count: usize,
    ratio_stable: f64,
}

impl WideStats {
    /// Log the strategy chosen for the table.
    fn log(&self, table: &str, strategy: &str) {
        let (n, pct) = (self.scalar_col_count, self.ratio_stable * 100.0);
        eprintln!(
            "  Wide table detected: {table} ({n} columns, {pct:.0}% stable) → strategy: {strategy}"
        );
    }
}

/// Share of the scalar columns present in at least `stable_threshold` of the rows.
fn stable_ratio(
    observed: &ObservedKeys,
    row_count: f64,
    stable_threshold: f64,
    scalar_cols: usize,
) -> f64 {
    let stable_count = observed
        .values()
        .filter(|k| k.total_count as f64 / row_count >= stable_threshold)
        .count();
    stable_count as f64 / scalar_cols as f64
}

/// Apply a wide-table strategy to `schema` if its scalar column count exceeds the threshold.
///
/// Eligible for the root and for Object and `ObjectArray` children. Returns the companion table
/// of the split (`_wide` or `_pivot`), if any.
fn apply_wide_strategy(
    schema: &mut TableSchema,
    observed: &ObservedKeys,
    config: &FinalizerConfig,
    existing_names: &std::collections::HashSet<String>,
) -> Option<TableSchema> {
    let is_wide_eligible = matches!(
        schema.child_kind,
        Some(ChildKind::Object | ChildKind::ObjectArray) | None
    );
    // Only scalar columns make a table wide. PG array columns stay on the table whatever the
    // strategy, so they neither raise the width nor dilute the stable ratio (#84).
    let scalar_col_count = schema.data_columns().filter(|c| !is_pg_array(c)).count();

    if !is_wide_eligible || scalar_col_count <= config.wide_column_threshold {
        return None;
    }

    let row_count = schema.row_count.max(1) as f64;
    let ratio_stable = stable_ratio(
        observed,
        row_count,
        config.stable_threshold,
        scalar_col_count,
    );
    let stats = WideStats {
        row_count,
        scalar_col_count,
        ratio_stable,
    };

    if ratio_stable > WIDE_TABLE_HIGH_STABLE_RATIO && schema.row_count >= 10 {
        let strategy = "Columns (high stable ratio — legitimate schema, not key explosion)";
        stats.log(&schema.name, strategy);
        return None;
    }
    // The rows of the root are the records of the file: its keys are fields, kept by frequency (#200).
    if schema.parent_table.is_none() {
        return apply_autosplit_strategy(schema, observed, config, &stats, existing_names);
    }
    apply_non_autosplit_strategy(schema, observed, config, &stats, existing_names)
}

/// Chooses the split of a wide child table: the identity/companion split (Pivot) when its values are
/// all of one type (its keys are data: currencies, languages…), `AutoSplit` by frequency otherwise.
/// A suffix grid found in the names is only proposed (`--suggest`), never applied here (#200).
fn apply_non_autosplit_strategy(
    schema: &mut TableSchema,
    observed: &ObservedKeys,
    config: &FinalizerConfig,
    stats: &WideStats,
    existing_names: &std::collections::HashSet<String>,
) -> Option<TableSchema> {
    if suggest_wide_strategy(observed) == InferredStrategy::Pivot {
        stats.log(&schema.name, "Pivot (split identité/compagnon)");
        return Some(apply_pivot_split(schema, observed, config, existing_names));
    }
    apply_autosplit_strategy(schema, observed, config, stats, existing_names)
}

/// Move the keys of the suffix grid of `schema` to a structured companion `_pivot` table (one
/// row per base) and return it. `schema` keeps its name and every other column (scalars outside
/// the grid, PG arrays) and becomes the identity table: `AutoSplit` with the grid keys as
/// `medium_keys`, so its real children are still written through `recurse_children` (#163).
/// Applied by the TOML `suffix_columns` override; `stable_threshold` is recorded on the `AutoSplit`
/// strategy.
pub fn apply_structured_pivot_split(
    schema: &mut TableSchema,
    suffix_schema: SuffixSchema,
    grid_keys: std::collections::HashSet<String>,
    stable_threshold: f64,
    existing_names: &std::collections::HashSet<String>,
) -> TableSchema {
    schema
        .columns
        .retain(|c| c.is_generated || !grid_keys.contains(&c.original_name));
    let companion_name = unique_companion_name(&schema.name, "pivot", existing_names);
    // A key ending with no suffix is a bare base: it fills `value`.
    let has_bare_keys = grid_keys.iter().any(|k| {
        !suffix_schema
            .suffix_cols
            .iter()
            .any(|sc| k.len() > sc.suffix.len() && k.ends_with(sc.suffix.as_str()))
    });
    let companion =
        structured_pivot_companion(schema, companion_name.clone(), suffix_schema, has_bare_keys);
    schema.inferred_strategy = InferredStrategy::AutoSplit {
        stable_threshold,
        medium_keys: grid_keys,
        wide_table_name: companion_name,
    };
    companion
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
    observed: &ObservedKeys,
    config: &FinalizerConfig,
    existing_names: &std::collections::HashSet<String>,
) -> TableSchema {
    let all_keys: std::collections::HashSet<String> = schema
        .data_columns()
        .filter(|c| !is_pg_array(c))
        .map(|c| c.original_name.clone())
        .collect();
    schema.columns.retain(|c| c.is_generated || is_pg_array(c));
    let wide_name = unique_companion_name(&schema.name, "pivot", existing_names);
    let value_type = infer_medium_value_type(observed, &all_keys);
    build_wide_pivot_schema(schema, wide_name, value_type, all_keys, config)
}

/// Companion name of a split, `{base}_{suffix}` (`_pivot` or `_wide`), falling back to
/// `{base}_{suffix}_eav` on self-collision (schema already named `..._{suffix}`) and to an incremental
/// `_{suffix}_2`, `_{suffix}_3`, ... on collision with any other already-known table name.
fn unique_companion_name(
    schema_name: &str,
    suffix: &str,
    existing_names: &std::collections::HashSet<String>,
) -> String {
    let derived = crate::schema::naming::derived_table_name;
    let base_name = schema_name
        .strip_suffix(&format!("_{suffix}"))
        .unwrap_or(schema_name);
    let candidate = derived(&format!("{base_name}_{suffix}"));
    let mut candidate = if candidate == schema_name {
        derived(&format!("{base_name}_{suffix}_eav"))
    } else {
        candidate
    };
    let mut suffix_n = 2;
    while existing_names.contains(&candidate) {
        candidate = derived(&format!("{base_name}_{suffix}_{suffix_n}"));
        suffix_n += 1;
    }
    candidate
}

/// PG array column — never routed to an EAV companion (issue #48).
const fn is_pg_array(col: &ColumnSchema) -> bool {
    matches!(col.pg_type, PgType::Array(_))
}

/// The scalar keys of an `AutoSplit` table below `stable_threshold`: they all go to the `_wide` companion,
/// however rare (#200).
fn collect_medium_keys(
    observed: &ObservedKeys,
    row_count: f64,
    stable_threshold: f64,
) -> std::collections::HashSet<String> {
    observed
        .iter()
        .filter(|(_, t)| (t.total_count as f64 / row_count) < stable_threshold)
        .map(|(k, _)| k.clone())
        .collect()
}

fn infer_medium_value_type(
    observed: &ObservedKeys,
    medium_keys: &std::collections::HashSet<String>,
) -> PgType {
    medium_keys
        .iter()
        .filter_map(|k| observed.get(k))
        .fold(None::<PgType>, |acc, t| {
            Some(acc.map_or_else(|| t.pg_type.clone(), |a| widen_pg_types(a, &t.pg_type)))
        })
        .unwrap_or(PgType::Text)
}

fn apply_autosplit_strategy(
    schema: &mut TableSchema,
    observed: &ObservedKeys,
    config: &FinalizerConfig,
    stats: &WideStats,
    existing_names: &std::collections::HashSet<String>,
) -> Option<TableSchema> {
    let (row_count, stable) = (stats.row_count, config.stable_threshold);
    let medium_keys = collect_medium_keys(observed, row_count, stable);
    // Every key is frequent (few rows): no companion to build, the table stays Columns.
    if medium_keys.is_empty() {
        stats.log(&schema.name, "Columns (every key is frequent)");
        return None;
    }
    // pg-array columns are kept unconditionally: their tracker counts elements, not occurrences,
    // and the scalar EAV companion cannot hold them (issue #48).
    schema.columns.retain(|c| {
        c.is_generated
            || is_pg_array(c)
            || observed
                .get(&c.original_name)
                .is_some_and(|t| t.total_count as f64 / row_count >= stable)
    });
    // Scalars only, like `scalar_col_count`: the kept pg-array columns are not stable keys.
    let stable_col_count = schema.data_columns().filter(|c| !is_pg_array(c)).count();
    let wide_name = unique_companion_name(&schema.name, "wide", existing_names);
    let medium = medium_keys.len();
    stats.log(
        &schema.name,
        &format!("AutoSplit ({stable_col_count} stable cols, {medium} medium → {wide_name})"),
    );
    let value_type = infer_medium_value_type(observed, &medium_keys);
    Some(build_wide_pivot_schema(
        schema,
        wide_name,
        value_type,
        medium_keys,
        config,
    ))
}

fn build_wide_pivot_schema(
    schema: &mut TableSchema,
    wide_name: String,
    value_type: PgType,
    medium_keys: std::collections::HashSet<String>,
    config: &FinalizerConfig,
) -> TableSchema {
    let mut wide_schema =
        TableSchema::new(wide_name.clone(), vec![wide_name.clone()], schema.depth + 1);
    wide_schema.parent_table = Some(schema.name.clone());
    wide_schema.child_kind = Some(ChildKind::Object);
    wide_schema
        .columns
        .push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    wide_schema
        .columns
        .push(ColumnSchema::parent_fk(&schema.name));
    wide_schema
        .columns
        .push(ColumnSchema::data("key", PgType::Text, true));
    wide_schema
        .columns
        .push(ColumnSchema::data("value", value_type, false));
    wide_schema.inferred_strategy = InferredStrategy::Pivot;
    schema.inferred_strategy = InferredStrategy::AutoSplit {
        stable_threshold: config.stable_threshold,
        medium_keys,
        wide_table_name: wide_name,
    };
    wide_schema
}

/// A wide table with this fraction or more of stable columns is kept as-is (Columns strategy).
/// Below this, the table is split or pivoted.
const WIDE_TABLE_HIGH_STABLE_RATIO: f64 = 0.5;

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
        }
    }

    fn stat(table: &str, key: &str, pg_type: PgType) -> ColumnStats {
        ColumnStats {
            table_name: table.to_string(),
            column_name: NamingRegistry::column_name(key),
            original_name: key.to_string(),
            pg_type,
            total_count: 3,
            null_count: 0,
            type_histogram: Vec::new(),
        }
    }

    /// The split reads the keys by their JSON name: two keys of one column name stay two keys, and
    /// a PG array column is not a scalar key (#200).
    #[test]
    fn observed_keys_are_the_scalar_json_keys_of_each_table() {
        let stats = [
            stat("t", "energy-kcal", PgType::Integer),
            stat("t", "energy_kcal", PgType::DoublePrecision),
            stat("t", "tags", PgType::Array(Box::new(PgType::Text))),
            stat("u", "x", PgType::Text),
        ];
        let by_table = observed_keys_by_table(&stats);
        let keys: Vec<&str> = by_table["t"].keys().map(String::as_str).collect();
        assert_eq!(keys, ["energy-kcal", "energy_kcal"]);
        assert_eq!(
            by_table["t"]["energy_kcal"].pg_type,
            PgType::DoublePrecision
        );
        assert_eq!(by_table["u"].len(), 1);
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
    #[allow(clippy::too_many_lines)] // single test case with verbose setup
    fn already_assigned_schema_is_never_reprocessed() {
        let mut observer = SchemaObserver::new(256);
        let obj: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"id": 1, "foo": {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}}"#)
                .unwrap();
        observer.observe_root("root", &obj);

        let mut naming = NamingRegistry::new();
        for path_key in observer.tables.keys() {
            naming.table_name_from_dot_key(path_key);
        }

        // Simule la Phase 2 ayant déjà fusionné "root_foo" en SiblingCollapse — une forme de
        // colonnes différente de ce que le TableEntry brut (5 colonnes scalaires) produirait.
        let mut fused = TableSchema::new(
            "root_foo".to_string(),
            vec!["root".to_string(), "foo".to_string()],
            1,
        );
        fused.parent_table = Some("root".to_string());
        fused.inferred_strategy = InferredStrategy::SiblingCollapse(SiblingSchema {
            key_col_name: "key".to_string(),
            key_shape: KeyShape::Slug,
            array_children: false,
        });
        fused
            .columns
            .push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        fused.columns.push(ColumnSchema::parent_fk("root"));
        // 3 colonnes data > wide_column_threshold(2) — nécessaire pour que apply_wide_strategy
        // dépasse le premier seuil et atteigne réellement le chemin de retraitement.
        for col_name in ["key", "x1", "imgid"] {
            fused.columns.push(ColumnSchema {
                name: col_name.to_string(),
                original_name: col_name.to_string(),
                pg_type: PgType::Text,
                not_null: true,
                is_generated: false,
                is_parent_fk: false,
            });
        }
        let mut schemas = vec![fused];

        let stats = crate::schema::inspector::collect_stats(&observer, &mut naming);
        apply_wide_table_strategies(&mut schemas, &stats, &make_config());

        assert_eq!(
            schemas.len(),
            1,
            "aucun compagnon ne doit être créé à partir d'une table déjà fusionnée"
        );
        assert!(
            matches!(
                schemas[0].inferred_strategy,
                InferredStrategy::SiblingCollapse(_)
            ),
            "le SiblingCollapse de la Phase 2 doit survivre intact — trouvé : {:?}",
            schemas[0].inferred_strategy
        );
        assert_eq!(
            schemas[0].data_columns().count(),
            3,
            "colonnes inchangées (key, x1, imgid)"
        );
    }
}
