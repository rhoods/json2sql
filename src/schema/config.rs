//! TOML configuration file: user-defined type and strategy overrides applied before Pass 2.
//!
//! The config is optional. When present, `apply_overrides` validates each override against
//! the finalized schema and mutates the relevant [`TableSchema`] entries in place.
//! Unknown table or column names produce a hard error rather than silently doing nothing.
//!
//! Fonctions :
//! - struct `GroupConfig` — définition d'un groupe de fusion (`SiblingCollapse` manuel).
//! - struct `SchemaConfig` — config TOML complète (groupes de fusion + overrides par table).
//! - fn `SchemaConfig::from_file` — charge et parse le fichier TOML.
//! - enum `ConfigWarning` — warning non-fatal (table/colonne/type/stratégie/groupe inconnu).
//! - fn `ConfigWarning::to_message` — message lisible pour chaque variante de warning.
//! - struct `DeferredNormalize` — override `normalize_dynamic_keys` en attente (mutation globale).
//! - struct `DeferredFlatten` — override `flatten` en attente (mutation globale).
//! - fn `apply_overrides` — point d'entrée par table : dispatch strategy/`suffix_columns`/types,
//!   reporte `normalize_dynamic_keys`/`flatten` en différé (nécessitent une mutation globale).
//! - fn `toml_str` — helper de lecture d'une clé TOML en `String`.
//! - fn `has_nonempty_suffix_columns` — vrai si `suffix_columns` est un array TOML non vide.
//! - fn `conflicting_strategy_override` — détecte un conflit `strategy` vs `suffix_columns`
//!   (#31, `suffix_columns` gagne toujours).
//! - fn `apply_strategy_override` — applique la clé `strategy` (`pivot`/`jsonb`/`columns`/`structured_pivot`/
//!   `normalize_dynamic_keys`/`flatten`).
//! - fn `apply_suffix_columns_override` — applique `suffix_columns` (`StructuredPivot` explicite).
//! - fn `apply_column_type_overrides` — applique les overrides de type par colonne.
//! - fn `apply_group_overrides` — applique les groupes de fusion (`[group.*]`, stratégie `keyed_pivot`).
//! - fn `build_merged_keyed_pivot_schema` — construit le schéma fusionné (colonnes union + clé `key_id`).
//! - fn `apply_keyed_pivot_merge` — fusionne N tables membres en une seule `SiblingCollapse`.
//! - fn `apply_overrides_complete` — enchaîne overrides + groupes + `exclude_absorbed_children`.
//! - struct `SkipCascadeWarning` — table retirée par `Skip` ayant entraîné de vrais enfants en cascade.
//! - fn `apply_user_overrides` — applique les overrides IHM (`Pivot`/`Jsonb`/`Skip`) après chargement
//!   d'un snapshot ; `Skip` retire aussi la table `_wide` compagnon d'un `AutoSplit` et tout vrai
//!   enfant cascadant depuis l'un ou l'autre, à toute profondeur.
//! - fn `apply_single_user_override` — dispatch `Pivot`/`Jsonb`/`Columns` pour une seule table.
//! - fn `wide_companions_of_skipped` — compagnons `_wide` des tables `AutoSplit` skip-ées.
//! - fn `close_over_real_children` — fermeture transitive d'un ensemble de tables sur leurs
//!   vrais enfants (`parent_table`), en une passe (schémas triés topologiquement).
//! - fn `compute_skip_cascade` — ensemble complet des tables à retirer pour `Skip` + un
//!   `SkipCascadeWarning` par racine ayant réellement cascadé (`pub` : requête pure réutilisable
//!   par l'IHM pour afficher la cascade avant application).
//! - fn `prime_tracker_from_pg_type` — reconstruit un `TypeTracker` représentatif depuis un `PgType`
//!   déjà résolu (pour réutiliser `build_suffix_schema_from_list` après finalisation).
//! - fn `parse_pg_type` — parse une chaîne de type SQL (avec alias) en `PgType`.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

use crate::error::{J2sError, Result};
use crate::schema::wide_strategies::{
    apply_flatten, apply_normalize_dynamic_keys, apply_structured_pivot_columns,
    apply_wide_strategy_columns, build_union_columns,
};
use crate::schema::suffix_detector::build_suffix_schema_from_list;
use crate::schema::table_schema::{ColumnSchema, KeyShape, SiblingSchema, TableSchema, InferredStrategy, UserOverride};
use crate::schema::type_tracker::PgType;

/// TOML config file for manual type overrides.
///
/// ```toml
/// [users]
/// age = "INTEGER"
/// created_at = "TIMESTAMP"
///
/// [nutrients]
/// strategy = "structured_pivot"
/// suffix_columns = ["_100g", "_unit", "_label"]
///
/// [users_orders]
/// amount = "DOUBLE PRECISION"
/// ```
///
/// Keys are the `PostgreSQL` column names (sanitized). Values are SQL type strings.
/// Special keys: `strategy`, `suffix_columns`.
/// Définition d'un groupe de fusion (`SiblingCollapse` manuel).
#[derive(Debug, Deserialize)]
pub struct GroupConfig {
    pub strategy: String,
    pub members: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaConfig {
    /// Groupes de fusion : `nom_résultant` → { strategy, members }
    #[serde(default)]
    pub group: HashMap<String, GroupConfig>,
    /// Overrides par table : `table_name` → { `colonne_ou_strategy` → valeur }
    #[serde(flatten)]
    pub tables: HashMap<String, HashMap<String, toml::Value>>,
}

impl SchemaConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(J2sError::Io)?;
        toml::from_str(&content).map_err(|e| {
            J2sError::InvalidInput(format!(
                "Failed to parse schema config '{}': {}",
                path.display(),
                e
            ))
        })
    }
}

/// Non-fatal warning emitted when a TOML config override references an unknown table,
/// column, type, strategy, or group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigWarning {
    UnknownTable(String),
    UnknownColumn { table: String, column: String },
    UnknownType    { table: String, column: String, type_str: String },
    UnknownStrategy { table: String, strategy: String },
    UnknownGroupStrategy { group: String, strategy: String },
    GroupMergeFailed { group: String, found: usize, expected: usize },
    /// Override to `to_strategy` is incompatible with the table's current column layout
    /// (inferred as `from_strategy`). Column names were lost during finalization and cannot
    /// be reconstructed.
    InvalidOverride { table: String, from_strategy: String, to_strategy: String },
    /// Both `strategy` and `suffix_columns` were set for the same table. `suffix_columns`
    /// takes precedence — `strategy` is ignored entirely (see #31).
    ConflictingOverride { table: String, ignored_strategy: String },
}

impl ConfigWarning {
    pub fn to_message(&self) -> String {
        match self {
            Self::UnknownTable(t) =>
                format!("schema-config: table '{t}' not found in inferred schema"),
            Self::UnknownColumn { table, column } =>
                format!("schema-config: column '{table}.{column}' not found"),
            Self::UnknownType { table, column, type_str } =>
                format!("schema-config: unknown type '{type_str}' for '{table}.{column}', ignored"),
            Self::UnknownStrategy { table, strategy } =>
                format!("schema-config: unknown strategy '{strategy}' for '{table}', ignored"),
            Self::UnknownGroupStrategy { group, strategy } =>
                format!("schema-config: unknown group strategy '{strategy}' for group '{group}', ignored"),
            Self::GroupMergeFailed { group, found, expected } =>
                format!("group '{group}': {found}/{expected} member(s) found, merge ignored"),
            Self::InvalidOverride { table, from_strategy, to_strategy } =>
                format!("schema-config: cannot override '{table}' from {from_strategy} to {to_strategy} — column names are lost after finalization, ignored"),
            Self::ConflictingOverride { table, ignored_strategy } =>
                format!("schema-config: table '{table}' has both 'strategy' and 'suffix_columns' set — suffix_columns takes precedence, ignoring strategy = '{ignored_strategy}'"),
        }
    }
}

struct DeferredNormalize { table_name: String, id_column: String }
struct DeferredFlatten  { table_name: String, prefix: String, max_depth: u8 }

/// Apply type overrides from `config` to the finalized schemas.
/// Matches by table name and column name (both sanitized `PostgreSQL` identifiers).
/// Unknown tables, columns, types, or strategies are returned as `ConfigWarning` values
/// rather than written to stderr — the caller decides how to display them.
pub fn apply_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) -> crate::error::Result<Vec<ConfigWarning>> {
    let mut warnings: Vec<ConfigWarning> = Vec::new();
    let mut deferred_normalize: Vec<DeferredNormalize> = Vec::new();
    let mut deferred_flatten:   Vec<DeferredFlatten>   = Vec::new();

    for (table_name, col_overrides) in &config.tables {
        match schemas.iter_mut().find(|s| &s.name == table_name) {
            None => warnings.push(ConfigWarning::UnknownTable(table_name.clone())),
            Some(schema) => {
                // suffix_columns always wins over a conflicting `strategy` key (see #31):
                // it's the more specific override and already does the real column work
                // (apply_suffix_columns_override below). A genuinely invalid/misspelled
                // strategy value still runs through apply_strategy_override so its own
                // UnknownStrategy warning fires — only a *recognized* strategy other than
                // "structured_pivot" counts as a real conflict here.
                match conflicting_strategy_override(col_overrides) {
                    Some(ignored_strategy) => warnings.push(ConfigWarning::ConflictingOverride {
                        table: table_name.clone(),
                        ignored_strategy,
                    }),
                    None => warnings.extend(apply_strategy_override(schema, table_name, col_overrides, &mut deferred_normalize, &mut deferred_flatten)),
                }
                warnings.extend(apply_suffix_columns_override(schema, table_name, col_overrides));
                warnings.extend(apply_column_type_overrides(schema, table_name, col_overrides));
            }
        }
    }

    for op in deferred_normalize {
        let original = schemas.iter().find(|s| s.name == op.table_name).map(|s| s.inferred_strategy.clone());
        apply_normalize_dynamic_keys(schemas, &op.table_name, op.id_column.clone())?;
        if let Some(s) = schemas.iter_mut().find(|s| s.name == op.table_name) {
            if let Some(orig) = original { s.inferred_strategy = orig; }
            s.set_toml_override(Some(UserOverride::NormalizeDynamicKeys { id_column: op.id_column.into() }));
        }
    }
    for op in deferred_flatten {
        apply_flatten(schemas, &op.table_name, &op.prefix, op.max_depth)?;
    }
    Ok(warnings)
}

fn toml_str(map: &HashMap<String, toml::Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| if let toml::Value::String(s) = v { Some(s.clone()) } else { None })
}

fn has_nonempty_suffix_columns(col_overrides: &HashMap<String, toml::Value>) -> bool {
    matches!(col_overrides.get("suffix_columns"), Some(toml::Value::Array(arr))
        if arr.iter().any(|v| matches!(v, toml::Value::String(_))))
}

/// Returns the `strategy` value to ignore if this table's config combines a recognized,
/// non-`structured_pivot` `strategy` with a non-empty `suffix_columns` — a real conflict
/// per #31 (`suffix_columns` always wins). Returns `None` for: no `strategy` key, a
/// non-string value, `"structured_pivot"` (the documented normal combo), an unrecognized
/// value (left to `apply_strategy_override`'s own `UnknownStrategy` warning), or no
/// `suffix_columns`.
fn conflicting_strategy_override(col_overrides: &HashMap<String, toml::Value>) -> Option<String> {
    if !has_nonempty_suffix_columns(col_overrides) {
        return None;
    }
    let strategy_str = toml_str(col_overrides, "strategy")?;
    match strategy_str.to_lowercase().as_str() {
        "pivot" | "jsonb" | "columns" | "normalize_dynamic_keys" | "flatten" => Some(strategy_str),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)] // exhaustive dispatch over all override strategy variants
fn apply_strategy_override(
    schema: &mut TableSchema,
    table_name: &str,
    col_overrides: &HashMap<String, toml::Value>,
    deferred_normalize: &mut Vec<DeferredNormalize>,
    deferred_flatten: &mut Vec<DeferredFlatten>,
) -> Vec<ConfigWarning> {
    let Some(toml::Value::String(strategy_str)) = col_overrides.get("strategy") else { return vec![] };
    match strategy_str.to_lowercase().as_str() {
        "pivot" => {
            let original = schema.inferred_strategy.clone();
            eprintln!("  Override strategy: {table_name} → Pivot");
            apply_wide_strategy_columns(schema, InferredStrategy::Pivot);
            schema.inferred_strategy = original;
            schema.set_toml_override(Some(UserOverride::Pivot));
        }
        "jsonb" => {
            let original = schema.inferred_strategy.clone();
            eprintln!("  Override strategy: {table_name} → Jsonb");
            apply_wide_strategy_columns(schema, InferredStrategy::Jsonb);
            schema.inferred_strategy = original;
            schema.set_toml_override(Some(UserOverride::Jsonb));
        }
        "columns" => {
            if !matches!(schema.inferred_strategy, InferredStrategy::Columns) {
                return vec![ConfigWarning::InvalidOverride {
                    table: table_name.to_string(),
                    from_strategy: format!("{:?}", schema.inferred_strategy)
                        .split('(').next().unwrap_or("Unknown").to_string(),
                    to_strategy: "columns".to_string(),
                }];
            }
            eprintln!("  Override strategy: {table_name} → Columns (no-op, already inferred)");
            schema.set_toml_override(Some(UserOverride::Columns));
        }
        // Intentional no-op, not a stub to fill in: the actual column/strategy work happens
        // in apply_suffix_columns_override (below, driven by the `suffix_columns` key), which
        // sets schema.inferred_strategy = StructuredPivot(..) directly. `toml_override` is not
        // set here because `UserOverride` has no `StructuredPivot` variant — effective_strategy()
        // still resolves correctly by falling through to inferred_strategy. Verified in #28.
        "structured_pivot" => {}
        "normalize_dynamic_keys" => {
            let id_col = toml_str(col_overrides, "id_column").unwrap_or_else(|| "key_id".to_string());
            deferred_normalize.push(DeferredNormalize { table_name: table_name.to_string(), id_column: id_col });
        }
        "flatten" => {
            let prefix = toml_str(col_overrides, "prefix").unwrap_or_else(|| format!("{table_name}_"));
            let max_depth = col_overrides.get("max_depth")
                .and_then(|v| if let toml::Value::Integer(n) = v { u8::try_from(*n).ok() } else { None })
                .unwrap_or(1);
            deferred_flatten.push(DeferredFlatten { table_name: table_name.to_string(), prefix, max_depth });
        }
        other => return vec![ConfigWarning::UnknownStrategy {
            table: table_name.to_string(),
            strategy: other.to_string(),
        }],
    }
    vec![]
}

fn apply_suffix_columns_override(
    schema: &mut TableSchema,
    table_name: &str,
    col_overrides: &HashMap<String, toml::Value>,
) -> Vec<ConfigWarning> {
    let Some(toml::Value::Array(arr)) = col_overrides.get("suffix_columns") else { return vec![] };
    let suffix_list: Vec<String> = arr
        .iter()
        .filter_map(|v| if let toml::Value::String(s) = v { Some(s.clone()) } else { None })
        .collect();
    if suffix_list.is_empty() { return vec![]; }

    // Same compatibility guard as the "columns" branch of apply_strategy_override: once a
    // table is no longer Columns (SiblingCollapse/AutoSplit/Pivot via cascade/wide-table
    // detection), its column names are gone and suffix_columns can't be reconstructed
    // safely — reject rather than silently overwrite (#31).
    if !matches!(schema.inferred_strategy, InferredStrategy::Columns) {
        return vec![ConfigWarning::InvalidOverride {
            table: table_name.to_string(),
            from_strategy: format!("{:?}", schema.inferred_strategy)
                .split('(').next().unwrap_or("Unknown").to_string(),
            to_strategy: "structured_pivot".to_string(),
        }];
    }

    // At config-apply time the schema is already finalized (columns are resolved).
    // Build a dummy TypeTracker map from existing column types so
    // build_suffix_schema_from_list can widen types correctly.
    let mut type_map: indexmap::IndexMap<String, crate::schema::type_tracker::TypeTracker> =
        indexmap::IndexMap::new();
    for col in schema.data_columns() {
        let mut tracker = crate::schema::type_tracker::TypeTracker::new(256);
        prime_tracker_from_pg_type(&mut tracker, &col.pg_type);
        type_map.insert(col.original_name.clone(), tracker);
    }
    let suffix_schema = build_suffix_schema_from_list(&suffix_list, &type_map);
    eprintln!("  Override strategy: {table_name} → StructuredPivot (suffixes: {suffix_list:?})");
    apply_structured_pivot_columns(schema, suffix_schema);
    vec![]
}

fn apply_column_type_overrides(
    schema: &mut TableSchema,
    table_name: &str,
    col_overrides: &HashMap<String, toml::Value>,
) -> Vec<ConfigWarning> {
    let mut warnings = Vec::new();
    for (col_name, value) in col_overrides {
        if matches!(col_name.as_str(), "strategy" | "suffix_columns" | "id_column" | "prefix" | "max_depth") {
            continue;
        }
        let toml::Value::String(type_str) = value else { continue };
        match schema.columns.iter_mut().find(|c| &c.name == col_name) {
            None => warnings.push(ConfigWarning::UnknownColumn {
                table: table_name.to_string(),
                column: col_name.clone(),
            }),
            Some(col) => match parse_pg_type(type_str) {
                None => warnings.push(ConfigWarning::UnknownType {
                    table: table_name.to_string(),
                    column: col_name.clone(),
                    type_str: type_str.clone(),
                }),
                Some(pg_type) => {
                    eprintln!("  Override: {}.{} {} → {}", table_name, col_name, col.pg_type.as_sql(), pg_type.as_sql());
                    col.pg_type = pg_type;
                }
            },
        }
    }
    warnings
}

/// Appliquer les groupes de fusion définis dans la config.
/// Doit être appelé APRÈS `apply_overrides` et AVANT la sauvegarde du snapshot.
pub fn apply_group_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) -> Vec<ConfigWarning> {
    let mut warnings = Vec::new();
    for (group_name, group_cfg) in &config.group {
        match group_cfg.strategy.to_lowercase().as_str() {
            "keyed_pivot" => {
                if let Some(w) = apply_keyed_pivot_merge(schemas, group_name, &group_cfg.members) {
                    warnings.push(w);
                }
            }
            other => warnings.push(ConfigWarning::UnknownGroupStrategy {
                group: group_name.clone(),
                strategy: other.to_string(),
            }),
        }
    }
    warnings
}

/// Apply all config overrides in sequence, then re-run child exclusion.
///
/// Strategy overrides may change a parent table from `Columns` to `Jsonb`/`Pivot`, which
/// means its former child tables would receive no data. `exclude_absorbed_children` removes
/// them so Pass 2 never tries to insert into non-existent tables.
pub fn apply_overrides_complete(
    schemas: &mut Vec<TableSchema>,
    config: &SchemaConfig,
) -> crate::error::Result<Vec<ConfigWarning>> {
    let mut warnings = apply_overrides(schemas, config)?;
    warnings.extend(apply_group_overrides(schemas, config));
    crate::schema::finalizer::exclude_absorbed_children(schemas);
    Ok(warnings)
}

#[allow(clippy::too_many_lines)] // struct construction pipeline: generated cols → key col → union cols → strategy
fn build_merged_keyed_pivot_schema(group_name: &str, cloned: &[TableSchema]) -> TableSchema {
    let refs: Vec<&TableSchema> = cloned.iter().collect();
    let first = &cloned[0];
    let mut merged =
        TableSchema::new(group_name.to_string(), vec![group_name.to_string()], first.depth);
    merged.parent_table.clone_from(&first.parent_table);
    merged.child_kind.clone_from(&first.child_kind);
    merged.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    if let Some(ref parent) = first.parent_table {
        merged.columns.push(ColumnSchema::parent_fk(parent));
    }
    if first.has_order_column() {
        merged.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
    merged.columns.push(ColumnSchema {
        name: "key_id".to_string(),
        original_name: "key_id".to_string(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    for col in build_union_columns(&refs) {
        merged.columns.push(col);
    }
    merged.inferred_strategy = InferredStrategy::SiblingCollapse(SiblingSchema {
        key_col_name: "key_id".to_string(),
        key_shape: KeyShape::Mixed,
        array_children: false,
    });
    merged
}

fn apply_keyed_pivot_merge(schemas: &mut Vec<TableSchema>, group_name: &str, members: &[String]) -> Option<ConfigWarning> {
    let mut indices: Vec<usize> = members
        .iter()
        .filter_map(|name| schemas.iter().position(|s| &s.name == name))
        .collect();

    if indices.len() < 2 {
        return Some(ConfigWarning::GroupMergeFailed {
            group: group_name.to_string(),
            found: indices.len(),
            expected: members.len(),
        });
    }
    indices.sort_unstable();
    let insert_pos = indices[0];

    // Cloner les membres avant toute mutation
    let cloned: Vec<TableSchema> = indices.iter().map(|&i| schemas[i].clone()).collect();
    let merged = build_merged_keyed_pivot_schema(group_name, &cloned);

    // Retirer les membres du plus grand index au plus petit pour éviter le décalage
    for &i in indices.iter().rev() {
        schemas.remove(i);
    }
    schemas.insert(insert_pos, merged);

    eprintln!(
        "  Groupe '{}' : {} tables → 1 (SiblingCollapse)",
        group_name,
        indices.len()
    );
    None
}

/// Recorded when a `Skip` (direct, or via an `AutoSplit` `_wide` companion) cascades onto
/// real children (`parent_table` pointing at a removed table, at any depth).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkipCascadeWarning {
    pub removed_table: String,
    pub cascaded_children: Vec<String>,
}

/// Apply IHM strategy overrides (`Pivot | Jsonb | Skip`) to a mutable schema slice.
///
/// Called by the CLI after loading a snapshot that includes `strategy_overrides`.
/// `Skip` removes the table — and if it had `AutoSplit`, also removes the companion
/// `_wide` table, along with any real children cascading transitively from either
/// (see [`SkipCascadeWarning`]). `Pivot` and `Jsonb` mutate `inferred_strategy` in place.
pub fn apply_user_overrides(
    schemas: &mut Vec<TableSchema>,
    overrides: &HashMap<String, UserOverride>,
) -> Vec<SkipCascadeWarning> {
    for (table_name, ov) in overrides {
        if let Some(s) = schemas.iter_mut().find(|s| &s.name == table_name) {
            apply_single_user_override(s, ov);
        }
    }

    let (removed, warnings) = compute_skip_cascade(schemas, overrides);
    schemas.retain(|s| !removed.contains(&s.name));
    warnings
}

/// Mutate a single schema in place for the `Pivot`/`Jsonb`/`Columns` branches of
/// `apply_user_overrides`. `Skip` and the remaining variants are no-ops here — `Skip` is
/// handled separately by [`compute_skip_cascade`].
fn apply_single_user_override(s: &mut TableSchema, ov: &UserOverride) {
    match ov {
        UserOverride::Pivot | UserOverride::Jsonb => {
            let strategy = if matches!(ov, UserOverride::Pivot) {
                InferredStrategy::Pivot
            } else {
                InferredStrategy::Jsonb
            };
            let original = s.inferred_strategy.clone();
            apply_wide_strategy_columns(s, strategy);
            s.inferred_strategy = original;
            if s.ui_override().is_none() {
                s.set_ui_override(Some(ov.clone()));
            }
        }
        UserOverride::Columns => {
            if matches!(s.inferred_strategy, InferredStrategy::Columns) && s.ui_override().is_none() {
                s.set_ui_override(Some(UserOverride::Columns));
            }
        }
        UserOverride::Skip
        | UserOverride::JsonbFlatten
        | UserOverride::Flatten { .. }
        | UserOverride::NormalizeDynamicKeys { .. } => {}
    }
}

/// Companion `_wide` table for each `AutoSplit` table being skipped, keyed by the Skip root
/// name. Unlike a real child, the companion isn't reachable via `parent_table` — it's a
/// sibling table generated by the `AutoSplit` strategy, so it must be seeded explicitly.
fn wide_companions_of_skipped(
    schemas: &[TableSchema],
    overrides: &HashMap<String, UserOverride>,
) -> HashMap<String, String> {
    overrides.iter()
        .filter_map(|(name, ov)| {
            if !matches!(ov, UserOverride::Skip) { return None; }
            schemas.iter().find(|s| &s.name == name).and_then(|s| {
                if let InferredStrategy::AutoSplit { wide_table_name, .. } = &s.inferred_strategy {
                    Some((name.clone(), wide_table_name.clone()))
                } else {
                    None
                }
            })
        })
        .collect()
}

/// Every table reachable from `seed` through `parent_table`, at any depth, plus `seed` itself.
/// `schemas` must be topologically sorted (parent before child) — a single forward pass then
/// suffices, same technique as `collect_surviving_route_targets` in `finalizer/guard.rs`.
fn close_over_real_children(schemas: &[TableSchema], seed: HashSet<String>) -> HashSet<String> {
    let mut closure = seed;
    for schema in schemas {
        if let Some(ref parent) = schema.parent_table {
            if closure.contains(parent) {
                closure.insert(schema.name.clone());
            }
        }
    }
    closure
}

/// Compute the full set of tables to remove for `Skip` (direct roots, their wide companions,
/// and every real child cascading transitively from either), plus one [`SkipCascadeWarning`]
/// per direct root that actually took a real child down with it.
///
/// A table the user Skip-ed directly is always reported as its own root, never folded into
/// another root's `cascaded_children` — even if it's also reachable by cascade from that root.
///
/// Public so callers that need to *display* the cascade (e.g. the GUI, before the user commits
/// to an import) can query it without mutating `schemas`, the way [`apply_user_overrides`] does.
pub fn compute_skip_cascade(
    schemas: &[TableSchema],
    overrides: &HashMap<String, UserOverride>,
) -> (HashSet<String>, Vec<SkipCascadeWarning>) {
    let wide_companion = wide_companions_of_skipped(schemas, overrides);
    let direct_skip_roots: HashSet<String> = overrides.iter()
        .filter(|(name, ov)| matches!(ov, UserOverride::Skip) && schemas.iter().any(|s| &s.name == *name))
        .map(|(name, _)| name.clone())
        .collect();

    let mut seed = direct_skip_roots.clone();
    seed.extend(wide_companion.values().cloned());
    let removed = close_over_real_children(schemas, seed);

    let mut warnings: Vec<SkipCascadeWarning> = direct_skip_roots.iter()
        .filter_map(|root| {
            let mut own_seed = HashSet::from([root.clone()]);
            if let Some(wide_name) = wide_companion.get(root) {
                own_seed.insert(wide_name.clone());
            }
            let mut cascaded: Vec<String> = close_over_real_children(schemas, own_seed)
                .into_iter()
                .filter(|name| name != root && !direct_skip_roots.contains(name))
                .collect();
            if cascaded.is_empty() {
                return None;
            }
            cascaded.sort();
            Some(SkipCascadeWarning { removed_table: root.clone(), cascaded_children: cascaded })
        })
        .collect();
    warnings.sort_by(|a, b| a.removed_table.cmp(&b.removed_table));

    (removed, warnings)
}

/// Prime a `TypeTracker` with a representative observation so `to_pg_type()` returns
/// a type consistent with the given `PgType`.  Used when rebuilding type maps from
/// already-resolved column schemas.
const fn prime_tracker_from_pg_type(
    tracker: &mut crate::schema::type_tracker::TypeTracker,
    pg_type: &PgType,
) {
    use crate::schema::type_tracker::InferredType;
    let inferred = match pg_type {
        PgType::Integer => InferredType::Integer,
        PgType::BigInt => InferredType::BigInt,
        PgType::DoublePrecision => InferredType::Float,
        PgType::Boolean => InferredType::Boolean,
        PgType::Uuid => InferredType::Uuid,
        PgType::Date => InferredType::Date,
        PgType::Timestamp => InferredType::Timestamp,
        PgType::VarChar(_) | PgType::Text | PgType::Jsonb | PgType::Array(_) => {
            InferredType::Varchar
        }
    };
    tracker.type_counts[inferred as usize] += 1;
    tracker.total_count += 1;
}

/// Parse a SQL type string into a `PgType`.
/// Supports common aliases (case-insensitive).
fn parse_pg_type(s: &str) -> Option<PgType> {
    match s.trim().to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" => Some(PgType::Integer),
        "BIGINT" | "INT8" => Some(PgType::BigInt),
        "DOUBLE PRECISION" | "FLOAT" | "FLOAT8" | "REAL" | "FLOAT4" => {
            Some(PgType::DoublePrecision)
        }
        "BOOLEAN" | "BOOL" => Some(PgType::Boolean),
        "UUID" => Some(PgType::Uuid),
        "DATE" => Some(PgType::Date),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Some(PgType::Timestamp),
        "TEXT" => Some(PgType::Text),
        other => {
            // VARCHAR(N)
            if let Some(inner) = other.strip_prefix("VARCHAR(").and_then(|s| s.strip_suffix(')')) {
                if let Ok(n) = inner.trim().parse::<u32>() {
                    return Some(PgType::VarChar(n));
                }
            }
            // CHARACTER VARYING(N)
            if let Some(inner) = other
                .strip_prefix("CHARACTER VARYING(")
                .and_then(|s| s.strip_suffix(')'))
            {
                if let Ok(n) = inner.trim().parse::<u32>() {
                    return Some(PgType::VarChar(n));
                }
            }
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pg_type() {
        assert_eq!(parse_pg_type("INTEGER"), Some(PgType::Integer));
        assert_eq!(parse_pg_type("int"), Some(PgType::Integer));
        assert_eq!(parse_pg_type("BIGINT"), Some(PgType::BigInt));
        assert_eq!(parse_pg_type("double precision"), Some(PgType::DoublePrecision));
        assert_eq!(parse_pg_type("float"), Some(PgType::DoublePrecision));
        assert_eq!(parse_pg_type("BOOLEAN"), Some(PgType::Boolean));
        assert_eq!(parse_pg_type("bool"), Some(PgType::Boolean));
        assert_eq!(parse_pg_type("UUID"), Some(PgType::Uuid));
        assert_eq!(parse_pg_type("DATE"), Some(PgType::Date));
        assert_eq!(parse_pg_type("TIMESTAMP"), Some(PgType::Timestamp));
        assert_eq!(parse_pg_type("TEXT"), Some(PgType::Text));
        assert_eq!(parse_pg_type("VARCHAR(128)"), Some(PgType::VarChar(128)));
        assert_eq!(parse_pg_type("CHARACTER VARYING(64)"), Some(PgType::VarChar(64)));
        assert_eq!(parse_pg_type("NONSENSE"), None);
    }

    // --- ConfigWarning ---

    #[test]
    fn config_warning_to_message_unknown_table() {
        let w = ConfigWarning::UnknownTable("orders".to_string());
        assert!(w.to_message().contains("orders"));
        assert!(w.to_message().contains("not found"));
    }

    #[test]
    fn config_warning_to_message_unknown_column() {
        let w = ConfigWarning::UnknownColumn { table: "users".to_string(), column: "age".to_string() };
        let msg = w.to_message();
        assert!(msg.contains("users"));
        assert!(msg.contains("age"));
    }

    #[test]
    fn config_warning_to_message_unknown_type() {
        let w = ConfigWarning::UnknownType {
            table: "users".to_string(),
            column: "age".to_string(),
            type_str: "BADTYPE".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("BADTYPE"));
        assert!(msg.contains("users"));
        assert!(msg.contains("age"));
    }

    #[test]
    fn config_warning_to_message_unknown_strategy() {
        let w = ConfigWarning::UnknownStrategy { table: "tags".to_string(), strategy: "magic".to_string() };
        let msg = w.to_message();
        assert!(msg.contains("magic"));
        assert!(msg.contains("tags"));
    }

    #[test]
    fn config_warning_to_message_group_merge_failed() {
        let w = ConfigWarning::GroupMergeFailed { group: "merged".to_string(), found: 1, expected: 3 };
        let msg = w.to_message();
        assert!(msg.contains("merged"));
        assert!(msg.contains('1'));
        assert!(msg.contains('3'));
    }

    #[test]
    fn config_warning_clone_and_eq() {
        let w = ConfigWarning::UnknownTable("t".to_string());
        assert_eq!(w, ConfigWarning::UnknownTable("t".to_string()));
        assert_ne!(w, ConfigWarning::UnknownTable("other".to_string()));
    }

    // --- apply_overrides + helpers (return Vec<ConfigWarning>) ---

    #[test]
    fn apply_overrides_unknown_table_returns_warning() {
        let mut schemas = vec![simple_table("users")];
        let mut tables = HashMap::new();
        tables.insert("ghost".to_string(), HashMap::new());
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownTable("ghost".to_string())]);
    }

    #[test]
    fn apply_overrides_unknown_column_returns_warning() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};
        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "name".to_string(), original_name: "name".to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
            s
        }];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("ghost_col".to_string(), toml::Value::String("TEXT".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownColumn {
            table: "users".to_string(), column: "ghost_col".to_string(),
        }]);
    }

    #[test]
    fn apply_overrides_unknown_type_returns_warning() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};
        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "age".to_string(), original_name: "age".to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
            s
        }];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("age".to_string(), toml::Value::String("NONSENSE".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownType {
            table: "users".to_string(), column: "age".to_string(), type_str: "NONSENSE".to_string(),
        }]);
    }

    #[test]
    fn apply_overrides_unknown_strategy_returns_warning() {
        let mut schemas = vec![simple_table("tags")];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("strategy".to_string(), toml::Value::String("magic".to_string()));
        tables.insert("tags".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownStrategy {
            table: "tags".to_string(), strategy: "magic".to_string(),
        }]);
    }

    #[test]
    fn apply_overrides_valid_override_no_warnings() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};
        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "age".to_string(), original_name: "age".to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
            s
        }];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("age".to_string(), toml::Value::String("INTEGER".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_apply_overrides() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};

        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "age".to_string(),
                original_name: "age".to_string(),
                pg_type: PgType::Text,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
            s
        }];

        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("age".to_string(), toml::Value::String("INTEGER".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty(), "valid override must produce no warnings");

        let col = schemas[0].columns.iter().find(|c| c.name == "age").unwrap();
        assert_eq!(col.pg_type, PgType::Integer);
    }

    fn toml_strategy(strategy: &str) -> HashMap<String, toml::Value> {
        let mut m = HashMap::new();
        m.insert("strategy".to_string(), toml::Value::String(strategy.to_string()));
        m
    }

    #[test]
    fn toml_columns_on_columns_table_sets_toml_override() {
        let mut schemas = vec![simple_table("flat")];
        // inferred as Columns (default for simple_table)
        let mut tables = HashMap::new();
        tables.insert("flat".to_string(), toml_strategy("columns"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty(), "no warning when inferred is already Columns");
        assert_eq!(schemas[0].toml_override(), Some(&UserOverride::Columns));
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns);
    }

    #[test]
    fn toml_columns_on_pivot_table_returns_invalid_override_warning() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("metrics")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Pivot);
        schemas[0].inferred_strategy = InferredStrategy::Pivot;
        let mut tables = HashMap::new();
        tables.insert("metrics".to_string(), toml_strategy("columns"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(&warnings[0], ConfigWarning::InvalidOverride { table, .. } if table == "metrics"));
        assert_eq!(schemas[0].toml_override(), None, "toml_override must not be set on invalid override");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Pivot, "inferred_strategy must be unchanged");
    }

    #[test]
    fn toml_columns_on_jsonb_table_returns_invalid_override_warning() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("events")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Jsonb);
        schemas[0].inferred_strategy = InferredStrategy::Jsonb;
        let mut tables = HashMap::new();
        tables.insert("events".to_string(), toml_strategy("columns"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(&warnings[0], ConfigWarning::InvalidOverride { table, .. } if table == "events"));
    }

    #[test]
    fn toml_pivot_sets_toml_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("tags")];
        schemas[0].inferred_strategy = InferredStrategy::Columns;
        let mut tables = HashMap::new();
        tables.insert("tags".to_string(), toml_strategy("pivot"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(schemas[0].toml_override(), Some(&UserOverride::Pivot));
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Pivot);
    }

    #[test]
    fn toml_jsonb_sets_toml_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("blob")];
        schemas[0].inferred_strategy = InferredStrategy::Columns;
        let mut tables = HashMap::new();
        tables.insert("blob".to_string(), toml_strategy("jsonb"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(schemas[0].toml_override(), Some(&UserOverride::Jsonb));
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Jsonb);
    }

    #[test]
    fn toml_normalize_dynamic_keys_sets_toml_override_and_preserves_inferred() {
        use crate::schema::table_schema::{ChildKind, ColumnSchema};
        let mut parent = simple_table("images");
        parent.inferred_strategy = InferredStrategy::Columns;
        parent.columns.push(ColumnSchema::generated("j2s_id", PgType::BigInt));
        // NormalizeDynamicKeys requires at least one Object child
        let mut child = TableSchema::new("images_12584".to_string(), vec!["images".to_string(), "12584".to_string()], 1);
        child.parent_table = Some("images".to_string());
        child.child_kind = Some(ChildKind::Object);
        let mut schemas = vec![parent, child];
        let mut cols = HashMap::new();
        cols.insert("strategy".to_string(), toml::Value::String("normalize_dynamic_keys".to_string()));
        cols.insert("id_column".to_string(), toml::Value::String("image_id".to_string()));
        let mut tables = HashMap::new();
        tables.insert("images".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides(&mut schemas, &config).unwrap();
        let images = schemas.iter().find(|s| s.name == "images").unwrap();
        assert_eq!(images.inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(
            images.toml_override(),
            Some(&UserOverride::NormalizeDynamicKeys { id_column: "image_id".into() })
        );
        assert_eq!(
            *images.effective_strategy(),
            InferredStrategy::NormalizeDynamicKeys { id_column: "image_id".into() }
        );
    }

    // --- Guard 1: strategy + suffix_columns conflict (#31) ---

    fn cols_with_strategy_and_suffix(strategy: &str) -> HashMap<String, toml::Value> {
        let mut cols = HashMap::new();
        cols.insert("strategy".to_string(), toml::Value::String(strategy.to_string()));
        cols.insert(
            "suffix_columns".to_string(),
            toml::Value::Array(vec![toml::Value::String("_100g".to_string())]),
        );
        cols
    }

    #[test]
    fn toml_conflicting_strategy_and_suffix_columns_ignores_strategy() {
        let mut schemas = vec![simple_table("nutrients")];
        let mut tables = HashMap::new();
        tables.insert("nutrients".to_string(), cols_with_strategy_and_suffix("pivot"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert_eq!(warnings, vec![ConfigWarning::ConflictingOverride {
            table: "nutrients".to_string(),
            ignored_strategy: "pivot".to_string(),
        }]);
        assert_eq!(schemas[0].toml_override(), None, "strategy must be ignored, not applied");
        assert!(
            matches!(schemas[0].inferred_strategy, InferredStrategy::StructuredPivot(_)),
            "suffix_columns must still apply normally"
        );
    }

    #[test]
    fn toml_structured_pivot_strategy_with_suffix_columns_is_not_a_conflict() {
        let mut schemas = vec![simple_table("nutrients")];
        let mut tables = HashMap::new();
        // Mixed case must match the .to_lowercase() comparison used by apply_strategy_override.
        tables.insert("nutrients".to_string(), cols_with_strategy_and_suffix("Structured_Pivot"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert!(warnings.is_empty(), "structured_pivot + suffix_columns is the documented normal combo");
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::StructuredPivot(_)));
    }

    #[test]
    fn toml_unknown_strategy_with_suffix_columns_still_emits_unknown_strategy() {
        let mut schemas = vec![simple_table("nutrients")];
        let mut tables = HashMap::new();
        tables.insert("nutrients".to_string(), cols_with_strategy_and_suffix("pvot"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert_eq!(warnings, vec![ConfigWarning::UnknownStrategy {
            table: "nutrients".to_string(),
            strategy: "pvot".to_string(),
        }], "a genuine typo must not be masked by the conflict guard");
        assert!(
            matches!(schemas[0].inferred_strategy, InferredStrategy::StructuredPivot(_)),
            "suffix_columns must still apply even though strategy was invalid"
        );
    }

    #[test]
    fn toml_non_string_strategy_with_suffix_columns_is_not_a_conflict() {
        let mut schemas = vec![simple_table("nutrients")];
        let mut cols = HashMap::new();
        cols.insert("strategy".to_string(), toml::Value::Integer(42));
        cols.insert(
            "suffix_columns".to_string(),
            toml::Value::Array(vec![toml::Value::String("_100g".to_string())]),
        );
        let mut tables = HashMap::new();
        tables.insert("nutrients".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert!(warnings.is_empty(), "a non-string strategy value is a no-op, not a real conflict");
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::StructuredPivot(_)));
    }

    #[test]
    fn toml_normalize_dynamic_keys_with_suffix_columns_ignores_strategy() {
        use crate::schema::table_schema::{ChildKind, ColumnSchema};
        let mut parent = simple_table("images");
        parent.columns.push(ColumnSchema::generated("j2s_id", PgType::BigInt));
        let mut child = TableSchema::new("images_12584".to_string(), vec!["images".to_string(), "12584".to_string()], 1);
        child.parent_table = Some("images".to_string());
        child.child_kind = Some(ChildKind::Object);
        let mut schemas = vec![parent, child];
        let mut cols = cols_with_strategy_and_suffix("normalize_dynamic_keys");
        cols.insert("id_column".to_string(), toml::Value::String("image_id".to_string()));
        let mut tables = HashMap::new();
        tables.insert("images".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert_eq!(warnings, vec![ConfigWarning::ConflictingOverride {
            table: "images".to_string(),
            ignored_strategy: "normalize_dynamic_keys".to_string(),
        }]);
        let images = schemas.iter().find(|s| s.name == "images").unwrap();
        assert_eq!(images.toml_override(), None, "the deferred normalize_dynamic_keys must not be scheduled");
        assert!(matches!(images.inferred_strategy, InferredStrategy::StructuredPivot(_)));
        // Only two schemas: parent + its original child. No derived "_by_key" table was created.
        assert_eq!(schemas.len(), 2);
    }

    #[test]
    fn config_warning_conflicting_override_to_message() {
        let w = ConfigWarning::ConflictingOverride {
            table: "nutrients".to_string(),
            ignored_strategy: "pivot".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("nutrients"));
        assert!(msg.contains("pivot"));
        assert!(msg.contains("suffix_columns"));
    }

    fn simple_table(name: &str) -> TableSchema {
        TableSchema::new(name.to_string(), vec![name.to_string()], 0)
    }

    // --- Guard 2: suffix_columns requires inferred_strategy == Columns (#31) ---

    fn suffix_columns_only(suffix: &str) -> HashMap<String, toml::Value> {
        let mut cols = HashMap::new();
        cols.insert(
            "suffix_columns".to_string(),
            toml::Value::Array(vec![toml::Value::String(suffix.to_string())]),
        );
        cols
    }

    #[test]
    fn toml_suffix_columns_on_pivot_table_returns_invalid_override_warning() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("metrics")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Pivot);
        schemas[0].inferred_strategy = InferredStrategy::Pivot;
        let column_names_before: Vec<String> = schemas[0].columns.iter().map(|c| c.name.clone()).collect();
        let mut tables = HashMap::new();
        tables.insert("metrics".to_string(), suffix_columns_only("_100g"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert_eq!(warnings.len(), 1);
        assert!(matches!(&warnings[0],
            ConfigWarning::InvalidOverride { table, from_strategy, to_strategy }
            if table == "metrics" && from_strategy == "Pivot" && to_strategy == "structured_pivot"
        ));
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Pivot, "strategy must be unchanged");
        let column_names_after: Vec<String> = schemas[0].columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(column_names_after, column_names_before, "columns must be unchanged");
    }

    #[test]
    fn toml_suffix_columns_on_columns_table_no_warning_non_regression() {
        // Non-regression for #28: suffix_columns alone on a freshly-inferred Columns table
        // must still be accepted with no warning.
        let mut schemas = vec![simple_table("nutrients")];
        let mut tables = HashMap::new();
        tables.insert("nutrients".to_string(), suffix_columns_only("_100g"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert!(warnings.is_empty());
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::StructuredPivot(_)));
    }

    #[test]
    fn toml_conflicting_strategy_and_suffix_columns_on_non_columns_table_stacks_both_warnings() {
        // Both guards target the same table config in one pass: guard 1 (strategy vs.
        // suffix_columns conflict) and guard 2 (suffix_columns requires Columns) fire
        // independently — see #31 "Questions ouvertes". Decision: no dedup, both warnings
        // surface, and the table keeps its original (non-Columns) layout untouched.
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("metrics")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Pivot);
        schemas[0].inferred_strategy = InferredStrategy::Pivot;
        let column_names_before: Vec<String> = schemas[0].columns.iter().map(|c| c.name.clone()).collect();
        let mut tables = HashMap::new();
        tables.insert("metrics".to_string(), cols_with_strategy_and_suffix("pivot"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();

        assert_eq!(warnings, vec![
            ConfigWarning::ConflictingOverride { table: "metrics".to_string(), ignored_strategy: "pivot".to_string() },
            ConfigWarning::InvalidOverride {
                table: "metrics".to_string(),
                from_strategy: "Pivot".to_string(),
                to_strategy: "structured_pivot".to_string(),
            },
        ]);
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Pivot, "table keeps its original layout");
        let column_names_after: Vec<String> = schemas[0].columns.iter().map(|c| c.name.clone()).collect();
        assert_eq!(column_names_after, column_names_before, "columns must be unchanged");
    }

    #[test]
    fn toml_suffix_columns_survives_apply_overrides_complete() {
        let mut schemas = vec![simple_table("nutrients")];
        let mut cols = HashMap::new();
        cols.insert(
            "suffix_columns".to_string(),
            toml::Value::Array(vec![toml::Value::String("_100g".to_string())]),
        );
        let mut tables = HashMap::new();
        tables.insert("nutrients".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };

        apply_overrides_complete(&mut schemas, &config).unwrap();

        let nutrients = &schemas[0];
        assert!(matches!(nutrients.inferred_strategy, InferredStrategy::StructuredPivot(_)));
        assert_eq!(nutrients.toml_override(), None, "suffix_columns has no UserOverride variant to set");
        assert!(matches!(*nutrients.effective_strategy(), InferredStrategy::StructuredPivot(_)));
    }

    // --- apply_group_overrides (return Vec<ConfigWarning>) ---

    #[test]
    fn apply_group_overrides_merge_failure_returns_warning() {
        let mut schemas = vec![simple_table("a")];
        let mut group = HashMap::new();
        group.insert("merged".to_string(), GroupConfig {
            strategy: "keyed_pivot".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables: HashMap::new(), group };
        let warnings = apply_group_overrides(&mut schemas, &config);
        assert_eq!(warnings, vec![ConfigWarning::GroupMergeFailed {
            group: "merged".to_string(),
            found: 1,
            expected: 2,
        }]);
    }

    #[test]
    fn apply_group_overrides_unknown_strategy_returns_warning() {
        let mut schemas = vec![simple_table("a"), simple_table("b")];
        let mut group = HashMap::new();
        group.insert("merged".to_string(), GroupConfig {
            strategy: "magic".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables: HashMap::new(), group };
        let warnings = apply_group_overrides(&mut schemas, &config);
        assert_eq!(warnings, vec![ConfigWarning::UnknownGroupStrategy {
            group: "merged".to_string(),
            strategy: "magic".to_string(),
        }]);
    }

    #[test]
    fn apply_group_overrides_valid_merge_no_warnings() {
        let mut schemas = vec![simple_table("a"), simple_table("b")];
        let mut group = HashMap::new();
        group.insert("merged".to_string(), GroupConfig {
            strategy: "keyed_pivot".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables: HashMap::new(), group };
        let warnings = apply_group_overrides(&mut schemas, &config);
        assert!(warnings.is_empty());
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "merged");
    }

    #[test]
    fn config_warning_to_message_unknown_group_strategy() {
        let w = ConfigWarning::UnknownGroupStrategy {
            group: "g1".to_string(),
            strategy: "magic".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("magic"));
        assert!(msg.contains("g1"));
    }

    #[test]
    fn config_warning_invalid_override_to_message() {
        let w = ConfigWarning::InvalidOverride {
            table: "metrics".to_string(),
            from_strategy: "Pivot".to_string(),
            to_strategy: "columns".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("metrics"));
        assert!(msg.contains("Pivot"));
        assert!(msg.contains("columns"));
    }

    #[test]
    fn config_warning_invalid_override_clone_and_eq() {
        let w = ConfigWarning::InvalidOverride {
            table: "t".to_string(),
            from_strategy: "Jsonb".to_string(),
            to_strategy: "columns".to_string(),
        };
        assert_eq!(w.clone(), w);
        let w2 = ConfigWarning::InvalidOverride {
            table: "other".to_string(),
            from_strategy: "Jsonb".to_string(),
            to_strategy: "columns".to_string(),
        };
        assert_ne!(w, w2);
    }

    // --- apply_overrides_complete (aggregates both warning sources) ---

    #[test]
    fn apply_overrides_complete_aggregates_both_sources() {
        let mut schemas = vec![simple_table("users")];
        let mut tables = HashMap::new();
        tables.insert("ghost".to_string(), HashMap::new());
        let mut group = HashMap::new();
        group.insert("g".to_string(), GroupConfig {
            strategy: "keyed_pivot".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { group, tables };
        let warnings = apply_overrides_complete(&mut schemas, &config).unwrap();
        assert!(warnings.iter().any(|w| matches!(w, ConfigWarning::UnknownTable(_))));
        assert!(warnings.iter().any(|w| matches!(w, ConfigWarning::GroupMergeFailed { .. })));
    }

    #[test]
    fn apply_overrides_complete_no_warnings_clean_config() {
        let mut schemas = vec![simple_table("users")];
        let config = SchemaConfig { tables: HashMap::new(), group: HashMap::new() };
        let warnings = apply_overrides_complete(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty());
    }

    /// A `toml_override` applied via `apply_overrides_complete()`, after `finalize()` already ran,
    /// must be visible in `effective_strategy()`.
    #[test]
    fn full_pipeline_toml_override_after_finalize_is_visible_in_effective_strategy() {
        use crate::schema::registry::{RegistryConfig, SchemaRegistry};

        let mut reg = SchemaRegistry::new(RegistryConfig::default());
        let obj = serde_json::json!({"name": "Alice", "age": 30});
        reg.observe_root("users", obj.as_object().unwrap());
        let mut schemas = reg.finalize();

        let mut tables = HashMap::new();
        tables.insert("users".to_string(), toml_strategy("jsonb"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides_complete(&mut schemas, &config).unwrap();

        let users = schemas.iter().find(|s| s.name == "users").unwrap();
        assert_eq!(
            *users.effective_strategy(),
            InferredStrategy::Jsonb,
            "toml_override applied after finalize() must be visible in effective_strategy() after the full pipeline"
        );
    }

    /// `exclude_absorbed_children()` (called inside `apply_overrides_complete`) reads
    /// `absorbs_children()` → `effective_strategy()`, so a `toml_override` applied earlier in the
    /// same call that flips `absorbs_children()` to true must already be visible to it.
    #[test]
    fn apply_overrides_complete_excludes_children_absorbed_by_a_new_override() {
        let mut parent = simple_table("images");
        parent.inferred_strategy = InferredStrategy::Columns;
        let mut child = simple_table("images_items");
        child.parent_table = Some("images".to_string());
        let mut schemas = vec![parent, child];

        let mut tables = HashMap::new();
        tables.insert("images".to_string(), toml_strategy("jsonb"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        apply_overrides_complete(&mut schemas, &config).unwrap();

        assert!(
            schemas.iter().all(|s| s.name != "images_items"),
            "images_items must be excluded once images' toml_override=Jsonb makes it absorb children"
        );
    }

    #[test]
    fn apply_user_overrides_skip_removes_table() {
        let mut schemas = vec![simple_table("a"), simple_table("b")];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "b");
    }

    #[test]
    fn apply_user_overrides_skip_removes_autosplit_companion() {
        let mut t = simple_table("products");
        t.inferred_strategy = InferredStrategy::AutoSplit {
            stable_threshold: 0.8,
            rare_threshold: 0.01,
            medium_keys: Default::default(),
            wide_table_name: "products_wide".to_string(),
        };
        let mut schemas = vec![t, simple_table("products_wide"), simple_table("orders")];
        let mut overrides = HashMap::new();
        overrides.insert("products".to_string(), UserOverride::Skip);
        apply_user_overrides(&mut schemas, &overrides);
        assert!(!schemas.iter().any(|s| s.name == "products"),       "main table removed");
        assert!(!schemas.iter().any(|s| s.name == "products_wide"),  "companion _wide removed");
        assert!(schemas.iter().any(|s| s.name == "orders"),          "unrelated table kept");
    }

    #[test]
    fn apply_user_overrides_skip_cascades_one_level_real_child() {
        let parent = simple_table("commandes");
        let mut child = simple_table("commandes_lignes");
        child.parent_table = Some("commandes".to_string());
        let mut schemas = vec![parent, child];
        let mut overrides = HashMap::new();
        overrides.insert("commandes".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert!(schemas.is_empty(), "parent and its real child must both be removed");
        assert_eq!(warnings, vec![SkipCascadeWarning {
            removed_table: "commandes".to_string(),
            cascaded_children: vec!["commandes_lignes".to_string()],
        }]);
    }

    #[test]
    fn apply_user_overrides_skip_cascades_multiple_levels() {
        let a = simple_table("a");
        let mut b = simple_table("b");
        b.parent_table = Some("a".to_string());
        let mut c = simple_table("c");
        c.parent_table = Some("b".to_string());
        let mut schemas = vec![a, b, c];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert!(schemas.is_empty(), "all three levels must cascade");
        assert_eq!(warnings, vec![SkipCascadeWarning {
            removed_table: "a".to_string(),
            cascaded_children: vec!["b".to_string(), "c".to_string()],
        }]);
    }

    #[test]
    fn apply_user_overrides_skip_cascades_via_autosplit_wide_companion() {
        let mut t = simple_table("products");
        t.inferred_strategy = InferredStrategy::AutoSplit {
            stable_threshold: 0.8,
            rare_threshold: 0.01,
            medium_keys: Default::default(),
            wide_table_name: "products_wide".to_string(),
        };
        let mut wide_child = simple_table("products_wide_tags");
        wide_child.parent_table = Some("products_wide".to_string());
        let mut schemas = vec![t, simple_table("products_wide"), wide_child, simple_table("orders")];
        let mut overrides = HashMap::new();
        overrides.insert("products".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "orders");
        assert_eq!(warnings, vec![SkipCascadeWarning {
            removed_table: "products".to_string(),
            cascaded_children: vec!["products_wide".to_string(), "products_wide_tags".to_string()],
        }]);
    }

    #[test]
    fn apply_user_overrides_skip_without_real_children_emits_no_warning() {
        let mut schemas = vec![simple_table("logs")];
        let mut overrides = HashMap::new();
        overrides.insert("logs".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert!(schemas.is_empty());
        assert!(warnings.is_empty(), "a Skip with no real children must not produce a cascade warning");
    }

    #[test]
    fn apply_user_overrides_skip_root_table_cascades_without_blocking() {
        let root = simple_table("root");
        let mut child_d = simple_table("d");
        child_d.parent_table = Some("root".to_string());
        let mut child_e = simple_table("e");
        child_e.parent_table = Some("root".to_string());
        let mut schemas = vec![root, child_d, child_e];
        let mut overrides = HashMap::new();
        overrides.insert("root".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert!(schemas.is_empty(), "skipping the root table is not blocked, even though it empties the schema");
        assert_eq!(warnings, vec![SkipCascadeWarning {
            removed_table: "root".to_string(),
            cascaded_children: vec!["d".to_string(), "e".to_string()],
        }]);
    }

    #[test]
    fn apply_user_overrides_skip_direct_and_cascaded_on_same_table_no_duplicate() {
        let a = simple_table("a");
        let mut b = simple_table("b");
        b.parent_table = Some("a".to_string());
        let mut schemas = vec![a, b];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        overrides.insert("b".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert!(schemas.is_empty());
        assert!(
            warnings.is_empty(),
            "b was directly Skip-ed by the user, not a hidden cascade side effect of a — no warning to emit"
        );
    }

    #[test]
    fn apply_user_overrides_skip_cascade_warnings_are_deterministically_sorted() {
        let z = simple_table("z");
        let mut z_child = simple_table("z_child");
        z_child.parent_table = Some("z".to_string());
        let a = simple_table("a");
        let mut a_child = simple_table("a_child");
        a_child.parent_table = Some("a".to_string());
        // Deliberately not sorted alphabetically nor grouped by root.
        let mut schemas = vec![z, z_child, a, a_child];
        let mut overrides = HashMap::new();
        overrides.insert("z".to_string(), UserOverride::Skip);
        overrides.insert("a".to_string(), UserOverride::Skip);

        let warnings = apply_user_overrides(&mut schemas, &overrides);

        assert_eq!(
            warnings,
            vec![
                SkipCascadeWarning { removed_table: "a".to_string(), cascaded_children: vec!["a_child".to_string()] },
                SkipCascadeWarning { removed_table: "z".to_string(), cascaded_children: vec!["z_child".to_string()] },
            ],
            "warnings must be sorted by removed_table regardless of HashMap/schemas insertion order"
        );
    }

    #[test]
    fn apply_user_overrides_pivot_sets_ui_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("tags")];
        let mut overrides = HashMap::new();
        overrides.insert("tags".to_string(), UserOverride::Pivot);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].ui_override(), Some(&UserOverride::Pivot), "ui_override must be set");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Pivot);
        // Column layout must have been restructured (EAV: key + value)
        assert!(schemas[0].columns.iter().any(|c| c.name == "key"), "Pivot layout: key column expected");
        assert!(schemas[0].columns.iter().any(|c| c.name == "value"), "Pivot layout: value column expected");
    }

    #[test]
    fn apply_user_overrides_jsonb_sets_ui_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("blob")];
        let mut overrides = HashMap::new();
        overrides.insert("blob".to_string(), UserOverride::Jsonb);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas[0].ui_override(), Some(&UserOverride::Jsonb), "ui_override must be set");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Jsonb);
        assert!(schemas[0].columns.iter().any(|c| c.name == "data"), "Jsonb layout: data column expected");
    }

    #[test]
    fn apply_user_overrides_pivot_does_not_overwrite_existing_ui_override() {
        let mut schemas = vec![simple_table("tags")];
        schemas[0].set_ui_override(Some(UserOverride::Jsonb)); // already set from snapshot
        let mut overrides = HashMap::new();
        overrides.insert("tags".to_string(), UserOverride::Pivot); // strategy_overrides says Pivot
        apply_user_overrides(&mut schemas, &overrides);
        // Guard: ui_override.is_none() — existing Jsonb override must be preserved
        assert_eq!(schemas[0].ui_override(), Some(&UserOverride::Jsonb), "existing ui_override must not be overwritten");
    }

    #[test]
    fn apply_user_overrides_columns_on_columns_table_sets_ui_override() {
        let mut schemas = vec![simple_table("flat")];
        // simple_table infers Columns by default
        let mut overrides = HashMap::new();
        overrides.insert("flat".to_string(), UserOverride::Columns);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas[0].ui_override(), Some(&UserOverride::Columns));
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns);
    }

    #[test]
    fn apply_user_overrides_columns_on_pivot_table_is_no_op() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("metrics")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Pivot);
        schemas[0].inferred_strategy = InferredStrategy::Pivot;
        let mut overrides = HashMap::new();
        overrides.insert("metrics".to_string(), UserOverride::Columns);
        apply_user_overrides(&mut schemas, &overrides);
        // No ui_override should be set — incompatible layout
        assert_eq!(schemas[0].ui_override(), None, "Columns override on Pivot must not set ui_override");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Pivot, "inferred_strategy unchanged");
    }

    #[test]
    fn apply_user_overrides_columns_does_not_overwrite_existing_ui_override() {
        let mut schemas = vec![simple_table("flat")];
        schemas[0].set_ui_override(Some(UserOverride::Jsonb));
        let mut overrides = HashMap::new();
        overrides.insert("flat".to_string(), UserOverride::Columns);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas[0].ui_override(), Some(&UserOverride::Jsonb), "existing ui_override preserved");
    }

    #[test]
    fn apply_user_overrides_no_override_unchanged() {
        let mut schemas = vec![simple_table("unchanged")];
        let overrides: HashMap<String, UserOverride> = HashMap::new();
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::Columns));
    }

    /// TUI (`json2sql-ui`) manual strategy selection must be reflected in `effective_strategy()`.
    #[test]
    fn apply_user_overrides_reflected_in_effective_strategy() {
        let mut schemas = vec![simple_table("blob")];

        let mut overrides = HashMap::new();
        overrides.insert("blob".to_string(), UserOverride::Jsonb);
        apply_user_overrides(&mut schemas, &overrides);

        assert_eq!(
            *schemas[0].effective_strategy(),
            InferredStrategy::Jsonb,
            "manual TUI strategy selection must be reflected in effective_strategy()"
        );
    }
}
