//! Core domain types for the finalized schema produced by Pass 1.
//!
//! The two central types are [`TableSchema`] (one SQL table and its materialization strategy)
//! and [`ColumnSchema`] (one column with inferred `PostgreSQL` type). Both are serialized to the
//! inspect JSON written between Pass 1 and Pass 2.
//!
//! ## `InferredStrategy` and `UserOverride`
//!
//! Each [`TableSchema`] carries an [`InferredStrategy`] that determines how Pass 2 materializes
//! the table in `PostgreSQL`. The strategy is inferred by `finalize()` at the end of Pass 1.
//! The user can override it through the IHM via [`UserOverride`] before running Pass 2.
//!
//! | Strategy | Kind | When used | Output shape |
//! |---|---|---|---|
//! | `Columns` | inferred | default | one column per JSON key |
//! | `Pivot` | inferred / user | homogeneous values, dynamic keys | `(key TEXT, value T)` EAV |
//! | `Jsonb` | inferred / user | heterogeneous / irregular structure | single `JSONB` column |
//! | `StructuredPivot` | inferred | keys share prefix + typed suffixes | one row per prefix group |
//! | `KeyedPivot` | inferred | N sibling tables, same schema | merged table with key column |
//! | `MultiKeyedPivot` | inferred | siblings with mixed key shapes | two synthetic pivot tables |
//! | `AutoSplit` | inferred | root table with bi-modal key frequency | main table + `_wide` companion |
//! | `Ignore` | inferred | key appears in < `rare_threshold` of rows | excluded from schema |
//! | `Skip` | user | user wants table removed | table excluded from DDL and Pass 2 |
//! | `NormalizeDynamicKeys` | TOML-only | arbitrary JSON keys → row IDs | key becomes a column |
//! | `Flatten` | TOML-only | inline child fields into parent | child table removed |
//! | `JsonbFlatten` | TOML-only | store child as JSONB on parent | child table removed |
//!
//! See each variant's doc comment for a concrete JSON → SQL example.

use std::collections::HashMap;

use super::naming::PG_TABLE_MAX_IDENT;
use super::type_tracker::PgType;

/// One suffix column in a `StructuredPivot` table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SuffixColumn {
    /// The raw suffix string as it appears in JSON keys, e.g. "_100g"
    pub suffix: String,
    /// The `PostgreSQL` column name derived from the suffix, e.g. "`c_100g`"
    pub col_name: String,
    /// The inferred `PostgreSQL` type for this suffix column
    pub pg_type: PgType,
}

/// Describes the suffix decomposition detected for a `StructuredPivot` table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SuffixSchema {
    /// Detected suffix columns, sorted by suffix string
    pub suffix_cols: Vec<SuffixColumn>,
    /// `PostgreSQL` type for the "base value" column (key with no suffix)
    pub value_type: PgType,
}

/// Shape of the sibling keys — used to name the key column semantically.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum KeyShape {
    /// All keys are pure integers: "1", "2", "42"
    Numeric,
    /// All keys are 2-3 char alpha codes (ISO language/country): "fr", "en", "deu"
    IsoLang,
    /// Keys are slugs or compound strings: "`en_glass`", "`palm_oil`"
    Slug,
    /// Mix of numeric and ISO codes
    Mixed,
}

fn default_data_col_name() -> String {
    "j2s_data".to_string()
}

/// Metadata for a `KeyedPivot` table (sibling tables collapsed into one).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SiblingSchema {
    /// Name of the column that holds the original sibling key (e.g. "`key_id`", "`lang_code`", "key")
    pub key_col_name: String,
    /// Detected shape of the sibling keys
    pub key_shape: KeyShape,
    /// True when the collapsed siblings were `ObjectArray` children (each key maps to an array of
    /// objects). Pass 2 iterates the array and emits one row per element with `j2s_order`.
    #[serde(default)]
    pub array_children: bool,
    /// Name of the JSONB column that stores the raw child object/array.
    /// Defaults to "data"; falls back to "`j2s_data`" if "data" collides with a union column.
    #[serde(default = "default_data_col_name")]
    pub data_col_name: String,
}

/// One key-shape subgroup within a `MultiKeyedPivot` parent.
/// Each group produces its own synthetic pivot table in the schema.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SiblingGroup {
    /// Name of the synthetic pivot table (e.g. "`products_images_num`").
    pub pivot_table: String,
    /// If true, this group handles numeric-keyed children; false = non-numeric.
    pub key_is_numeric: bool,
    /// Pivot column metadata (key column name, key shape, etc.).
    pub sibling_schema: SiblingSchema,
    /// Names of the original child tables absorbed into this group.
    /// Used by `exclude_absorbed_children` to remove them from the schema.
    pub absorbed_names: Vec<String>,
    /// Path segment used for this group in the schema path and `path_map` lookup
    /// (e.g. `"num"`, `"key"`, `"cluster_0"`). Empty in old snapshots — Pass 2 falls
    /// back to the `key_is_numeric` heuristic when this is empty.
    #[serde(default)]
    pub path_segment: String,
    /// Path last-segments of the child schemas absorbed into this cluster
    /// (i.e. the JSON keys seen in Pass 1). Used at Pass 2 to route runtime keys
    /// to the correct cluster when multiple non-numeric groups exist.
    /// Empty in old snapshots — falls back to `key_is_numeric` heuristic.
    #[serde(default)]
    pub absorbed_path_segments: Vec<String>,
}

/// System-inferred strategy for handling "wide" tables — tables with many dynamic keys.
///
/// Set by `finalize()` at the end of Pass 1. Not directly editable by the user; the user
/// chooses a [`UserOverride`] instead, which is applied before Pass 2 via `apply_user_overrides`.
///
/// See the [module-level table](self) for a quick comparison and each variant's
/// doc comment for a concrete JSON → SQL example.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub enum InferredStrategy {
    /// Default: one SQL column per JSON key.
    ///
    /// Trigger: fewer than `wide_column_threshold` distinct keys observed across all rows.
    ///
    /// JSON: `{ "name": "Alice", "age": 30 }` in table `users`
    /// → `users(name TEXT, age INTEGER)` — one row per JSON object.
    #[default]
    Columns,

    /// EAV pivot: one row per key-value pair — columns: `(key TEXT, value <type>)`.
    ///
    /// Trigger: many dynamic keys, all values share a compatible `PostgreSQL` type
    /// (e.g. a nutrient map where every value is a FLOAT).
    ///
    /// JSON: `{ "nutrients": { "energy": 250, "fat": 12, "salt": 0.3 } }`
    /// → `products_nutrients(j2s_parent_id, key TEXT, value FLOAT)`
    /// → rows: `("energy", 250)`, `("fat", 12)`, `("salt", 0.3)`.
    Pivot,

    /// Store the entire object as a single JSONB column.
    ///
    /// Trigger: values are heterogeneous (mixed types) or structure varies too much for EAV.
    ///
    /// JSON: `{ "meta": { "v": 2, "tags": ["a","b"], "active": true } }`
    /// → `products_meta(j2s_parent_id, meta JSONB)`
    /// → one row: `('{"v":2,"tags":["a","b"],"active":true}')`.
    Jsonb,

    /// Structured pivot: keys share a common prefix; their suffixes become typed columns.
    ///
    /// Trigger: suffix detector finds ≥ N keys following a `{base}` / `{base}_{suffix}` pattern
    /// (e.g. `energy`, `energy_100g`, `energy_unit`).
    ///
    /// JSON: `{ "energy": 250, "energy_100g": 1046, "energy_unit": "kcal",
    ///          "fat": 12,     "fat_100g": 50,       "fat_unit": "g" }`
    /// → `nutrients(j2s_parent_id, j2s_key TEXT, value INTEGER, per_100g FLOAT, unit TEXT)`
    /// → rows: `("energy", 250, 1046, "kcal")`, `("fat", 12, 50, "g")`.
    StructuredPivot(SuffixSchema),

    /// Sibling collapse: N structurally similar child tables are merged into one canonical table.
    ///
    /// Trigger: ≥ `sibling_threshold` child tables whose column sets have pairwise Jaccard
    /// similarity ≥ `min_jaccard`. The original sibling tables are removed from the schema.
    ///
    /// JSON: `{ "images": { "front": {"url":"a.jpg","w":100}, "back": {"url":"b.jpg","w":80} } }`
    /// → `images(j2s_parent_id, key_id TEXT, url TEXT, w INTEGER)`
    /// → rows: `("front","a.jpg",100)`, `("back","b.jpg",80)`.
    KeyedPivot(SiblingSchema),

    /// Multi-group sibling collapse: siblings have two distinct key shapes (numeric + text).
    ///
    /// Trigger: same conditions as `KeyedPivot` but the key suffixes are a mix of integers
    /// ("1","2") and slugs ("front","back"). Each shape group produces its own synthetic pivot
    /// table; the parent itself becomes a pure routing table (no rows emitted in Pass 2).
    ///
    /// JSON: `{ "media": { "1": {"url":"a"}, "2": {"url":"b"}, "front": {"url":"c"} } }`
    /// → `media_num(parent_fk, key_id INTEGER, url TEXT)` — rows: `(1,"a")`, `(2,"b")`
    /// → `media_txt(parent_fk, key_id TEXT, url TEXT)` — row: `("front","c")`.
    MultiKeyedPivot(Vec<SiblingGroup>),

    /// Root table split: bi-modal key frequency → main table + `{name}_wide` EAV companion.
    ///
    /// Trigger: root table with many keys exhibiting a clear stable/rare frequency split.
    /// - Keys with frequency ≥ `stable_threshold` → columns on the main table.
    /// - Keys with `rare_threshold` ≤ freq < `stable_threshold` → EAV rows in `{name}_wide`.
    /// - Keys with frequency < `rare_threshold` → dropped entirely.
    ///
    /// JSON (over 1000 rows; "name" always present, "desc" in 60%, "extra" in 0.5%):
    /// → `products(j2s_id UUID, name TEXT, desc TEXT)` — stable + medium keys
    /// → `products_wide(j2s_anchor UUID, key TEXT, value TEXT)` — medium-freq keys as EAV
    /// (key "extra" silently dropped — below `rare_threshold`).
    AutoSplit {
        stable_threshold: f64,
        rare_threshold: f64,
        /// Pre-computed set of medium-frequency keys written to the _wide table in Pass 2.
        medium_keys: std::collections::HashSet<String>,
        /// `PostgreSQL` name of the companion wide table, e.g. "`products_wide`".
        wide_table_name: String,
    },

    /// Key appears in fewer than `rare_threshold` of rows — excluded from all schemas and data.
    ///
    /// Applied during `finalize()` before column building. No table or column is produced.
    /// The dropped key is recorded in the anomaly report so the user knows data was omitted.
    Ignore,

    /// Normalize dynamic keys: each JSON key in the object becomes a row.
    ///
    /// Applied manually via the IHM (not auto-inferred). The key itself is stored as a typed
    /// ID column; the remaining fields become regular columns (union across all keys).
    ///
    /// JSON: `{ "images": { "12584": {"url":"a.jpg","w":100}, "99001": {"url":"b.jpg","w":80} } }`
    /// → `images(j2s_parent_id, image_id TEXT, url TEXT, w INTEGER)`
    /// → rows: `("12584","a.jpg",100)`, `("99001","b.jpg",80)`.
    NormalizeDynamicKeys {
        /// Name of the column that will hold the original JSON key (e.g. `"image_id"`).
        id_column: String,
    },

    /// Flatten nested object: inlines the child's scalar fields as columns in the parent table.
    ///
    /// Applied manually via the IHM. The child table is removed from the schema.
    /// Each inlined column is prefixed with `prefix` (e.g. `"nutrients_"`).
    ///
    /// JSON: `{ "name": "X", "nutrients": { "calories": 250, "fat": 12 } }`
    /// → `product(name TEXT, nutrients_calories INTEGER, nutrients_fat INTEGER)`
    /// (child table `product_nutrients` removed).
    Flatten {
        /// Prefix prepended to inlined column names (e.g. `"nutrients_"`).
        prefix: String,
        /// Maximum nesting depth to flatten. Currently only depth = 1 is implemented.
        max_depth: u8,
    },

    /// Store the child object or array as a JSONB column directly on the parent table.
    ///
    /// Applied manually via the IHM. The child table is removed from the schema.
    /// One-to-one child → single JSONB object; one-to-many → JSONB array.
    ///
    /// JSON: `{ "name": "X", "tags": ["a", "b", "c"] }`
    /// → `product(name TEXT, tags JSONB)` — value stored as `'["a","b","c"]'`
    /// (child table `product_tags` removed).
    JsonbFlatten,
}

/// User-facing override for a table's materialization strategy.
///
/// Applied by `apply_user_overrides` between Pass 1 and Pass 2.
/// Only three choices are exposed to the user in the IHM — the full inferred strategy set
/// is not directly editable.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UserOverride {
    /// Force EAV pivot layout — equivalent to [`InferredStrategy::Pivot`].
    Pivot,
    /// Force JSONB storage — equivalent to [`InferredStrategy::Jsonb`].
    Jsonb,
    /// Remove the table from the schema and skip it in Pass 2.
    ///
    /// `#[serde(alias = "Ignore")]` provides backward-compat for snapshots saved before this
    /// variant was renamed.
    #[serde(alias = "Ignore")]
    Skip,
}

/// A column in a finalized table schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    /// Sanitized `PostgreSQL` column name (<= 63 chars)
    pub name: String,
    /// Original JSON field name
    pub original_name: String,
    pub pg_type: PgType,
    pub not_null: bool,
    /// True for `j2s_id`, j2s_{parent}_id, `j2s_order` — these are never in the JSON
    pub is_generated: bool,
    /// True only for the FK column pointing to the parent table (j2s_{parent}_id).
    /// Allows reliable identification independent of the column name.
    #[serde(default)]
    pub is_parent_fk: bool,
}

impl ColumnSchema {
    /// Create a generated j2s column.
    #[must_use]
    pub fn generated(name: &str, pg_type: PgType) -> Self {
        Self {
            name: name.to_string(),
            original_name: name.to_string(),
            pg_type,
            not_null: true,
            is_generated: true,
            is_parent_fk: false,
        }
    }

    /// Create the FK column pointing to the parent table.
    /// Column name: `j2s_{parent_name}_id`, truncated so the total is ≤ 63 chars.
    #[must_use]
    pub fn parent_fk(parent_name: &str) -> Self {
        // NamingRegistry guarantees table names ≤ PG_TABLE_MAX_IDENT (53), so
        // "j2s_" (4) + name (≤53) + "_id" (3) = ≤60 — always within PG's 63-byte limit.
        debug_assert!(
            parent_name.len() <= PG_TABLE_MAX_IDENT,
            "parent_name '{}' is {} chars — NamingRegistry should have truncated it to ≤{}",
            parent_name, parent_name.len(), PG_TABLE_MAX_IDENT
        );
        let col_name = format!("j2s_{parent_name}_id");
        Self {
            name: col_name.clone(),
            original_name: col_name,
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: true,
        }
    }
}

/// Describes the kind of child relationship.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChildKind {
    /// Nested JSON object → one-to-one or one-to-many relationship
    Object,
    /// Array of objects → one-to-many
    ObjectArray,
    /// Array of scalars → junction table with (j2s_{parent}_id, value, `j2s_order`)
    ScalarArray,
}

/// A fully resolved table schema ready for DDL generation and data loading.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    /// `PostgreSQL` table name (sanitized, <= 63 chars)
    pub name: String,
    /// Original path segments, e.g. `["users", "orders", "items"]`
    pub path: Vec<String>,
    /// Columns in declaration order (generated j2s_ columns come first)
    pub columns: Vec<ColumnSchema>,
    /// Name of the parent table (None for root table)
    pub parent_table: Option<String>,
    /// How this table relates to its parent
    pub child_kind: Option<ChildKind>,
    /// Depth in the hierarchy (root = 0)
    pub depth: usize,
    /// How wide-table keys are materialized — set by `finalize()`, optionally mutated by `apply_user_overrides`.
    #[serde(alias = "wide_strategy")]
    pub inferred_strategy: InferredStrategy,
    /// Maps prefixed column name → source JSON field for columns inlined via Flatten strategy.
    /// e.g. `nutrients_calories` → `nutrients` means: look up `obj["nutrients"]["calories"]`.
    /// Empty for tables that have no flattened children.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub flatten_sources: HashMap<String, String>,
    /// Cascaded routing: maps a JSON sub-key found inside a sibling pivot row to the child
    /// table that should receive that sub-object. Populated by `finalize_cascading` for two cases:
    ///   1. Co-sibling children merged into a synthetic pivot T (`child_routes["k"]` = T.name)
    ///   2. Independent children re-parented from an absorbed sibling to this pivot table
    ///      Empty for all non-cascaded tables (routing falls back to `path_map` in Pass 2).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub child_routes: HashMap<String, String>,
}

impl TableSchema {
    /// Create a new table schema with default (`Columns`) strategy and no columns.
    #[must_use]
    pub fn new(name: String, path: Vec<String>, depth: usize) -> Self {
        Self {
            name,
            path,
            columns: Vec::new(),
            parent_table: None,
            child_kind: None,
            depth,
            inferred_strategy: InferredStrategy::default(),
            flatten_sources: HashMap::new(),
            child_routes: HashMap::new(),
        }
    }

    #[must_use]
    pub const fn is_root(&self) -> bool {
        self.parent_table.is_none()
    }

    /// Returns true if this is a junction table for a scalar array child.
    ///
    /// Junction tables have the schema `(j2s_parent_id, value <type>, j2s_order INT)` — there
    /// are no user-facing data columns. They arise from JSON like `{ "tags": ["a", "b", "c"] }`.
    #[must_use]
    pub const fn is_junction(&self) -> bool {
        matches!(self.child_kind, Some(ChildKind::ScalarArray))
    }

    /// Returns true if Pass 2 should emit a `j2s_order` column for positional ordering.
    ///
    /// True for `ObjectArray` and `ScalarArray` children — any child that comes from a JSON array
    /// where the position of each element is semantically meaningful.
    #[must_use]
    pub const fn has_order_column(&self) -> bool {
        matches!(
            self.child_kind,
            Some(ChildKind::ObjectArray | ChildKind::ScalarArray)
        )
    }

    /// Return only user-facing data columns, excluding all generated `j2s_*` columns
    /// (`j2s_id`, `j2s_{parent}_id`, `j2s_order`). Used for Jaccard scoring and DDL output.
    pub fn data_columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.columns.iter().filter(|c| !c.is_generated)
    }

    /// Return all column names in order (for COPY FROM STDIN header).
    #[allow(dead_code)]
    #[must_use]
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Find a column by its original JSON field name.
    #[must_use]
    pub fn find_by_original(&self, original: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.original_name == original)
    }
}

impl InferredStrategy {
    /// Returns true if this strategy changes the default column-per-key layout.
    #[allow(dead_code)]
    #[must_use]
    pub const fn is_wide(&self) -> bool {
        !matches!(self, Self::Columns)
    }

    /// Returns the names of child tables explicitly absorbed by this strategy.
    ///
    /// Only `MultiKeyedPivot` carries an explicit absorbed-names list (one per `SiblingGroup`).
    /// For `KeyedPivot`, the absorbed sibling tables are tracked separately in the schema list
    /// (each sibling receives `InferredStrategy::Ignore` during finalization) — returns empty here.
    #[must_use]
    pub fn absorbed_names(&self) -> Vec<&str> {
        match self {
            Self::MultiKeyedPivot(groups) => groups
                .iter()
                .flat_map(|g| g.absorbed_names.iter().map(std::string::String::as_str))
                .collect(),
            _ => vec![],
        }
    }

    /// Returns true if child tables should be excluded from the schema because their
    /// data is absorbed into this table's wide column (Pivot / Jsonb / etc.).
    /// `AutoSplit` does NOT absorb children — they remain as separate tables.
    /// `NormalizeDynamicKeys` and Flatten absorb their child tables.
    #[must_use]
    pub const fn absorbs_children(&self) -> bool {
        matches!(
            self,
            Self::Pivot
                | Self::Jsonb
                | Self::StructuredPivot(_)
                | Self::KeyedPivot(_)
                | Self::NormalizeDynamicKeys { .. }
                | Self::Flatten { .. }
                | Self::JsonbFlatten
        )
        // MultiKeyedPivot: absorption handled via SiblingGroup.absorbed_names,
        // not through this flag — the parent itself absorbs nothing directly.
    }
}

impl std::fmt::Display for KeyShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric => write!(f, "NUMERIC"),
            Self::IsoLang => write!(f, "ISO_LANG"),
            Self::Slug    => write!(f, "SLUG"),
            Self::Mixed   => write!(f, "MIXED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::naming::PG_TABLE_MAX_IDENT;

    fn make_schema(name: &str) -> TableSchema {
        TableSchema::new(name.to_string(), vec![name.to_string()], 0)
    }

    #[test]
    fn child_routes_empty_by_default() {
        let s = make_schema("root");
        assert!(s.child_routes.is_empty());
    }

    #[test]
    fn child_routes_round_trip_json() {
        let mut s = make_schema("root");
        s.child_routes.insert("k1".to_string(), "root_pivot_k1".to_string());
        s.child_routes.insert("k2".to_string(), "root_pivot_k2".to_string());

        let json = serde_json::to_string(&s).unwrap();
        let back: TableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back.child_routes.get("k1").map(|s| s.as_str()), Some("root_pivot_k1"));
        assert_eq!(back.child_routes.get("k2").map(|s| s.as_str()), Some("root_pivot_k2"));
    }

    #[test]
    fn child_routes_skipped_when_empty_in_json() {
        let s = make_schema("root");
        let json = serde_json::to_string(&s).unwrap();
        // Empty map must not appear in the serialised form (skip_serializing_if)
        assert!(!json.contains("child_routes"));
    }

    #[test]
    fn parent_fk_short_name() {
        let col = ColumnSchema::parent_fk("users");
        assert_eq!(col.name, "j2s_users_id");
        assert!(col.is_parent_fk);
        assert!(col.is_generated);
    }

    #[test]
    fn parent_fk_max_budget_name_fits_pg_limit() {
        // A parent name at the PG_TABLE_MAX_IDENT budget (53 chars) must produce
        // a column name ≤ 63 chars: "j2s_" (4) + 53 + "_id" (3) = 60 ≤ 63.
        let parent_name = "a".repeat(PG_TABLE_MAX_IDENT);
        let col = ColumnSchema::parent_fk(&parent_name);
        assert_eq!(col.name.len(), 4 + PG_TABLE_MAX_IDENT + 3);
        assert!(col.name.len() <= 63);
    }

    #[test]
    fn child_routes_absent_in_old_json_deserialises_as_empty() {
        // Simulate a schema snapshot that predates the child_routes field.
        let json = r#"{"name":"root","path":["root"],"columns":[],"depth":0,"wide_strategy":"Columns","flatten_sources":{}}"#;
        let s: TableSchema = serde_json::from_str(json).unwrap();
        assert!(s.child_routes.is_empty());
    }

    #[test]
    fn user_override_skip_deserializes_from_ignore() {
        let v: UserOverride = serde_json::from_str(r#""Ignore""#).unwrap();
        assert_eq!(v, UserOverride::Skip);
    }

    #[test]
    fn user_override_round_trip() {
        for v in [UserOverride::Pivot, UserOverride::Jsonb, UserOverride::Skip] {
            let json = serde_json::to_string(&v).unwrap();
            let back: UserOverride = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v);
        }
    }

    #[test]
    fn table_schema_inferred_strategy_old_json_alias() {
        // Old snapshots serialise the field as "wide_strategy" — must still load.
        let json = r#"{"name":"root","path":["root"],"columns":[],"depth":0,"wide_strategy":"Columns","flatten_sources":{}}"#;
        let s: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(s.inferred_strategy, InferredStrategy::Columns);
    }
}
