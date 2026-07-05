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
//! | `SiblingCollapse` | inferred | N sibling tables, same schema | merged table with key column |
//! | `SiblingCollapseMulti` | inferred | siblings with mixed key shapes | two synthetic pivot tables |
//! | `AutoSplit` | inferred | root table with bi-modal key frequency | main table + `_wide` companion |
//! | `Ignore` | inferred | key appears in < `rare_threshold` of rows | excluded from schema |
//! | `Skip` | user | user wants table removed | table excluded from DDL and Pass 2 |
//! | `NormalizeDynamicKeys` | TOML-only | arbitrary JSON keys → row IDs | key becomes a column |
//! | `Flatten` | TOML-only | inline child fields into parent | child table removed |
//! | `JsonbFlatten` | TOML-only | store child as JSONB on parent | child table removed |
//!
//! See each variant's doc comment for a concrete JSON → SQL example.

use std::collections::HashMap;
use std::sync::Arc;

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

/// Metadata for a `SiblingCollapse` table (sibling tables collapsed into one).
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
}

/// One key-shape subgroup within a `SiblingCollapseMulti` parent.
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
    /// JSON input:
    /// `[{"name":"Alice","age":30}, {"name":"Bob","age":25}]`
    ///
    /// DDL:
    /// ```text
    /// CREATE TABLE users (
    ///     j2s_id  BIGINT PRIMARY KEY,
    ///     name    TEXT,
    ///     age     INTEGER
    /// );
    /// -- 2 rows: ("Alice",30), ("Bob",25)
    /// ```
    #[default]
    Columns,

    /// EAV pivot: rotates dynamic keys into rows — `(key TEXT, value <type>)` instead of one column per key.
    ///
    /// Trigger: more than `wide_column_threshold` distinct keys (default **100**, CLI flag
    /// `--wide-column-threshold`), all values share a compatible PostgreSQL type (detected by
    /// `suggest_wide_strategy`: if values mix TEXT + FLOAT + BOOL → `Jsonb` instead).
    ///
    /// JSON input (3 products, each with the same nutrient keys):
    /// `[{"name":"Nutella","nutrients":{"energy":250,"fat":12,"salt":0.3}}, ...]`
    ///
    /// Without pivot, Pass 1 would produce one column per nutrient key (hundreds for real datasets).
    /// With Pivot, the columns are rotated into rows:
    ///
    /// ```text
    /// CREATE TABLE products_nutrients (
    ///     j2s_parent_id  BIGINT,          -- FK to products.j2s_id
    ///     key            TEXT NOT NULL,   -- the nutrient key ("energy", "fat", …)
    ///     value          DOUBLE PRECISION -- the nutrient value
    /// );
    /// -- 3 rows per product (one per nutrient key):
    /// -- ("energy",250), ("fat",12), ("salt",0.3)
    /// ```
    Pivot,

    /// Store the entire object as a single `JSONB` column.
    ///
    /// Trigger (two independent paths):
    /// - Wide table (> `wide_column_threshold`, default **100**) with heterogeneous value types
    ///   (e.g. INTEGER + TEXT + BOOL in the same object) → `Pivot` is not possible.
    /// - Any table exceeding the PostgreSQL hard limit of **1600** total columns after `finalize()`
    ///   → forced by `apply_column_limit_guard` regardless of the wide threshold.
    ///
    /// JSON input:
    /// `[{"name":"Nutella","meta":{"v":2,"tags":["a","b"],"active":true}}, ...]`
    ///
    /// DDL:
    /// ```text
    /// CREATE TABLE products_meta (
    ///     j2s_parent_id  BIGINT,  -- FK to products.j2s_id
    ///     data           JSONB    -- the whole object stored as-is
    /// );
    /// -- 1 row per product: ('{"v":2,"tags":["a","b"],"active":true}')
    /// ```
    Jsonb,

    /// Structured pivot: keys share a `{base}_{suffix}` pattern; suffixes become typed columns.
    ///
    /// Trigger: more than `wide_column_threshold` distinct keys (default **100**) AND at least
    /// `SUFFIX_MIN_COVERAGE` (30%) of those keys follow a `{base}` / `{base}_{suffix}` pattern
    /// (e.g. `energy`, `energy_100g`, `energy_unit`). One row per base group.
    ///
    /// JSON input (flat object with prefixed keys):
    /// ```text
    /// { "energy":250, "energy_100g":1046, "energy_unit":"kcal",
    ///   "fat":12,     "fat_100g":50,      "fat_unit":"g" }
    /// ```
    ///
    /// DDL — one column per detected suffix, one row per base:
    /// ```text
    /// CREATE TABLE products_nutrients (
    ///     j2s_parent_id  BIGINT,
    ///     j2s_key        TEXT NOT NULL,    -- the base name ("energy", "fat")
    ///     value          INTEGER,          -- bare key value
    ///     per_100g       DOUBLE PRECISION, -- "_100g" suffix
    ///     unit           TEXT              -- "_unit" suffix
    /// );
    /// -- 2 rows: ("energy",250,1046,"kcal"), ("fat",12,50,"g")
    /// ```
    StructuredPivot(SuffixSchema),

    /// Sibling collapse: N child tables with the same schema merged into one, with a key column
    /// identifying the origin. The original data columns are **preserved** (not rotated into EAV).
    ///
    /// Trigger: ≥ `sibling_threshold` child tables under the same parent whose column sets have
    /// pairwise Jaccard similarity ≥ `min_jaccard`. The sibling tables disappear from the schema.
    ///
    /// JSON input — N objects under the same parent key, same fields:
    /// ```text
    /// { "images": {
    ///     "front": {"url":"a.jpg","w":100,"h":50},
    ///     "back":  {"url":"b.jpg","w":80, "h":40},
    ///     "top":   {"url":"c.jpg","w":90        }
    /// } }
    /// ```
    /// Pass 1 creates 3 sibling tables: `images_front`, `images_back`, `images_top`.
    /// SiblingCollapse merges them — columns are the **union** of all siblings, missing fields → NULL:
    ///
    /// ```text
    /// CREATE TABLE images (
    ///     j2s_parent_id  BIGINT,
    ///     key_id         TEXT NOT NULL, -- "front" | "back" | "top"
    ///     url            TEXT,
    ///     w              INTEGER,
    ///     h              INTEGER        -- NULL for "top" (field absent in that sibling)
    /// );
    /// -- 3 rows: ("front","a.jpg",100,50), ("back","b.jpg",80,40), ("top","c.jpg",90,NULL)
    /// ```
    SiblingCollapse(SiblingSchema),

    /// Multi-group sibling collapse: like `SiblingCollapse` but the sibling keys mix integers and slugs,
    /// so two synthetic tables are produced — one per key shape. The parent becomes a routing table
    /// (no rows, only `j2s_id`).
    ///
    /// Trigger: same Jaccard conditions as `SiblingCollapse`, but `classify_key_shape` detects both
    /// numeric keys ("1","2") and slug keys ("front","back") in the same sibling group.
    ///
    /// JSON input — same parent key, mixed numeric + text child keys:
    /// ```text
    /// { "media": {
    ///     "1":     {"url":"a.jpg","size":1200},
    ///     "2":     {"url":"b.jpg","size":800 },
    ///     "front": {"url":"c.jpg","size":950 }
    /// } }
    /// ```
    /// Pass 1 creates 3 siblings. SiblingCollapseMulti splits them into 2 synthetic tables:
    ///
    /// ```text
    /// -- Parent: routing only, no data rows
    /// CREATE TABLE media (j2s_id BIGINT PRIMARY KEY);
    ///
    /// -- Numeric group
    /// CREATE TABLE media_key_num (
    ///     j2s_parent_id  BIGINT,
    ///     key_id         INTEGER NOT NULL, -- 1, 2
    ///     url            TEXT,
    ///     size           INTEGER
    /// );
    /// -- 2 rows: (1,"a.jpg",1200), (2,"b.jpg",800)
    ///
    /// -- Text group
    /// CREATE TABLE media_key_txt (
    ///     j2s_parent_id  BIGINT,
    ///     key_id         TEXT NOT NULL,   -- "front"
    ///     url            TEXT,
    ///     size           INTEGER
    /// );
    /// -- 1 row: ("front","c.jpg",950)
    /// ```
    SiblingCollapseMulti(Vec<SiblingGroup>),

    /// Root table split: bi-modal key frequency → main table (stable columns) + `{name}_wide`
    /// EAV companion (medium-frequency keys). Rare keys are dropped entirely.
    ///
    /// Trigger: root table (no parent) with > `wide_column_threshold` distinct keys (default
    /// **100**) AND object children, where key frequencies form two clusters: many always-present
    /// keys + many rarely-present keys.
    /// - freq ≥ `stable_threshold`                        → regular column on main table
    /// - `rare_threshold` ≤ freq < `stable_threshold`    → EAV row in `{name}_wide`
    /// - freq < `rare_threshold`                          → dropped (anomaly report)
    ///
    /// JSON input (1000 products; "name" in 100% rows, "desc" in 60%, "promo_code" in 0.3%):
    /// ```text
    /// {"name":"Nutella","desc":"Chocolate spread","promo_code":"SAVE10", ...}
    /// {"name":"Oreo",   "desc":"Cookies",                               ...}
    /// ...
    /// ```
    ///
    /// DDL — two tables produced:
    /// ```text
    /// -- Main table: stable keys become regular columns
    /// CREATE TABLE products (
    ///     j2s_id  UUID PRIMARY KEY,
    ///     name    TEXT,   -- 100% frequency → stable column
    ///     desc    TEXT    -- 60% frequency  → stable column (above stable_threshold)
    /// );
    ///
    /// -- Companion: medium-frequency keys stored as EAV rows
    /// CREATE TABLE products_wide (
    ///     j2s_anchor  UUID,        -- FK to products.j2s_id
    ///     key         TEXT NOT NULL,
    ///     value       TEXT
    /// );
    /// -- "promo_code" dropped: 0.3% < rare_threshold → anomaly report only
    /// ```
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
    /// Also used internally to mark sibling tables absorbed by `SiblingCollapse`/`SiblingCollapseMulti`.
    ///
    /// No DDL produced — the table or key is silently omitted from the schema.
    Ignore,

    /// Normalize dynamic keys: each JSON key in the object becomes a row, with the key stored
    /// as a typed ID column. Child object fields become regular columns (union across all keys).
    ///
    /// Applied manually via the IHM (not auto-inferred). Similar to `SiblingCollapse` but triggered
    /// by the user, not by sibling detection.
    ///
    /// JSON input — arbitrary numeric IDs as keys, each with the same nested fields:
    /// ```text
    /// { "images": {
    ///     "12584": {"url":"a.jpg","w":100},
    ///     "99001": {"url":"b.jpg","w":80 }
    /// } }
    /// ```
    ///
    /// DDL — child tables collapsed into the parent, ID becomes a column:
    /// ```text
    /// CREATE TABLE images (
    ///     j2s_parent_id  BIGINT,
    ///     image_id       TEXT NOT NULL, -- the original JSON key ("12584", "99001")
    ///     url            TEXT,
    ///     w              INTEGER
    /// );
    /// -- 2 rows: ("12584","a.jpg",100), ("99001","b.jpg",80)
    /// ```
    NormalizeDynamicKeys {
        /// Name of the column that will hold the original JSON key (e.g. `"image_id"`).
        ///
        /// `Arc<str>` for the same reason as `Flatten::prefix` — cheap to clone in the
        /// Pass 2 hot path.
        id_column: Arc<str>,
    },

    /// Flatten nested object: inlines the child's scalar fields as columns in the parent table.
    ///
    /// Applied manually via the IHM. The child table is removed from the schema.
    /// Each inlined column is prefixed with `prefix` to avoid name collisions.
    ///
    /// JSON input — nested object whose fields should land directly on the parent:
    /// `[{"name":"Nutella","nutrients":{"calories":250,"fat":12}}, ...]`
    ///
    /// DDL — child table `product_nutrients` is removed, its columns appear on `products`:
    /// ```text
    /// CREATE TABLE products (
    ///     j2s_id              BIGINT PRIMARY KEY,
    ///     name                TEXT,
    ///     nutrients_calories  INTEGER, -- prefix "nutrients_" + field "calories"
    ///     nutrients_fat       INTEGER  -- prefix "nutrients_" + field "fat"
    /// );
    /// -- 1 row: ("Nutella",250,12)
    /// ```
    Flatten {
        /// Prefix prepended to inlined column names (e.g. `"nutrients_"`).
        ///
        /// `Arc<str>` rather than `String`: `effective_strategy()` clones this value on every
        /// call in the Pass 2 hot path whenever an override is active. An `Arc` clone is a
        /// refcount bump; a `String` clone is a heap allocation per row.
        prefix: Arc<str>,
        /// Maximum nesting depth to flatten. Currently only depth = 1 is implemented.
        max_depth: u8,
    },

    /// Store the child object or array as a `JSONB` column directly on the parent table.
    ///
    /// Applied manually via the IHM. The child table is removed from the schema.
    /// One-to-one child → single JSONB object; one-to-many child → JSONB array.
    ///
    /// JSON input — child array that should stay inline rather than become a separate table:
    /// `[{"name":"Nutella","tags":["chocolate","spread","jar"]}, ...]`
    ///
    /// DDL — child table `products_tags` is removed, the array lands as a JSONB column:
    /// ```text
    /// CREATE TABLE products (
    ///     j2s_id  BIGINT PRIMARY KEY,
    ///     name    TEXT,
    ///     tags    JSONB   -- stored as '["chocolate","spread","jar"]'
    /// );
    /// -- 1 row: ("Nutella",'["chocolate","spread","jar"]')
    /// ```
    JsonbFlatten,
}

/// User-facing override for a table's materialization strategy.
///
/// Applied by `apply_user_overrides` between Pass 1 and Pass 2.
/// Converts to [`InferredStrategy`] via `From<&UserOverride>` for use in Pass 2.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UserOverride {
    /// Force standard column layout — equivalent to [`InferredStrategy::Columns`].
    Columns,
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
    /// Inline child as a JSONB column on the parent — IHM override only.
    JsonbFlatten,
    /// Inline child's scalar fields as prefixed columns on the parent.
    Flatten {
        prefix: Arc<str>,
        max_depth: u8,
    },
    /// Normalize dynamic keys: each JSON key becomes a row with the key as a typed ID column.
    NormalizeDynamicKeys {
        id_column: Arc<str>,
    },
}

impl From<&UserOverride> for InferredStrategy {
    fn from(ov: &UserOverride) -> Self {
        match ov {
            UserOverride::Columns                           => InferredStrategy::Columns,
            UserOverride::Pivot                             => InferredStrategy::Pivot,
            UserOverride::Jsonb                             => InferredStrategy::Jsonb,
            UserOverride::Skip                              => InferredStrategy::Ignore,
            UserOverride::JsonbFlatten                      => InferredStrategy::JsonbFlatten,
            UserOverride::Flatten { prefix, max_depth }     => InferredStrategy::Flatten {
                prefix: prefix.clone(),
                max_depth: *max_depth,
            },
            UserOverride::NormalizeDynamicKeys { id_column } => InferredStrategy::NormalizeDynamicKeys {
                id_column: id_column.clone(),
            },
        }
    }
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
    /// How wide-table keys are materialized — set by `finalize()`. Never mutated after that.
    #[serde(alias = "wide_strategy", default)]
    pub inferred_strategy: InferredStrategy,
    /// Strategy override from config.toml — takes precedence over `inferred_strategy`.
    /// Private: mutate only via `set_toml_override()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    toml_override: Option<UserOverride>,
    /// Strategy override from the IHM — takes precedence over `toml_override` and `inferred_strategy`.
    /// Private: mutate only via `set_ui_override()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ui_override: Option<UserOverride>,
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
    /// Number of rows observed for this table during Pass 1. Zero for tables built from
    /// snapshots that predate this field, or for tables created synthetically during finalization.
    #[serde(default)]
    pub row_count: u64,
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
            toml_override: None,
            ui_override: None,
            flatten_sources: HashMap::new(),
            child_routes: HashMap::new(),
            row_count: 0,
        }
    }

    /// Returns the effective strategy applying priority: ui_override > toml_override > inferred_strategy.
    ///
    /// `Flatten`/`NormalizeDynamicKeys` carry their `String` payload as `Arc<str>`, so the
    /// `Cow::Owned` clone below is a refcount bump, not a heap allocation — cheap enough to
    /// compute fresh on every call in the Pass 2 hot path, with no cache to keep in sync.
    #[must_use]
    pub fn effective_strategy(&self) -> std::borrow::Cow<InferredStrategy> {
        if let Some(ov) = &self.ui_override {
            return std::borrow::Cow::Owned(InferredStrategy::from(ov));
        }
        if let Some(ov) = &self.toml_override {
            return std::borrow::Cow::Owned(InferredStrategy::from(ov));
        }
        std::borrow::Cow::Borrowed(&self.inferred_strategy)
    }

    #[must_use]
    pub const fn is_root(&self) -> bool {
        self.parent_table.is_none()
    }

    /// Current IHM-level strategy override, if any.
    #[must_use]
    pub const fn ui_override(&self) -> Option<&UserOverride> {
        self.ui_override.as_ref()
    }

    /// Current config.toml-level strategy override, if any.
    /// Only read from tests today (production code checks `ui_override()` only) — kept `pub`
    /// for API symmetry with `set_toml_override()`/`ui_override()`. `main.rs` denies dead_code
    /// crate-wide, and the bin target doesn't see `#[cfg(test)]` call sites, hence the `allow`.
    #[must_use]
    #[allow(dead_code)]
    pub const fn toml_override(&self) -> Option<&UserOverride> {
        self.toml_override.as_ref()
    }

    /// Sets the IHM-level strategy override.
    pub fn set_ui_override(&mut self, ov: Option<UserOverride>) {
        self.ui_override = ov;
    }

    /// Sets the config.toml-level strategy override.
    pub fn set_toml_override(&mut self, ov: Option<UserOverride>) {
        self.toml_override = ov;
    }

    /// Returns true if this table absorbs its children, considering all override levels.
    #[must_use]
    pub fn absorbs_children(&self) -> bool {
        self.effective_strategy().absorbs_children()
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
    /// Only `SiblingCollapseMulti` carries an explicit absorbed-names list (one per `SiblingGroup`).
    /// For `SiblingCollapse`, absorbed siblings are removed from schemas via `exclude_absorbed_children`
    /// — returns empty here.
    #[must_use]
    pub fn absorbed_names(&self) -> Vec<&str> {
        match self {
            Self::SiblingCollapseMulti(groups) => groups
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
                | Self::SiblingCollapse(_)
                | Self::NormalizeDynamicKeys { .. }
                | Self::Flatten { .. }
                | Self::JsonbFlatten
        )
        // SiblingCollapseMulti: absorption handled via SiblingGroup.absorbed_names,
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
        let variants = [
            UserOverride::Pivot,
            UserOverride::Jsonb,
            UserOverride::Skip,
            UserOverride::JsonbFlatten,
            UserOverride::Flatten { prefix: "p_".into(), max_depth: 1 },
            UserOverride::NormalizeDynamicKeys { id_column: "key_id".into() },
        ];
        for v in variants {
            let json = serde_json::to_string(&v).unwrap();
            let back: UserOverride = serde_json::from_str(&json).unwrap();
            assert_eq!(back, v);
        }
    }

    #[test]
    fn from_user_override_pivot() {
        assert_eq!(InferredStrategy::from(&UserOverride::Pivot), InferredStrategy::Pivot);
    }

    #[test]
    fn from_user_override_jsonb() {
        assert_eq!(InferredStrategy::from(&UserOverride::Jsonb), InferredStrategy::Jsonb);
    }

    #[test]
    fn from_user_override_skip_becomes_ignore() {
        assert_eq!(InferredStrategy::from(&UserOverride::Skip), InferredStrategy::Ignore);
    }

    #[test]
    fn from_user_override_jsonb_flatten() {
        assert_eq!(InferredStrategy::from(&UserOverride::JsonbFlatten), InferredStrategy::JsonbFlatten);
    }

    #[test]
    fn from_user_override_flatten() {
        let ov = UserOverride::Flatten { prefix: "img_".into(), max_depth: 2 };
        assert_eq!(
            InferredStrategy::from(&ov),
            InferredStrategy::Flatten { prefix: "img_".into(), max_depth: 2 }
        );
    }

    #[test]
    fn from_user_override_normalize_dynamic_keys() {
        let ov = UserOverride::NormalizeDynamicKeys { id_column: "image_id".into() };
        assert_eq!(
            InferredStrategy::from(&ov),
            InferredStrategy::NormalizeDynamicKeys { id_column: "image_id".into() }
        );
    }

    #[test]
    fn effective_strategy_returns_inferred_when_no_override() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Pivot;
        assert_eq!(*s.effective_strategy(), InferredStrategy::Pivot);
    }

    #[test]
    fn effective_strategy_toml_overrides_inferred() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Columns;
        s.toml_override = Some(UserOverride::Jsonb);
        assert_eq!(*s.effective_strategy(), InferredStrategy::Jsonb);
    }

    #[test]
    fn effective_strategy_ui_overrides_toml_and_inferred() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Columns;
        s.toml_override = Some(UserOverride::Jsonb);
        s.ui_override = Some(UserOverride::Pivot);
        assert_eq!(*s.effective_strategy(), InferredStrategy::Pivot);
    }

    #[test]
    fn effective_strategy_ui_overrides_inferred_without_toml() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Columns;
        s.ui_override = Some(UserOverride::JsonbFlatten);
        assert_eq!(*s.effective_strategy(), InferredStrategy::JsonbFlatten);
    }

    #[test]
    fn table_schema_new_override_fields_absent_in_json() {
        let s = make_schema("t");
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("toml_override"), "toml_override should be absent when None");
        assert!(!json.contains("ui_override"), "ui_override should be absent when None");
    }

    #[test]
    fn table_schema_override_fields_roundtrip() {
        let mut s = make_schema("t");
        s.toml_override = Some(UserOverride::Jsonb);
        s.ui_override = Some(UserOverride::Pivot);
        let json = serde_json::to_string(&s).unwrap();
        let back: TableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back.toml_override, Some(UserOverride::Jsonb));
        assert_eq!(back.ui_override, Some(UserOverride::Pivot));
    }

    #[test]
    fn set_ui_override_none_falls_back_to_toml_override() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Columns;
        s.set_toml_override(Some(UserOverride::Jsonb));
        s.set_ui_override(Some(UserOverride::Pivot));
        assert_eq!(*s.effective_strategy(), InferredStrategy::Pivot);

        s.set_ui_override(None);

        assert_eq!(*s.effective_strategy(), InferredStrategy::Jsonb);
    }

    #[test]
    fn set_ui_override_still_takes_priority_over_toml_override() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Columns;
        s.set_toml_override(Some(UserOverride::Jsonb));
        s.set_ui_override(Some(UserOverride::Pivot));
        assert_eq!(*s.effective_strategy(), InferredStrategy::Pivot);
    }

    #[test]
    fn override_getters_return_current_value() {
        let mut s = make_schema("t");
        assert_eq!(s.ui_override(), None);
        assert_eq!(s.toml_override(), None);
        s.set_toml_override(Some(UserOverride::Jsonb));
        s.set_ui_override(Some(UserOverride::Pivot));
        assert_eq!(s.toml_override(), Some(&UserOverride::Jsonb));
        assert_eq!(s.ui_override(), Some(&UserOverride::Pivot));
    }

    /// Exhaustively walks every combination and every order of `set_ui_override()`/
    /// `set_toml_override()` mutations (including clearing back to `None`) and asserts
    /// `effective_strategy()` matches the documented priority after *every single* call.
    #[test]
    fn effective_strategy_never_diverges_across_setter_sequences() {
        let overrides = [None, Some(UserOverride::Pivot), Some(UserOverride::Jsonb), Some(UserOverride::Columns)];

        for ui in &overrides {
            for toml in &overrides {
                // Two independent schemas to cover both mutation orders (ui-then-toml and
                // toml-then-ui).
                let mut ui_then_toml = make_schema("t");
                ui_then_toml.inferred_strategy = InferredStrategy::NormalizeDynamicKeys { id_column: "k".into() };
                ui_then_toml.set_ui_override(ui.clone());
                assert_expected_effective_strategy(&ui_then_toml, ui, &None);
                ui_then_toml.set_toml_override(toml.clone());
                assert_expected_effective_strategy(&ui_then_toml, ui, toml);

                let mut toml_then_ui = make_schema("t");
                toml_then_ui.inferred_strategy = InferredStrategy::NormalizeDynamicKeys { id_column: "k".into() };
                toml_then_ui.set_toml_override(toml.clone());
                assert_expected_effective_strategy(&toml_then_ui, &None, toml);
                toml_then_ui.set_ui_override(ui.clone());
                assert_expected_effective_strategy(&toml_then_ui, ui, toml);

                // Clearing back to None must also recompute correctly, in both orders.
                toml_then_ui.set_ui_override(None);
                assert_expected_effective_strategy(&toml_then_ui, &None, toml);
                toml_then_ui.set_toml_override(None);
                assert_expected_effective_strategy(&toml_then_ui, &None, &None);
            }
        }
    }

    /// Helper for `effective_strategy_never_diverges_across_setter_sequences`: computes the
    /// expected strategy from the same ui_override > toml_override > inferred_strategy priority
    /// documented on `effective_strategy()`.
    fn assert_expected_effective_strategy(
        s: &TableSchema,
        ui: &Option<UserOverride>,
        toml: &Option<UserOverride>,
    ) {
        let expected = match (ui, toml) {
            (Some(ov), _) | (None, Some(ov)) => InferredStrategy::from(ov),
            (None, None) => s.inferred_strategy.clone(),
        };
        assert_eq!(*s.effective_strategy(), expected);
    }

    #[test]
    fn effective_strategy_borrows_when_no_override() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Pivot;
        assert!(matches!(s.effective_strategy(), std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn table_schema_old_json_without_overrides_deserialises_as_none() {
        let json = r#"{"name":"root","path":["root"],"columns":[],"depth":0,"inferred_strategy":"Columns","flatten_sources":{}}"#;
        let s: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(s.toml_override, None);
        assert_eq!(s.ui_override, None);
    }

    #[test]
    fn table_schema_absorbs_children_via_inferred_strategy() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Flatten { prefix: "p_".into(), max_depth: 1 };
        assert!(s.absorbs_children(), "Flatten (inferred) must absorb children");
    }

    #[test]
    fn table_schema_absorbs_children_via_ui_override_jsonb_flatten() {
        let mut s = make_schema("t");
        s.inferred_strategy = InferredStrategy::Columns;
        s.ui_override = Some(UserOverride::JsonbFlatten);
        assert!(s.absorbs_children(), "JsonbFlatten ui_override must absorb children");
    }

    #[test]
    fn table_schema_does_not_absorb_children_without_override() {
        let s = make_schema("t"); // Columns, no override
        assert!(!s.absorbs_children());
    }

    #[test]
    fn table_schema_inferred_strategy_old_json_alias() {
        // Old snapshots serialise the field as "wide_strategy" — must still load.
        let json = r#"{"name":"root","path":["root"],"columns":[],"depth":0,"wide_strategy":"Columns","flatten_sources":{}}"#;
        let s: TableSchema = serde_json::from_str(json).unwrap();
        assert_eq!(s.inferred_strategy, InferredStrategy::Columns);
    }
}
