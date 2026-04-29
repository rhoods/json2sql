use std::collections::HashMap;

use super::naming::PG_TABLE_MAX_IDENT;
use super::type_tracker::PgType;

/// One suffix column in a StructuredPivot table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SuffixColumn {
    /// The raw suffix string as it appears in JSON keys, e.g. "_100g"
    pub suffix: String,
    /// The PostgreSQL column name derived from the suffix, e.g. "c_100g"
    pub col_name: String,
    /// The inferred PostgreSQL type for this suffix column
    pub pg_type: PgType,
}

/// Describes the suffix decomposition detected for a StructuredPivot table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SuffixSchema {
    /// Detected suffix columns, sorted by suffix string
    pub suffix_cols: Vec<SuffixColumn>,
    /// PostgreSQL type for the "base value" column (key with no suffix)
    pub value_type: PgType,
}

/// Shape of the sibling keys — used to name the key column semantically.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum KeyShape {
    /// All keys are pure integers: "1", "2", "42"
    Numeric,
    /// All keys are 2-3 char alpha codes (ISO language/country): "fr", "en", "deu"
    IsoLang,
    /// Keys are slugs or compound strings: "en_glass", "palm_oil"
    Slug,
    /// Mix of numeric and ISO codes
    Mixed,
}

fn default_data_col_name() -> String {
    "j2s_data".to_string()
}

/// Metadata for a KeyedPivot table (sibling tables collapsed into one).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SiblingSchema {
    /// Name of the column that holds the original sibling key (e.g. "key_id", "lang_code", "key")
    pub key_col_name: String,
    /// Detected shape of the sibling keys
    pub key_shape: KeyShape,
    /// True when the collapsed siblings were ObjectArray children (each key maps to an array of
    /// objects). Pass 2 iterates the array and emits one row per element with j2s_order.
    #[serde(default)]
    pub array_children: bool,
    /// Name of the JSONB column that stores the raw child object/array.
    /// Defaults to "data"; falls back to "j2s_data" if "data" collides with a union column.
    #[serde(default = "default_data_col_name")]
    pub data_col_name: String,
}

/// One key-shape subgroup within a MultiKeyedPivot parent.
/// Each group produces its own synthetic pivot table in the schema.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SiblingGroup {
    /// Name of the synthetic pivot table (e.g. "products_images_num").
    pub pivot_table: String,
    /// If true, this group handles numeric-keyed children; false = non-numeric.
    pub key_is_numeric: bool,
    /// Pivot column metadata (key column name, key shape, etc.).
    pub sibling_schema: SiblingSchema,
    /// Names of the original child tables absorbed into this group.
    /// Used by `exclude_absorbed_children` to remove them from the schema.
    pub absorbed_names: Vec<String>,
}

/// Strategy for handling "wide" tables — tables with many dynamic keys.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub enum WideStrategy {
    /// Default: one SQL column per JSON key.
    #[default]
    Columns,
    /// EAV pivot: one row per key-value pair — columns: (key TEXT, value <type>).
    /// Best when keys are dynamic but values share a compatible type (e.g. nutrients → all FLOAT).
    Pivot,
    /// Store the entire object as a single JSONB column.
    /// Best when values are heterogeneous or structure is arbitrary.
    Jsonb,
    /// Structured pivot: group keys by common prefix, suffixes become typed columns.
    /// e.g. calcium/calcium_100g/calcium_unit → one row per nutrient with per_100g, unit columns.
    StructuredPivot(SuffixSchema),
    /// Sibling collapse: N child tables with the same schema are merged into 1 table.
    /// The child key becomes a column; each child object's fields become columns (union).
    /// e.g. products_images_1, products_images_2 → products_images with key_id + union cols.
    KeyedPivot(SiblingSchema),
    /// Multi-group sibling collapse: children have two distinct key shapes (e.g. numeric "1","2"
    /// and text "front_en","rev_fr"). Each shape group produces its own synthetic pivot table.
    /// The parent itself stores no rows — it is a pure routing table in Pass 2.
    MultiKeyedPivot(Vec<SiblingGroup>),
    /// Root table split: stable keys (freq >= stable_threshold) stay as columns in the main
    /// table; medium keys (rare_threshold <= freq < stable_threshold) go to a companion
    /// `{name}_wide` Pivot table linked by the same anchor UUID.
    /// Keys below rare_threshold are dropped entirely (see WideStrategy::Ignore).
    AutoSplit {
        stable_threshold: f64,
        rare_threshold: f64,
        /// Pre-computed set of medium-frequency keys written to the _wide table in Pass 2.
        medium_keys: std::collections::HashSet<String>,
        /// PostgreSQL name of the companion wide table, e.g. "products_wide".
        wide_table_name: String,
    },
    /// Key is present in < rare_threshold of rows — excluded from all schemas and data.
    /// Applied during finalize() before column building.
    Ignore,
    /// Normalize dynamic keys: each key in the object becomes a row, the key itself becomes
    /// a typed ID column. Similar to KeyedPivot but applied manually via the IHM.
    /// e.g. images.12584 → { image_id: "12584", url: ..., width: ... }
    NormalizeDynamicKeys {
        /// Name of the column that will hold the original JSON key (e.g. "image_id").
        id_column: String,
    },
    /// Flatten nested object: inlines the child object's scalar fields as columns in the
    /// parent table. The child table is removed from the schema.
    /// e.g. nutrients.calories → parent.nutrients_calories
    /// Set temporarily during apply_flatten(); removed from schema by the end of that function.
    Flatten {
        /// Prefix prepended to inlined column names (e.g. "nutrients_").
        prefix: String,
        /// Maximum nesting depth to flatten. Currently only depth = 1 is implemented.
        max_depth: u8,
    },
    /// Inline the child table's raw JSON into a JSONB column on the parent table.
    /// The child table is removed from the schema; the parent gains a `{child_name} JSONB` column.
    /// One-to-one child → single JSONB object; one-to-many → JSONB array.
    JsonbFlatten,
}

/// A column in a finalized table schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    /// Sanitized PostgreSQL column name (<= 63 chars)
    pub name: String,
    /// Original JSON field name
    pub original_name: String,
    pub pg_type: PgType,
    pub not_null: bool,
    /// True for j2s_id, j2s_{parent}_id, j2s_order — these are never in the JSON
    pub is_generated: bool,
    /// True only for the FK column pointing to the parent table (j2s_{parent}_id).
    /// Allows reliable identification independent of the column name.
    #[serde(default)]
    pub is_parent_fk: bool,
}

impl ColumnSchema {
    /// Create a generated j2s column.
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
    pub fn parent_fk(parent_name: &str) -> Self {
        // NamingRegistry guarantees table names ≤ PG_TABLE_MAX_IDENT (53), so
        // "j2s_" (4) + name (≤53) + "_id" (3) = ≤60 — always within PG's 63-byte limit.
        debug_assert!(
            parent_name.len() <= PG_TABLE_MAX_IDENT,
            "parent_name '{}' is {} chars — NamingRegistry should have truncated it to ≤{}",
            parent_name, parent_name.len(), PG_TABLE_MAX_IDENT
        );
        let col_name = format!("j2s_{}_id", parent_name);
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
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ChildKind {
    /// Nested JSON object → one-to-one or one-to-many relationship
    Object,
    /// Array of objects → one-to-many
    ObjectArray,
    /// Array of scalars → junction table with (j2s_{parent}_id, value, j2s_order)
    ScalarArray,
}

/// A fully resolved table schema ready for DDL generation and data loading.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    /// PostgreSQL table name (sanitized, <= 63 chars)
    pub name: String,
    /// Original path segments, e.g. ["users", "orders", "items"]
    pub path: Vec<String>,
    /// Columns in declaration order (generated j2s_ columns come first)
    pub columns: Vec<ColumnSchema>,
    /// Name of the parent table (None for root table)
    pub parent_table: Option<String>,
    /// How this table relates to its parent
    pub child_kind: Option<ChildKind>,
    /// Depth in the hierarchy (root = 0)
    pub depth: usize,
    /// How wide-table keys are stored (auto-detected or user-overridden).
    pub wide_strategy: WideStrategy,
    /// Maps prefixed column name → source JSON field for columns inlined via Flatten strategy.
    /// e.g. "nutrients_calories" → "nutrients" means: look up obj["nutrients"]["calories"].
    /// Empty for tables that have no flattened children.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub flatten_sources: HashMap<String, String>,
    /// Cascaded routing: maps a JSON sub-key found inside a sibling pivot row to the child
    /// table that should receive that sub-object. Populated by `finalize_cascading` for two cases:
    ///   1. Co-sibling children merged into a synthetic pivot T (child_routes["k"] = T.name)
    ///   2. Independent children re-parented from an absorbed sibling to this pivot table
    /// Empty for all non-cascaded tables (routing falls back to path_map in Pass 2).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub child_routes: HashMap<String, String>,
}

impl TableSchema {
    pub fn new(name: String, path: Vec<String>, depth: usize) -> Self {
        Self {
            name,
            path,
            columns: Vec::new(),
            parent_table: None,
            child_kind: None,
            depth,
            wide_strategy: WideStrategy::default(),
            flatten_sources: HashMap::new(),
            child_routes: HashMap::new(),
        }
    }

    pub fn is_root(&self) -> bool {
        self.parent_table.is_none()
    }

    pub fn is_junction(&self) -> bool {
        matches!(self.child_kind, Some(ChildKind::ScalarArray))
    }

    pub fn has_order_column(&self) -> bool {
        matches!(
            self.child_kind,
            Some(ChildKind::ObjectArray) | Some(ChildKind::ScalarArray)
        )
    }

    /// Return only data columns (excludes generated j2s_ columns).
    pub fn data_columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.columns.iter().filter(|c| !c.is_generated)
    }

    /// Return all column names in order (for COPY FROM STDIN header).
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Find a column by its original JSON field name.
    pub fn find_by_original(&self, original: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.original_name == original)
    }
}

impl WideStrategy {
    /// Returns true if this strategy changes the default column-per-key layout.
    pub fn is_wide(&self) -> bool {
        !matches!(self, WideStrategy::Columns)
    }

    /// Returns the names of all child tables directly absorbed by this strategy.
    /// For MultiKeyedPivot, this is the union of all groups' absorbed_names.
    pub fn absorbed_names(&self) -> Vec<&str> {
        match self {
            WideStrategy::MultiKeyedPivot(groups) => groups
                .iter()
                .flat_map(|g| g.absorbed_names.iter().map(|s| s.as_str()))
                .collect(),
            _ => vec![],
        }
    }

    /// Returns true if child tables should be excluded from the schema because their
    /// data is absorbed into this table's wide column (Pivot / Jsonb / etc.).
    /// AutoSplit does NOT absorb children — they remain as separate tables.
    /// NormalizeDynamicKeys and Flatten absorb their child tables.
    pub fn absorbs_children(&self) -> bool {
        matches!(
            self,
            WideStrategy::Pivot
                | WideStrategy::Jsonb
                | WideStrategy::StructuredPivot(_)
                | WideStrategy::KeyedPivot(_)
                | WideStrategy::NormalizeDynamicKeys { .. }
                | WideStrategy::Flatten { .. }
                | WideStrategy::JsonbFlatten
        )
        // MultiKeyedPivot: absorption handled via SiblingGroup.absorbed_names,
        // not through this flag — the parent itself absorbs nothing directly.
    }
}

impl std::fmt::Display for KeyShape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyShape::Numeric => write!(f, "NUMERIC"),
            KeyShape::IsoLang => write!(f, "ISO_LANG"),
            KeyShape::Slug    => write!(f, "SLUG"),
            KeyShape::Mixed   => write!(f, "MIXED"),
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
}
