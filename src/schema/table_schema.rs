use std::collections::HashMap;

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

/// Metadata for a KeyedPivot table (sibling tables collapsed into one).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SiblingSchema {
    /// Name of the column that holds the original sibling key (e.g. "key_id", "lang_code", "key")
    pub key_col_name: String,
    /// Detected shape of the sibling keys
    pub key_shape: KeyShape,
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
        // "j2s_" (4) + parent_name + "_id" (3) must fit in 63 chars → max 56 chars for parent_name
        let truncated = if parent_name.len() > 56 {
            &parent_name[..56]
        } else {
            parent_name
        };
        let col_name = format!("j2s_{}_id", truncated);
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
