//! Per-column type inference: accumulates observed JSON types across all rows and
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
//! resolves them to a single `PostgreSQL` type.
//!
//! [`TypeTracker`] counts occurrences of each [`InferredType`] variant as the JSON is
//! streamed. At finalization, `to_pg_type()` applies a widening hierarchy
//! (Integer → `BigInt` → Float → Text) to pick the narrowest `PostgreSQL` type compatible
//! with every observed value, except rare strings (below `RARE_STRING_RATE` of the non-null
//! values), which Pass 2 reports as anomalies. The resolved type is stored as [`PgType`] in a
//! `ColumnSchema`.
//!
//! Fonctions :
//! - enum `InferredType` — tous les types inférables depuis une valeur JSON (indexable, `repr(usize)`).
//! - enum `PgType` — type `PostgreSQL` résolu pour une colonne.
//! - fn `PgType::as_sql` — représentation SQL du type (ex: `VARCHAR(60)`, `INTEGER[]`).
//! - struct `TypeTracker` — suit tous les types et métadonnées observés pour un champ JSON.
//! - const `RARE_STRING_RATE` — part de chaînes sous laquelle elles ne décident pas du type (0,1 %).
//! - fn `TypeTracker::new` — crée un tracker vide pour un seuil TEXT/VARCHAR donné.
//! - fn `TypeTracker::observe` — enregistre une valeur JSON (hot path, appelé par ligne/champ).
//! - fn `TypeTracker::merge` — fusionne les observations d'un autre tracker (Pass 1 parallèle).
//! - fn `TypeTracker::dominant_type` — type non-null le plus fréquent.
//! - fn `TypeTracker::anomaly_rate` — fraction de valeurs différant du type dominant.
//! - fn `TypeTracker::is_not_null` — vrai si aucune valeur nulle observée.
//! - fn `TypeTracker::string_pg_type` — résout `VarChar`/`Text` en tenant compte de la longueur
//!   des représentations numériques si le champ est mixte string+nombre.
//! - fn `TypeTracker::has_frequent_strings` — vrai si les chaînes JSON (dates comprises) atteignent
//!   `RARE_STRING_RATE` des valeurs non nulles (sinon elles ne décident pas du type), sauf si ce ne sont
//!   que des UUID, ou que des dates et timestamps, sans nombre ni booléen (ils gardent leur type).
//! - fn `TypeTracker::to_pg_type` — résout le type PG final (dispatch + élargissement).
//! - fn `TypeTracker::is_object_field` — vrai si le champ ne contient que des objets.
//! - fn `TypeTracker::is_array_field` — vrai si le champ ne contient que des tableaux.
//! - fn `TypeTracker::has_anomalies` — vrai si plusieurs types distincts ont été observés.
//! - fn `TypeTracker::iter_types` — itère les (type, compte) non-nuls observés.
//! - fn `TypeTracker::active_type_count` — nombre de types non-Null distincts observés.
//! - fn `widen_pg_types` — type le plus large entre deux `PgType` (Text > `DoublePrecision` > `BigInt`).
//! - fn `infer_number_type` — classifie un nombre JSON en `InferredType`.
//! - fn `infer_string_type` — classifie une chaîne JSON en `InferredType` ; la Pass 2 s'en sert aussi
//!   pour vérifier les valeurs des colonnes UUID, DATE et TIMESTAMP.
//! - fn `is_uuid` — détecteur de format UUID (longueur puis motif).
//! - fn `is_timestamp` — timestamp complet : date, heure, puis secondes, fraction et fuseau facultatifs.
//! - fn `is_zone` — fin d'un timestamp : rien, `Z` ou un décalage `±HH`, `±HHMM`, `±HH:MM`.
//! - fn `at_most` — vrai si deux octets sont des chiffres ASCII dont la valeur ne dépasse pas un maximum.
//! - fn `is_date_bytes` — date `AAAA-MM-JJ` sur les 10 premiers octets, qui existe dans le calendrier.
//! - fn `two_digits` — valeur de deux chiffres ASCII.
//! - fn `is_digit` — vrai si l'octet est un chiffre ASCII.

use serde_json::Value;

/// All types that can be inferred from a JSON value.
/// `repr(usize)` + `Copy` allow use as a direct array index — no heap allocation needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum InferredType {
    Null = 0,
    Boolean = 1,
    Integer = 2, // fits in i32
    BigInt = 3,  // requires i64
    Float = 4,   // f64
    Uuid = 5,
    Date = 6,
    Timestamp = 7,
    Varchar = 8, // string, max_len <= text_threshold
    Text = 9,    // string, max_len > text_threshold
    /// Nested object → becomes a child table (not stored as a column)
    Object = 10,
    /// Array → becomes a child table (of objects) or a PG array column (of scalars)
    Array = 11,
}

impl InferredType {
    /// Total number of variants — size of the counts array.
    pub const COUNT: usize = 12;

    /// All variants in index order, for iteration.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Null,
        Self::Boolean,
        Self::Integer,
        Self::BigInt,
        Self::Float,
        Self::Uuid,
        Self::Date,
        Self::Timestamp,
        Self::Varchar,
        Self::Text,
        Self::Object,
        Self::Array,
    ];
}

// COUNT must equal ALL.len(), and each variant must sit at its own discriminant index.
// Adding a variant without updating ALL (or reordering) is a compile-time error here.
const _: () = {
    assert!(InferredType::ALL.len() == InferredType::COUNT);
    let mut i = 0;
    while i < InferredType::ALL.len() {
        assert!(InferredType::ALL[i] as usize == i);
        i += 1;
    }
};

/// The resolved `PostgreSQL` type for a column.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PgType {
    Integer,
    BigInt,
    DoublePrecision,
    Boolean,
    Uuid,
    Date,
    Timestamp,
    VarChar(u32), // computed as ceil(max_len * 1.2), min 1
    Text,
    /// `PostgreSQL` array of a scalar type, e.g. TEXT[], INTEGER[]
    Array(Box<Self>),
    /// `PostgreSQL` JSONB — used by `InferredStrategy::Jsonb` tables
    Jsonb,
}

impl PgType {
    #[must_use]
    pub fn as_sql(&self) -> String {
        match self {
            Self::Integer => "INTEGER".to_string(),
            Self::BigInt => "BIGINT".to_string(),
            Self::DoublePrecision => "DOUBLE PRECISION".to_string(),
            Self::Boolean => "BOOLEAN".to_string(),
            Self::Uuid => "UUID".to_string(),
            Self::Date => "DATE".to_string(),
            Self::Timestamp => "TIMESTAMP".to_string(),
            Self::VarChar(n) => format!("VARCHAR({n})"),
            Self::Text => "TEXT".to_string(),
            Self::Array(elem) => format!("{}[]", elem.as_sql()),
            Self::Jsonb => "JSONB".to_string(),
        }
    }
}

/// Share of non-null values below which Varchar/Text observations are ignored by `to_pg_type`.
const RARE_STRING_RATE: f64 = 0.001;

/// Tracks all observed types and metadata for a single JSON field (future column).
///
/// `type_counts` is a fixed-size array indexed by `InferredType as usize`, replacing the
/// former `IndexMap<InferredType, u64>`. This eliminates heap allocation entirely from the
/// observation hot path — `observe()` is called billions of times on large files.
///
/// Null observations are tracked separately via `null_count` and never appear in `type_counts`.
#[derive(Debug, Clone)]
pub struct TypeTracker {
    pub total_count: u64,
    pub null_count: u64,
    /// Histogram of non-null types observed, indexed by `InferredType as usize`.
    pub type_counts: [u64; InferredType::COUNT],
    /// Maximum string length observed (for VARCHAR sizing)
    pub max_len: u32,
    /// The threshold above which we use TEXT instead of VARCHAR
    pub text_threshold: u32,
}

impl TypeTracker {
    #[must_use]
    pub const fn new(text_threshold: u32) -> Self {
        Self {
            total_count: 0,
            null_count: 0,
            type_counts: [0; InferredType::COUNT],
            max_len: 0,
            text_threshold,
        }
    }

    /// Observe a single JSON value for this field.
    #[inline]
    pub fn observe(&mut self, value: &Value) {
        self.total_count += 1;
        match value {
            Value::Null => {
                self.null_count += 1;
            }
            Value::Bool(_) => {
                self.type_counts[InferredType::Boolean as usize] += 1;
            }
            Value::Number(n) => {
                self.type_counts[infer_number_type(n) as usize] += 1;
            }
            Value::String(s) => {
                let t = infer_string_type(s);
                let len = s.len() as u32;
                if len > self.max_len {
                    self.max_len = len;
                }
                self.type_counts[t as usize] += 1;
            }
            Value::Object(_) => {
                self.type_counts[InferredType::Object as usize] += 1;
            }
            Value::Array(_) => {
                self.type_counts[InferredType::Array as usize] += 1;
            }
        }
    }

    /// Merge observations from `other` into `self`.
    /// Used to combine per-worker `TypeTrackers` after parallel Pass 1.
    pub fn merge(&mut self, other: &Self) {
        self.total_count += other.total_count;
        self.null_count += other.null_count;
        for i in 0..InferredType::COUNT {
            self.type_counts[i] += other.type_counts[i];
        }
        if other.max_len > self.max_len {
            self.max_len = other.max_len;
        }
    }

    /// The dominant (most frequent) non-null type.
    #[allow(dead_code)]
    #[must_use]
    pub fn dominant_type(&self) -> InferredType {
        InferredType::ALL
            .iter()
            .skip(1) // skip Null — tracked via null_count, never in type_counts
            .max_by_key(|&&t| self.type_counts[t as usize])
            .copied()
            .filter(|&t| self.type_counts[t as usize] > 0)
            .unwrap_or(InferredType::Null)
    }

    /// Fraction of rows where type differs from the dominant type (anomaly rate).
    #[allow(dead_code)]
    #[must_use]
    pub fn anomaly_rate(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }
        let dominant_count = InferredType::ALL
            .iter()
            .skip(1)
            .map(|&t| self.type_counts[t as usize])
            .max()
            .unwrap_or(0);
        let anomalous = self.total_count - self.null_count - dominant_count;
        anomalous as f64 / self.total_count as f64
    }

    #[must_use]
    pub const fn is_not_null(&self) -> bool {
        self.null_count == 0
    }

    /// Resolve to the final `PostgreSQL` type.
    /// Merging rules: the "widest" type wins.
    #[must_use]
    fn string_pg_type(&self, has_float: bool, has_bigint: bool, has_int: bool) -> PgType {
        // max_len tracks observed string lengths only. For mixed string+numeric fields,
        // pass2 also formats numbers as strings — use conservative upper bounds so VarChar
        // is always wide enough without false positives in the anomaly detector.
        let num_repr_len = if has_float {
            25u32
        } else if has_bigint {
            20u32
        } else if has_int {
            11u32
        } else {
            0u32
        };
        let effective_max = self.max_len.max(num_repr_len);
        if effective_max > self.text_threshold {
            PgType::Text
        } else {
            PgType::VarChar(((f64::from(effective_max) * 1.2).ceil() as u32).max(1))
        }
    }

    /// True if JSON strings (dates, timestamps and UUIDs included) make up at least `RARE_STRING_RATE`
    /// of the non-null values. Rarer strings don't decide the type: in a numeric or boolean column,
    /// Pass 2 turns them into NULL + anomaly. UUIDs alone, or dates and timestamps alone, with no
    /// number or boolean, keep their own type; mixed with anything else they have no common type
    /// (#116).
    fn has_frequent_strings(&self) -> bool {
        let count = |t: InferredType| self.type_counts[t as usize];
        let plain = count(InferredType::Varchar) + count(InferredType::Text);
        let uuids = count(InferredType::Uuid);
        let dates = count(InferredType::Date) + count(InferredType::Timestamp);
        let scalars: u64 = [
            InferredType::Boolean,
            InferredType::Integer,
            InferredType::BigInt,
            InferredType::Float,
        ]
        .into_iter()
        .map(count)
        .sum();
        if plain == 0 && scalars == 0 && (uuids == 0 || dates == 0) {
            return false;
        }
        let strings = plain + uuids + dates;
        let non_null = self.total_count - self.null_count;
        strings > 0 && strings as f64 >= RARE_STRING_RATE * non_null as f64
    }

    pub fn to_pg_type(&self) -> PgType {
        let has = |t: InferredType| self.type_counts[t as usize] > 0;

        if self.has_frequent_strings() {
            return self.string_pg_type(
                has(InferredType::Float),
                has(InferredType::BigInt),
                has(InferredType::Integer),
            );
        }

        // Numeric widening: float > bigint > int
        if has(InferredType::Float) {
            return PgType::DoublePrecision;
        }
        if has(InferredType::BigInt) {
            return PgType::BigInt;
        }
        if has(InferredType::Integer) {
            return PgType::Integer;
        }

        if has(InferredType::Boolean) {
            return PgType::Boolean;
        }
        if has(InferredType::Timestamp) {
            return PgType::Timestamp;
        }
        if has(InferredType::Date) {
            return PgType::Date;
        }
        if has(InferredType::Uuid) {
            return PgType::Uuid;
        }

        // Fallback
        PgType::Text
    }

    /// True if this field contains only objects (→ child table, not a column).
    #[must_use]
    pub fn is_object_field(&self) -> bool {
        self.type_counts[InferredType::Object as usize] > 0 && self.active_type_count() == 1
    }

    /// True if this field contains only arrays (→ child table, not a column).
    #[must_use]
    pub fn is_array_field(&self) -> bool {
        self.type_counts[InferredType::Array as usize] > 0 && self.active_type_count() == 1
    }

    /// True if this field has mixed types that constitute an anomaly.
    #[must_use]
    pub fn has_anomalies(&self) -> bool {
        self.active_type_count() > 1
    }

    /// Iterate over (`InferredType`, count) pairs for non-zero, non-Null types.
    pub fn iter_types(&self) -> impl Iterator<Item = (InferredType, u64)> + '_ {
        InferredType::ALL
            .iter()
            .skip(1) // skip Null slot — tracked via null_count
            .filter_map(|&t| {
                let c = self.type_counts[t as usize];
                if c > 0 {
                    Some((t, c))
                } else {
                    None
                }
            })
    }

    /// Number of distinct non-Null types observed.
    fn active_type_count(&self) -> usize {
        self.type_counts[1..].iter().filter(|&&c| c > 0).count()
    }
}

// ---------------------------------------------------------------------------
// Public utilities
// ---------------------------------------------------------------------------

/// Return the "wider" of two `PgTypes` — the one that can represent all values of both.
#[must_use]
pub fn widen_pg_types(a: PgType, b: &PgType) -> PgType {
    if a == *b {
        return a;
    }
    match (&a, b) {
        (PgType::Text | PgType::VarChar(_) | PgType::Jsonb, _)
        | (_, PgType::Text | PgType::VarChar(_) | PgType::Jsonb) => PgType::Text,
        (PgType::DoublePrecision, _) | (_, PgType::DoublePrecision) => PgType::DoublePrecision,
        (PgType::BigInt, _) | (_, PgType::BigInt) => PgType::BigInt,
        _ => PgType::Text,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn infer_number_type(n: &serde_json::Number) -> InferredType {
    if n.is_f64() {
        // Check if it's actually a whole number stored as float
        if let Some(f) = n.as_f64() {
            if f.fract() == 0.0 && f >= f64::from(i32::MIN) && f <= f64::from(i32::MAX) {
                return InferredType::Integer;
            }
            if f.fract() == 0.0 {
                return InferredType::BigInt;
            }
        }
        return InferredType::Float;
    }
    if let Some(i) = n.as_i64() {
        if i32::try_from(i).is_ok() {
            return InferredType::Integer;
        }
        return InferredType::BigInt;
    }
    InferredType::Float
}

/// Regex-free heuristic type detection for strings.
/// Dispatches on length first to eliminate impossible candidates in O(1).
/// Pass 2 checks UUID, DATE and TIMESTAMP values with it: what it types so, `PostgreSQL` accepts.
#[inline]
#[must_use]
pub fn infer_string_type(s: &str) -> InferredType {
    let b = s.as_bytes();
    match b.len() {
        // UUID is exactly 36 bytes: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        36 => {
            if is_uuid(s) {
                InferredType::Uuid
            } else {
                InferredType::Varchar
            }
        }
        // Date is exactly 10 bytes: YYYY-MM-DD
        10 => {
            if is_date_bytes(b) {
                InferredType::Date
            } else {
                InferredType::Varchar
            }
        }
        // Timestamp needs at least 16 bytes: YYYY-MM-DDTHH:MM
        // (36 bytes is only checked as a UUID: a timestamp that long stays text)
        n if n >= 16 => {
            if is_timestamp(s) {
                InferredType::Timestamp
            } else {
                InferredType::Varchar
            }
        }
        _ => InferredType::Varchar,
    }
}

fn is_uuid(s: &str) -> bool {
    // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    for (i, &c) in b.iter().enumerate() {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            if c != b'-' {
                return false;
            }
        } else if !c.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

/// The whole string is checked: a date, `T` or a space, `HH:MM`, optional `:SS` and `.fraction`, then
/// optional `Z` or offset `±HH`, `±HHMM`, `±HH:MM`. Only forms `PostgreSQL` reads as TIMESTAMP (#116):
/// no named zone, no `24:00`, no second 60, although it takes them.
fn is_timestamp(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 16
        || !is_date_bytes(b)
        || !matches!(b[10], b'T' | b' ')
        || b[13] != b':'
        || !at_most(b[11], b[12], 23)
        || !at_most(b[14], b[15], 59)
    {
        return false;
    }
    let mut rest = &b[16..];
    if let [b':', s1, s2, tail @ ..] = rest {
        if !at_most(*s1, *s2, 59) {
            return false;
        }
        rest = tail;
        if let [b'.', fraction @ ..] = rest {
            let digits = fraction.iter().take_while(|c| c.is_ascii_digit()).count();
            if digits == 0 {
                return false;
            }
            rest = &fraction[digits..];
        }
    }
    is_zone(rest)
}

/// End of a timestamp: nothing, `Z`, or an offset `±HH`, `±HHMM`, `±HH:MM` up to ±15:59.
const fn is_zone(b: &[u8]) -> bool {
    match b {
        [] | [b'Z'] => true,
        [b'+' | b'-', h1, h2] => at_most(*h1, *h2, 15),
        [b'+' | b'-', h1, h2, m1, m2] | [b'+' | b'-', h1, h2, b':', m1, m2] => {
            at_most(*h1, *h2, 15) && at_most(*m1, *m2, 59)
        }
        _ => false,
    }
}

/// True if the two bytes are ASCII digits whose value is at most `max`.
const fn at_most(hi: u8, lo: u8, max: u16) -> bool {
    matches!(two_digits(hi, lo), Some(v) if v <= max)
}

/// `YYYY-MM-DD` on the first 10 bytes, and a day of the calendar: `PostgreSQL` rejects month 13,
/// April 31, February 29 of a common year and year 0 (#116).
const fn is_date_bytes(b: &[u8]) -> bool {
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    let (Some(century), Some(yy), Some(month), Some(day)) = (
        two_digits(b[0], b[1]),
        two_digits(b[2], b[3]),
        two_digits(b[5], b[6]),
        two_digits(b[8], b[9]),
    ) else {
        return false;
    };
    let year = century * 100 + yy;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        1..=12 => 31,
        _ => 0,
    };
    year > 0 && day >= 1 && day <= days_in_month
}

/// Value of two ASCII digits, or `None` if one is not a digit.
const fn two_digits(hi: u8, lo: u8) -> Option<u16> {
    if is_digit(hi) && is_digit(lo) {
        Some((hi - b'0') as u16 * 10 + (lo - b'0') as u16)
    } else {
        None
    }
}

#[inline]
const fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_integer_inference() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!(42));
        t.observe(&json!(100));
        assert_eq!(t.to_pg_type(), PgType::Integer);
    }

    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is an arbitrary test float, not pi
    fn test_float_widens_integer() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!(42));
        t.observe(&json!(3.14));
        assert_eq!(t.to_pg_type(), PgType::DoublePrecision);
    }

    #[test]
    fn test_varchar_sizing() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("hello")); // len 5
                                    // max_len=5, sized = ceil(5 * 1.2) = 6
        assert_eq!(t.to_pg_type(), PgType::VarChar(6));
    }

    #[test]
    fn test_text_threshold() {
        let mut t = TypeTracker::new(10);
        t.observe(&json!("this is a longer string")); // len 23 > 10
        assert_eq!(t.to_pg_type(), PgType::Text);
    }

    #[test]
    fn test_uuid_detection() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(t.to_pg_type(), PgType::Uuid);
    }

    #[test]
    fn test_date_detection() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("2024-03-15"));
        assert_eq!(t.to_pg_type(), PgType::Date);
    }

    #[test]
    fn test_timestamp_detection() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("2024-03-15T10:30:00Z"));
        assert_eq!(t.to_pg_type(), PgType::Timestamp);
    }

    #[test]
    fn test_anomaly_rate() {
        let mut t = TypeTracker::new(256);
        for _ in 0..1000 {
            t.observe(&json!(42));
        }
        for _ in 0..3 {
            t.observe(&json!("N/A"));
        }
        assert!(t.anomaly_rate() > 0.0);
        assert!(t.anomaly_rate() < 0.01);
    }

    #[test]
    fn test_not_null() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!(1));
        t.observe(&json!(2));
        assert!(t.is_not_null());
        t.observe(&Value::Null);
        assert!(!t.is_not_null());
    }

    #[test]
    fn test_dominant_type_not_null() {
        let mut t = TypeTracker::new(256);
        for _ in 0..100 {
            t.observe(&Value::Null);
        }
        t.observe(&json!(42));
        // Integer should dominate over Null (Null is never in type_counts)
        assert_eq!(t.dominant_type(), InferredType::Integer);
    }

    #[test]
    fn test_iter_types_skips_null() {
        let mut t = TypeTracker::new(256);
        t.observe(&Value::Null);
        t.observe(&json!(1));
        let types: Vec<_> = t.iter_types().collect();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].0, InferredType::Integer);
        assert_eq!(types[0].1, 1);
    }

    #[test]
    fn test_is_object_field() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!({"a": 1}));
        assert!(t.is_object_field());
        t.observe(&json!(42));
        assert!(!t.is_object_field());
    }

    #[test]
    fn test_has_anomalies() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!(1));
        assert!(!t.has_anomalies());
        t.observe(&json!("str"));
        assert!(t.has_anomalies());
    }

    // --- Mixed string+numeric VarChar sizing ---

    /// Champ mixte string+float : `max_len` string est petit (3) mais les floats
    /// formattés peuvent atteindre 25 chars → `VarChar` doit valoir ceil(25*1.2)=30.
    #[test]
    fn test_mixed_string_float_varchar_sized_for_float() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("N/A")); // 3 bytes
        t.observe(&json!(1234.567));
        // effective_max = max(3, 25) = 25 → ceil(25 * 1.2) = 30
        assert_eq!(t.to_pg_type(), PgType::VarChar(30));
    }

    /// Champ mixte string+bigint : `VarChar` doit valoir ceil(20*1.2)=24.
    #[test]
    fn test_mixed_string_bigint_varchar_sized_for_bigint() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("N/A")); // 3 bytes
        t.observe(&json!(9_999_999_999_i64)); // bigint
                                              // effective_max = max(3, 20) = 20 → ceil(20 * 1.2) = 24
        assert_eq!(t.to_pg_type(), PgType::VarChar(24));
    }

    /// Champ mixte string+integer : `VarChar` doit valoir ceil(11*1.2)=14.
    #[test]
    fn test_mixed_string_integer_varchar_sized_for_integer() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("N/A")); // 3 bytes
        t.observe(&json!(42));
        // effective_max = max(3, 11) = 11 → ceil(11 * 1.2) = 14
        assert_eq!(t.to_pg_type(), PgType::VarChar(14));
    }

    /// Quand la string est plus longue que la repr numérique, c'est `max_len` string qui prime.
    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is an arbitrary test float, not pi
    fn test_mixed_string_longer_than_num_repr_uses_string_max() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("a very long product name here!")); // 30 bytes
        t.observe(&json!(3.14));
        // effective_max = max(30, 25) = 30 → ceil(30 * 1.2) = 36
        assert_eq!(t.to_pg_type(), PgType::VarChar(36));
    }

    // --- Chaînes rares : ignorées pour le typage sous RARE_STRING_RATE ---

    /// Cas `OpenFoodFacts` : une chaîne parmi des milliers de flottants ne passe plus la colonne en texte.
    #[test]
    fn test_rare_string_keeps_float() {
        let mut t = TypeTracker::new(256);
        for i in 0..10_000 {
            t.observe(&json!(f64::from(i) + 0.5));
        }
        t.observe(&json!("10000000000000000000"));
        assert_eq!(t.to_pg_type(), PgType::DoublePrecision);
    }

    /// L'élargissement numérique reste appliqué au reste : int + bigint + chaîne rare → BIGINT.
    #[test]
    fn test_rare_string_keeps_numeric_widening() {
        let mut t = TypeTracker::new(256);
        for i in 0..5_000 {
            t.observe(&json!(i));
            t.observe(&json!(9_999_999_999_i64 + i64::from(i)));
        }
        t.observe(&json!("N/A"));
        assert_eq!(t.to_pg_type(), PgType::BigInt);
    }

    /// Une chaîne parmi 100 entiers (1 %) n'est pas rare : la colonne reste texte.
    #[test]
    fn test_frequent_string_still_varchar() {
        let mut t = TypeTracker::new(256);
        for i in 0..100 {
            t.observe(&json!(i));
        }
        t.observe(&json!("N/A"));
        assert!(matches!(t.to_pg_type(), PgType::VarChar(_)));
    }

    /// La part se calcule sur les valeurs non nulles : des nulls ne rendent pas une chaîne rare.
    #[test]
    fn test_rare_string_rate_ignores_nulls() {
        let mut t = TypeTracker::new(256);
        for i in 0..999 {
            t.observe(&json!(i));
        }
        t.observe(&json!("N/A")); // 1/1000 = exactement le seuil → pas rare
        for _ in 0..1_000_000 {
            t.observe(&Value::Null);
        }
        assert!(matches!(t.to_pg_type(), PgType::VarChar(_)));
    }

    /// Dates + une chaîne rare : la colonne reste texte et garde la chaîne. En `DATE`, elle
    /// deviendrait NULL + anomalie.
    #[test]
    fn test_rare_string_among_dates_still_varchar() {
        let mut t = TypeTracker::new(256);
        for _ in 0..10_000 {
            t.observe(&json!("2024-01-15"));
        }
        t.observe(&json!("unknown"));
        assert!(matches!(t.to_pg_type(), PgType::VarChar(_)));
    }

    // --- #116 : formats sans type commun, dates qui n'existent pas ---

    /// UUID + dates (ou timestamps) n'ont pas de type `PostgreSQL` commun : la colonne passe en texte.
    /// Avant : DATE (ou TIMESTAMP), et le COPY s'arrêtait sur le premier UUID.
    #[test]
    fn test_uuid_with_dates_is_varchar() {
        for other in ["2024-01-15", "2024-01-15T10:30:00Z"] {
            let mut t = TypeTracker::new(256);
            t.observe(&json!("550e8400-e29b-41d4-a716-446655440000"));
            t.observe(&json!(other));
            assert_eq!(t.to_pg_type(), PgType::VarChar(44), "{other}"); // ceil(36 * 1.2)
        }
    }

    /// Dates + timestamps restent compatibles : TIMESTAMP (`PostgreSQL` lit une date comme minuit).
    #[test]
    fn test_dates_with_timestamps_stay_timestamp() {
        let mut t = TypeTracker::new(256);
        t.observe(&json!("2024-01-15"));
        t.observe(&json!("2024-01-15T10:30:00Z"));
        assert_eq!(t.to_pg_type(), PgType::Timestamp);
    }

    /// Des dates fréquentes avec un nombre ou un booléen : texte. Avant : INTEGER ou BOOLEAN, et
    /// chaque date devenait NULL + anomalie.
    #[test]
    fn test_frequent_dates_with_a_number_or_bool_are_varchar() {
        for other in [json!(42), json!(true)] {
            let mut t = TypeTracker::new(256);
            for _ in 0..10_000 {
                t.observe(&json!("2024-01-15"));
            }
            t.observe(&other);
            assert!(matches!(t.to_pg_type(), PgType::VarChar(_)), "{other}");
        }
    }

    /// Une date rare parmi des entiers ne décide pas du type, comme toute chaîne rare.
    #[test]
    fn test_rare_date_among_integers_keeps_integer() {
        let mut t = TypeTracker::new(256);
        for i in 0..10_000 {
            t.observe(&json!(i));
        }
        t.observe(&json!("2024-01-15"));
        assert_eq!(t.to_pg_type(), PgType::Integer);
    }

    /// Une date est un jour du calendrier : `PostgreSQL` refuse le mois 13, le 31 avril, le 29 février
    /// d'une année non bissextile et l'année 0.
    #[test]
    fn test_date_must_exist_in_the_calendar() {
        for ok in ["2024-02-29", "2000-02-29", "0001-01-01", "9999-12-31"] {
            assert_eq!(infer_string_type(ok), InferredType::Date, "{ok}");
        }
        for bad in [
            "2024-13-45",
            "2024-00-10",
            "2024-01-00",
            "2024-04-31",
            "2023-02-29",
            "1900-02-29",
            "0000-01-01",
        ] {
            assert_eq!(infer_string_type(bad), InferredType::Varchar, "{bad}");
        }
    }

    /// Un timestamp est vérifié jusqu'au bout. Accepté : `T` ou espace, secondes et fraction
    /// facultatives, `Z` ou décalage `±HH`, `±HHMM`, `±HH:MM`. Refusé : ce que `PostgreSQL` refuse,
    /// et les fuseaux nommés (`UTC`), `24:00` et la seconde 60, qu'il accepte.
    #[test]
    fn test_timestamp_is_checked_to_the_end() {
        for ok in [
            "2024-01-15T10:30",
            "2024-01-15 10:30",
            "2024-01-15T10:30:00",
            "2024-01-15T10:30:00Z",
            "2024-01-15T10:30:00.123456789Z",
            "2024-01-15T10:30:00+02",
            "2024-01-15T10:30:00+0200",
            "2024-01-15T10:30:00-05:30",
            "2024-01-15T10:30+15:59",
        ] {
            assert_eq!(infer_string_type(ok), InferredType::Timestamp, "{ok}");
        }
        for bad in [
            "2024-01-15T10:30 junk",
            "2024-13-15T10:30",
            "2024-01-15T25:00",
            "2024-01-15T24:00",
            "2024-01-15T10:61",
            "2024-01-15T10:30:60",
            "2024-01-15T10:30:00.",
            "2024-01-15T10:30:00+16:00",
            "2024-01-15T10:30:00+02:60",
            "2024-01-15T10:30:00 UTC",
            "2024-01-15T10:30:00Z ",
        ] {
            assert_eq!(infer_string_type(bad), InferredType::Varchar, "{bad}");
        }
    }

    /// Parity test: observing on two separate trackers then merging must equal
    /// observing all values on a single tracker.
    #[test]
    #[allow(clippy::approx_constant)] // 3.14 is an arbitrary test float, not pi
    fn test_merge_parity() {
        let values = vec![
            json!(1),
            json!(2),
            json!(null),
            json!("hello"),
            json!(true),
            json!(3.14),
            json!(null),
            json!("world"),
        ];

        // Single tracker
        let mut single = TypeTracker::new(256);
        for v in &values {
            single.observe(v);
        }

        // Two separate trackers, merged
        let mut a = TypeTracker::new(256);
        let mut b = TypeTracker::new(256);
        for v in &values[..4] {
            a.observe(v);
        }
        for v in &values[4..] {
            b.observe(v);
        }
        a.merge(&b);

        assert_eq!(a.total_count, single.total_count);
        assert_eq!(a.null_count, single.null_count);
        assert_eq!(a.type_counts, single.type_counts);
        assert_eq!(a.max_len, single.max_len);
        assert_eq!(a.to_pg_type(), single.to_pg_type());
    }
}
