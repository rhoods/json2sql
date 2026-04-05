use indexmap::IndexMap;
use serde_json::Value;

/// All types that can be inferred from a JSON value.
/// Ordering matters: used to determine the "widest" type when merging.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InferredType {
    Null,
    Boolean,
    Integer,   // fits in i32
    BigInt,    // requires i64
    Float,     // f64
    Uuid,
    Date,
    Timestamp,
    Varchar,   // string, max_len <= text_threshold
    Text,      // string, max_len > text_threshold
    /// Nested object → becomes a child table (not stored as a column)
    Object,
    /// Array → becomes a child table or junction table
    Array,
}

/// The resolved PostgreSQL type for a column.
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
    /// PostgreSQL array of a scalar type, e.g. TEXT[], INTEGER[]
    Array(Box<PgType>),
    /// PostgreSQL JSONB — used by WideStrategy::Jsonb tables
    Jsonb,
}

impl PgType {
    pub fn as_sql(&self) -> String {
        match self {
            PgType::Integer => "INTEGER".to_string(),
            PgType::BigInt => "BIGINT".to_string(),
            PgType::DoublePrecision => "DOUBLE PRECISION".to_string(),
            PgType::Boolean => "BOOLEAN".to_string(),
            PgType::Uuid => "UUID".to_string(),
            PgType::Date => "DATE".to_string(),
            PgType::Timestamp => "TIMESTAMP".to_string(),
            PgType::VarChar(n) => format!("VARCHAR({})", n),
            PgType::Text => "TEXT".to_string(),
            PgType::Array(elem) => format!("{}[]", elem.as_sql()),
            PgType::Jsonb => "JSONB".to_string(),
        }
    }
}

/// Tracks all observed types and metadata for a single JSON field (future column).
#[derive(Debug, Clone)]
pub struct TypeTracker {
    pub total_count: u64,
    pub null_count: u64,
    /// Histogram of non-null types observed
    pub type_counts: IndexMap<InferredType, u64>,
    /// Maximum string length observed (for VARCHAR sizing)
    pub max_len: u32,
    /// The threshold above which we use TEXT instead of VARCHAR
    pub text_threshold: u32,
}

impl TypeTracker {
    pub fn new(text_threshold: u32) -> Self {
        Self {
            total_count: 0,
            null_count: 0,
            type_counts: IndexMap::new(),
            max_len: 0,
            text_threshold,
        }
    }

    /// Observe a single JSON value for this field.
    pub fn observe(&mut self, value: &Value) {
        self.total_count += 1;
        match value {
            Value::Null => {
                self.null_count += 1;
            }
            Value::Bool(_) => {
                *self.type_counts.entry(InferredType::Boolean).or_insert(0) += 1;
            }
            Value::Number(n) => {
                let t = infer_number_type(n);
                *self.type_counts.entry(t).or_insert(0) += 1;
            }
            Value::String(s) => {
                let t = infer_string_type(s);
                let len = s.len() as u32;
                if len > self.max_len {
                    self.max_len = len;
                }
                *self.type_counts.entry(t).or_insert(0) += 1;
            }
            Value::Object(_) => {
                *self.type_counts.entry(InferredType::Object).or_insert(0) += 1;
            }
            Value::Array(_) => {
                *self.type_counts.entry(InferredType::Array).or_insert(0) += 1;
            }
        }
    }

    /// The dominant (most frequent) non-null type.
    pub fn dominant_type(&self) -> InferredType {
        self.type_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(t, _)| t.clone())
            .unwrap_or(InferredType::Null)
    }

    /// Fraction of rows where type differs from the dominant type (anomaly rate).
    pub fn anomaly_rate(&self) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }
        let dominant_count = self
            .type_counts
            .iter()
            .max_by_key(|(_, c)| *c)
            .map(|(_, c)| *c)
            .unwrap_or(0);
        let anomalous = self.total_count - self.null_count - dominant_count;
        anomalous as f64 / self.total_count as f64
    }

    pub fn is_not_null(&self) -> bool {
        self.null_count == 0
    }

    /// Resolve to the final PostgreSQL type.
    /// Merging rules: the "widest" type wins.
    pub fn to_pg_type(&self) -> PgType {
        let has = |t: &InferredType| self.type_counts.contains_key(t);

        // If any text/string type is dominant, use string types
        if has(&InferredType::Text) || has(&InferredType::Varchar) {
            if self.max_len as u32 > self.text_threshold {
                return PgType::Text;
            } else {
                let sized = (self.max_len as f64 * 1.2).ceil() as u32;
                return PgType::VarChar(sized.max(1));
            }
        }

        // Numeric widening: float > bigint > int
        let has_float = has(&InferredType::Float);
        let has_bigint = has(&InferredType::BigInt);
        let has_int = has(&InferredType::Integer);

        if has_float {
            // If there's a mix of numeric + string, string wins
            return PgType::DoublePrecision;
        }
        if has_bigint {
            return PgType::BigInt;
        }
        if has_int {
            return PgType::Integer;
        }

        if has(&InferredType::Boolean) {
            return PgType::Boolean;
        }
        if has(&InferredType::Timestamp) {
            return PgType::Timestamp;
        }
        if has(&InferredType::Date) {
            return PgType::Date;
        }
        if has(&InferredType::Uuid) {
            return PgType::Uuid;
        }

        // Fallback
        PgType::Text
    }

    /// True if this field contains only objects (→ child table, not a column).
    pub fn is_object_field(&self) -> bool {
        self.type_counts.contains_key(&InferredType::Object)
            && self.type_counts.len() == 1
    }

    /// True if this field contains only arrays (→ child table, not a column).
    pub fn is_array_field(&self) -> bool {
        self.type_counts.contains_key(&InferredType::Array)
            && self.type_counts.len() == 1
    }

    /// True if this field has mixed types that constitute an anomaly.
    pub fn has_anomalies(&self) -> bool {
        self.type_counts.len() > 1
    }
}

// ---------------------------------------------------------------------------
// Public utilities
// ---------------------------------------------------------------------------

/// Return the "wider" of two PgTypes — the one that can represent all values of both.
pub fn widen_pg_types(a: PgType, b: &PgType) -> PgType {
    if a == *b {
        return a;
    }
    match (&a, b) {
        (PgType::Text, _) | (_, PgType::Text) => PgType::Text,
        (PgType::VarChar(_), _) | (_, PgType::VarChar(_)) => PgType::Text,
        (PgType::Jsonb, _) | (_, PgType::Jsonb) => PgType::Text,
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
            if f.fract() == 0.0 && f >= i32::MIN as f64 && f <= i32::MAX as f64 {
                return InferredType::Integer;
            }
            if f.fract() == 0.0 {
                return InferredType::BigInt;
            }
        }
        return InferredType::Float;
    }
    if let Some(i) = n.as_i64() {
        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
            return InferredType::Integer;
        }
        return InferredType::BigInt;
    }
    InferredType::Float
}

/// Regex-free heuristic type detection for strings.
/// Dispatches on length first to eliminate impossible candidates in O(1).
#[inline]
fn infer_string_type(s: &str) -> InferredType {
    let b = s.as_bytes();
    match b.len() {
        // UUID is exactly 36 bytes: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        36 => {
            if is_uuid(s) { InferredType::Uuid } else { InferredType::Varchar }
        }
        // Date is exactly 10 bytes: YYYY-MM-DD
        10 => {
            if is_date_bytes(b) { InferredType::Date } else { InferredType::Varchar }
        }
        // Timestamp needs at least 16 bytes: YYYY-MM-DDTHH:MM
        // (exclude 36 since we've already handled it above and it can't be a valid timestamp)
        n if n >= 16 => {
            if is_timestamp(s) { InferredType::Timestamp } else { InferredType::Varchar }
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

fn is_timestamp(s: &str) -> bool {
    // Minimal check: YYYY-MM-DDTHH:MM or YYYY-MM-DD HH:MM
    let b = s.as_bytes();
    if b.len() < 16 {
        return false;
    }
    is_date_bytes(&b[..10]) && (b[10] == b'T' || b[10] == b' ') && is_digit(b[11]) && is_digit(b[12]) && b[13] == b':' && is_digit(b[14]) && is_digit(b[15])
}

fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10 && is_date_bytes(b)
}

fn is_date_bytes(b: &[u8]) -> bool {
    b.len() >= 10
        && is_digit(b[0]) && is_digit(b[1]) && is_digit(b[2]) && is_digit(b[3])
        && b[4] == b'-'
        && is_digit(b[5]) && is_digit(b[6])
        && b[7] == b'-'
        && is_digit(b[8]) && is_digit(b[9])
}

#[inline]
fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
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
}
