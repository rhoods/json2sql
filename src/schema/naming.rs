use std::collections::HashMap;

/// Maximum PostgreSQL identifier length in bytes.
const PG_MAX_IDENT: usize = 63;
/// Characters reserved for the hash suffix when truncating.
const HASH_SUFFIX_LEN: usize = 8; // "_" + 7 hex chars

/// A table name that was truncated to fit within the 63-byte PostgreSQL limit.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TruncatedName {
    /// Dot-joined original path, e.g. "users.orders.items"
    pub original_path: String,
    /// The full unsanitized name before truncation
    pub full_name: String,
    /// The final PostgreSQL identifier after truncation
    pub pg_name: String,
}

/// A column name collision: multiple JSON fields that sanitize to the same SQL identifier.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnCollision {
    /// Name of the table where the collision occurred
    pub table_name: String,
    /// The sanitized name shared by all colliding fields
    pub sanitized_name: String,
    /// Original JSON field names that caused the collision
    pub original_names: Vec<String>,
    /// Resolved PostgreSQL column names (with hash suffix)
    pub resolved_names: Vec<String>,
}

/// Per-table registry for column name collision detection and resolution.
///
/// Usage:
/// 1. `register()` all original field names
/// 2. `build(table_name)` to detect collisions
/// 3. `resolve(original)` to get the final PostgreSQL column name
#[derive(Debug, Default)]
pub struct ColumnNameRegistry {
    /// sanitized_name → original names that map to it
    candidates: HashMap<String, Vec<String>>,
    /// original_name → resolved pg column name
    resolved: HashMap<String, String>,
    /// collision records (for warnings)
    collisions: Vec<ColumnCollision>,
}

impl ColumnNameRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an original JSON field name.
    pub fn register(&mut self, original: &str) {
        let sanitized = sanitize_identifier(original);
        let truncated = truncate_to_pg_limit(&sanitized, original);
        self.candidates
            .entry(truncated)
            .or_default()
            .push(original.to_string());
    }

    /// Detect collisions and compute resolved names. Must be called after all `register()` calls.
    pub fn build(&mut self, table_name: &str) {
        for (sanitized, originals) in &self.candidates {
            if originals.len() == 1 {
                // No collision — use sanitized name as-is
                self.resolved.insert(originals[0].clone(), sanitized.clone());
            } else {
                // Collision: every colliding field gets a hash suffix derived from its original name
                let max_base = PG_MAX_IDENT - HASH_SUFFIX_LEN;
                let base = if sanitized.len() > max_base {
                    &sanitized[..max_base]
                } else {
                    sanitized.as_str()
                };
                let mut resolved_names = Vec::new();
                for original in originals {
                    let hash = short_hash(original);
                    let resolved = format!("{}_{}", base, hash);
                    self.resolved.insert(original.clone(), resolved.clone());
                    resolved_names.push(resolved);
                }
                self.collisions.push(ColumnCollision {
                    table_name: table_name.to_string(),
                    sanitized_name: sanitized.clone(),
                    original_names: originals.clone(),
                    resolved_names,
                });
            }
        }
    }

    /// Return the resolved PostgreSQL column name for an original JSON field.
    pub fn resolve(&self, original: &str) -> String {
        self.resolved
            .get(original)
            .cloned()
            .unwrap_or_else(|| NamingRegistry::column_name(original))
    }

    /// Return all detected collisions (populated after `build()`).
    pub fn collisions(&self) -> &[ColumnCollision] {
        &self.collisions
    }
}

/// Manages safe PostgreSQL identifier generation.
/// Ensures uniqueness after truncation by appending a hash suffix.
#[derive(Debug, Default)]
pub struct NamingRegistry {
    /// original path → sanitized pg identifier
    cache: HashMap<String, String>,
    /// sanitized identifier → original path (for collision detection)
    reverse: HashMap<String, String>,
    /// names that were truncated (for warning purposes)
    truncations: Vec<TruncatedName>,
}

impl NamingRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert a hierarchical path (e.g. ["users", "orders", "items"]) to a
    /// safe PostgreSQL table name (<= 63 bytes, lowercase, no special chars).
    pub fn table_name(&mut self, path: &[String]) -> String {
        let key = path.join(".");
        if let Some(cached) = self.cache.get(&key) {
            return cached.clone();
        }
        let joined = path.join("_");
        let sanitized = sanitize_identifier(&joined);
        let result = self.ensure_unique(sanitized, &key);
        self.cache.insert(key, result.clone());
        result
    }

    /// Register and return a safe PostgreSQL table name from a pre-computed dot-joined path key
    /// (e.g. `"users.orders.items"`). Avoids two `path.join()` allocations compared to
    /// `table_name(&[String])` — use in hot loops where the dot-key is already available.
    pub fn table_name_from_dot_key(&mut self, dot_key: &str) -> String {
        if let Some(cached) = self.cache.get(dot_key) {
            return cached.clone();
        }
        // Replace '.' with '_' directly — equivalent to path.join("_") for dot-joined keys
        // since JSON field names never contain '.' (it is used exclusively as the path separator).
        let joined = dot_key.replace('.', "_");
        let sanitized = sanitize_identifier(&joined);
        let result = self.ensure_unique(sanitized, dot_key);
        self.cache.insert(dot_key.to_string(), result.clone());
        result
    }

    /// Read-only lookup of a pre-registered path. Must be called only after
    /// `table_name()` has been called for this path (i.e. after the pre-registration
    /// phase in `finalize()`). Safe to call from multiple threads.
    pub fn table_name_lookup(&self, path: &[String]) -> String {
        let key = path.join(".");
        self.cache.get(&key).cloned().unwrap_or_else(|| {
            // Fallback: should never happen after pre-registration, but be safe.
            sanitize_identifier(&path.join("_"))
        })
    }

    /// Read-only lookup from a pre-computed dot-joined key. Avoids the `path.join(".")` allocation.
    /// Use in the parallel schema-building phase where the dot-key is already available.
    pub fn table_name_lookup_from_dot_key(&self, dot_key: &str) -> String {
        self.cache.get(dot_key).cloned().unwrap_or_else(|| {
            sanitize_identifier(&dot_key.replace('.', "_"))
        })
    }

    /// Convert a JSON field name to a safe PostgreSQL column name.
    pub fn column_name(field: &str) -> String {
        let sanitized = sanitize_identifier(field);
        // Column names don't need global uniqueness tracking (they're per-table)
        // but we still need to truncate
        truncate_to_pg_limit(&sanitized, &sanitized)
    }

    fn ensure_unique(&mut self, sanitized: String, original_key: &str) -> String {
        let truncated = truncate_to_pg_limit(&sanitized, original_key);

        // Record truncation if the name was shortened
        if truncated != sanitized {
            self.truncations.push(TruncatedName {
                original_path: original_key.to_string(),
                full_name: sanitized.clone(),
                pg_name: truncated.clone(),
            });
        }

        if let Some(existing) = self.reverse.get(&truncated) {
            if existing == original_key {
                return truncated;
            }
            // Collision after truncation: re-hash the original key differently
            // This should be extremely rare
            let alt = truncate_to_pg_limit(&sanitized, &format!("{}_alt", original_key));
            self.reverse.insert(alt.clone(), original_key.to_string());
            return alt;
        }
        self.reverse.insert(truncated.clone(), original_key.to_string());
        truncated
    }

    /// Returns all table names that were truncated to fit the 63-byte PostgreSQL limit.
    pub fn truncated_names(&self) -> &[TruncatedName] {
        &self.truncations
    }
}

/// Sanitize a string to be a valid PostgreSQL identifier:
/// - Lowercase
/// - Replace non-alphanumeric (except underscore) with underscore
/// - Collapse consecutive underscores
/// - Remove leading/trailing underscores
/// - Prefix with `c_` if starts with a digit
pub fn sanitize_identifier(s: &str) -> String {
    // Fast path: all-ASCII input covers 99%+ of JSON field names and is ~2× faster
    // than the Unicode path because it avoids `to_lowercase()` allocation and `chars()`.
    if s.is_ascii() {
        let mut result = String::with_capacity(s.len());
        let mut last_was_underscore = false;

        for b in s.bytes() {
            match b {
                b'A'..=b'Z' => {
                    result.push((b + 32) as char); // ASCII to_lowercase
                    last_was_underscore = false;
                }
                b'a'..=b'z' | b'0'..=b'9' => {
                    result.push(b as char);
                    last_was_underscore = false;
                }
                b'_' => {
                    if !last_was_underscore && !result.is_empty() {
                        result.push('_');
                        last_was_underscore = true;
                    }
                }
                _ => {
                    if !last_was_underscore && !result.is_empty() {
                        result.push('_');
                        last_was_underscore = true;
                    }
                }
            }
        }

        let result = result.trim_end_matches('_').to_string();
        if result.is_empty() {
            return "col".to_string();
        }
        if result.as_bytes()[0].is_ascii_digit() {
            return format!("c_{}", result);
        }
        return result;
    }

    // Slow path: Unicode input (e.g. Japanese field names like "ja:カルシウム")
    let lower = s.to_lowercase();
    let mut result = String::with_capacity(lower.len());
    let mut last_was_underscore = false;

    for c in lower.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            if c == '_' {
                if !last_was_underscore && !result.is_empty() {
                    result.push('_');
                    last_was_underscore = true;
                }
            } else {
                result.push(c);
                last_was_underscore = false;
            }
        } else {
            if !last_was_underscore && !result.is_empty() {
                result.push('_');
                last_was_underscore = true;
            }
        }
    }

    let result = result.trim_end_matches('_').to_string();
    if result.is_empty() {
        return "col".to_string();
    }
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        return format!("c_{}", result);
    }
    result
}

/// Truncate an identifier to PG_MAX_IDENT bytes.
/// If truncation is needed, replace the last HASH_SUFFIX_LEN bytes with a hash.
fn truncate_to_pg_limit(sanitized: &str, original_key: &str) -> String {
    if sanitized.len() <= PG_MAX_IDENT {
        return sanitized.to_string();
    }
    let hash = short_hash(original_key);
    let prefix_len = PG_MAX_IDENT - HASH_SUFFIX_LEN;
    let prefix = &sanitized[..prefix_len];
    format!("{}_{}", prefix, hash)
}

/// Compute a 7-char hex hash of a string using FNV-1a 64-bit.
/// Implemented inline for stability — output is guaranteed identical across
/// dependency updates and must not change without a snapshot format version bump.
fn short_hash(s: &str) -> String {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let h = s.bytes().fold(FNV_OFFSET, |acc, b| {
        (acc ^ b as u64).wrapping_mul(FNV_PRIME)
    });
    format!("{:07x}", h & 0x0fff_ffff)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_sanitize() {
        assert_eq!(sanitize_identifier("firstName"), "firstname");
        assert_eq!(sanitize_identifier("first-name"), "first_name");
        assert_eq!(sanitize_identifier("first name"), "first_name");
        assert_eq!(sanitize_identifier("123abc"), "c_123abc");
        assert_eq!(sanitize_identifier(""), "col");
    }

    #[test]
    fn test_no_double_underscore() {
        assert_eq!(sanitize_identifier("first__name"), "first_name");
        assert_eq!(sanitize_identifier("a--b"), "a_b");
    }

    #[test]
    fn test_truncation_with_hash() {
        let long = "a".repeat(70);
        let result = truncate_to_pg_limit(&long, &long);
        assert_eq!(result.len(), PG_MAX_IDENT);
        // Different originals → different hashes
        let long2 = "b".repeat(70);
        let result2 = truncate_to_pg_limit(&long2, &long2);
        assert_ne!(result, result2);
    }

    #[test]
    fn test_table_name_registry() {
        let mut reg = NamingRegistry::new();
        let name = reg.table_name(&["users".to_string(), "orders".to_string()]);
        assert_eq!(name, "users_orders");
        // Idempotent
        let name2 = reg.table_name(&["users".to_string(), "orders".to_string()]);
        assert_eq!(name, name2);
    }

    #[test]
    fn test_column_name_registry_no_collision() {
        let mut reg = ColumnNameRegistry::new();
        reg.register("calcium");
        reg.register("iron");
        reg.build("nutrients");
        assert_eq!(reg.resolve("calcium"), "calcium");
        assert_eq!(reg.resolve("iron"), "iron");
        assert!(reg.collisions().is_empty());
    }

    #[test]
    fn test_column_name_registry_collision() {
        // Two fields that both sanitize to "ja" (non-ASCII content stripped)
        let mut reg = ColumnNameRegistry::new();
        reg.register("ja:カルシウム"); // → "ja"
        reg.register("ja:脂質");       // → "ja"
        reg.build("nutriments");

        let collisions = reg.collisions();
        assert_eq!(collisions.len(), 1);
        assert_eq!(collisions[0].sanitized_name, "ja");
        assert_eq!(collisions[0].original_names.len(), 2);

        // Resolved names must be distinct
        let r1 = reg.resolve("ja:カルシウム");
        let r2 = reg.resolve("ja:脂質");
        assert_ne!(r1, r2);

        // Both must start with "ja_"
        assert!(r1.starts_with("ja_"), "got: {}", r1);
        assert!(r2.starts_with("ja_"), "got: {}", r2);

        // Both must fit within 63 chars
        assert!(r1.len() <= 63);
        assert!(r2.len() <= 63);
    }

    #[test]
    fn test_long_table_name() {
        let mut reg = NamingRegistry::new();
        let path: Vec<String> = (0..10).map(|i| format!("level{}", i)).collect();
        let name = reg.table_name(&path);
        assert!(name.len() <= PG_MAX_IDENT, "name too long: {} chars", name.len());
    }

    // --- dot-key fast path must produce identical output to the path-slice version ---

    #[test]
    fn test_table_name_from_dot_key_matches_path_version() {
        let path = vec!["users".to_string(), "orders".to_string(), "items".to_string()];
        let dot_key = path.join(".");

        let mut reg_path = NamingRegistry::new();
        let via_path = reg_path.table_name(&path);

        let mut reg_key = NamingRegistry::new();
        let via_key = reg_key.table_name_from_dot_key(&dot_key);

        assert_eq!(via_path, via_key, "dot-key and path versions must agree");
        assert_eq!(via_key, "users_orders_items");
    }

    #[test]
    fn test_table_name_from_dot_key_special_chars() {
        // Field names with hyphens, mixed case — sanitization must produce same result
        let path = vec!["myRoot".to_string(), "some-field".to_string()];
        let dot_key = path.join(".");

        let mut reg_path = NamingRegistry::new();
        let via_path = reg_path.table_name(&path);

        let mut reg_key = NamingRegistry::new();
        let via_key = reg_key.table_name_from_dot_key(&dot_key);

        assert_eq!(via_path, via_key);
    }

    #[test]
    fn test_table_name_from_dot_key_long_path() {
        let path: Vec<String> = (0..10).map(|i| format!("level{}", i)).collect();
        let dot_key = path.join(".");

        let mut reg_path = NamingRegistry::new();
        let via_path = reg_path.table_name(&path);

        let mut reg_key = NamingRegistry::new();
        let via_key = reg_key.table_name_from_dot_key(&dot_key);

        assert_eq!(via_path, via_key);
        assert!(via_key.len() <= PG_MAX_IDENT);
    }

    #[test]
    fn test_table_name_lookup_from_dot_key_matches_path_version() {
        let path = vec!["products".to_string(), "nutrients".to_string()];
        let dot_key = path.join(".");

        let mut reg = NamingRegistry::new();
        reg.table_name(&path); // pre-register via path version
        let via_path = reg.table_name_lookup(&path);
        let via_key = reg.table_name_lookup_from_dot_key(&dot_key);

        assert_eq!(via_path, via_key);
    }

    #[test]
    fn test_short_hash_stable() {
        // Regression test: these values must never change — they are embedded in
        // existing schema snapshots. Any change here means a snapshot format break.
        assert_eq!(short_hash(""), "4222325");
        assert_eq!(short_hash("abc"), "541574b");
        assert_eq!(short_hash("ja:カルシウム"), "42c9fde");
        // Two distinct inputs must produce distinct hashes
        assert_ne!(short_hash("ja:カルシウム"), short_hash("ja:脂質"));
    }
}
