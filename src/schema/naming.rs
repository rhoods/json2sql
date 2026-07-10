//! `PostgreSQL` identifier sanitization and collision resolution.
//!
//! [`NamingRegistry`] converts raw JSON field names to valid SQL identifiers (≤ 63 bytes),
//! resolves duplicates with a short FNV-1a hash suffix, and caps table names at 53 chars
//! (`PG_TABLE_MAX_IDENT`) so all derived identifiers (`pk_…`, `fk_…_parent`, `j2s_…_id`)
//! stay within `PostgreSQL`'s 63-byte `NAMEDATALEN` limit.
//!
//! Fonctions :
//! - struct `TruncatedName` — nom de table tronqué pour tenir dans la limite 63 octets `PostgreSQL`.
//! - struct `ColumnCollision` — collision de noms de colonnes : plusieurs champs JSON → même identifiant SQL.
//! - struct `ColumnNameRegistry` — registre par table pour la détection/résolution de collisions de colonnes.
//! - fn `ColumnNameRegistry::new` — construit un registre vide.
//! - fn `ColumnNameRegistry::register` — enregistre un nom de champ JSON original.
//! - fn `ColumnNameRegistry::build` — détecte les collisions et calcule les noms résolus.
//! - fn `ColumnNameRegistry::resolve` — retourne le nom de colonne `PostgreSQL` résolu pour un champ.
//! - fn `ColumnNameRegistry::collisions` — retourne les collisions détectées.
//! - struct `NamingRegistry` — génère des identifiants `PostgreSQL` sûrs, assure l'unicité par suffixe hash.
//! - fn `NamingRegistry::new` — crée le registre de noms de table.
//! - fn `NamingRegistry::table_name` — calcule (ou lit du cache) le nom de table PG pour un chemin.
//! - fn `NamingRegistry::table_name_from_dot_key` — variante qui évite une allocation `join()`.
//! - fn `NamingRegistry::table_name_lookup` — lecture seule après pré-enregistrement.
//! - fn `NamingRegistry::table_name_lookup_from_dot_key` — lecture seule (thread-safe, phase parallèle).
//! - fn `NamingRegistry::column_name` — nom de colonne PG sûr pour un champ JSON.
//! - fn `NamingRegistry::ensure_unique` — résout les collisions de noms de table (suffixe hash déterministe).
//! - fn `NamingRegistry::hash_suffixed_name` — construit un nom avec suffixe hash (repli compteur si collision).
//! - fn `NamingRegistry::truncated_names` — noms tronqués à 63 octets (pour les warnings).
//! - fn `sanitize_ascii` — sanitisation rapide pour entrée tout-ASCII (minuscules, `_` pour non-alphanumérique).
//! - fn `sanitize_unicode` — sanitisation pour entrée non-ASCII (même règles, chemin Unicode).
//! - fn `sanitize_identifier` — dispatch vers `sanitize_ascii`/`sanitize_unicode` selon le contenu.
//! - fn `finalize_sanitized` — trim + préfixe `c_` si le résultat commence par un chiffre.
//! - fn `truncate_to_limit` — tronque un identifiant avec suffixe hash à une limite donnée.
//! - fn `truncate_to_pg_limit` — tronque un identifiant à la limite `PostgreSQL` (63 octets).
//! - fn `floor_char_boundary` — tronque au dernier caractère UTF-8 valide.
//! - fn `truncate_table_name` — tronque un nom de table en dépiautant les segments de chemin avant repli hash.
//! - fn `short_hash` — hash FNV-1a 64 bits, 7 caractères hex (stable entre versions).

use std::collections::HashMap;

use super::PATH_SEP;

/// Maximum `PostgreSQL` identifier length in bytes.
const PG_MAX_IDENT: usize = 63;
/// Maximum length for table names, ensuring all derived identifiers fit within `PG_MAX_IDENT`.
/// The tightest constraint is `fk_{name}_parent` (3 + name + 7 = 10 + name ≤ 63 → name ≤ 53).
pub const PG_TABLE_MAX_IDENT: usize = 53;
/// Characters reserved for the hash suffix when truncating.
const HASH_SUFFIX_LEN: usize = 8; // "_" + 7 hex chars

/// A table name that was truncated to fit within the 63-byte `PostgreSQL` limit.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TruncatedName {
    /// Dot-joined original path, e.g. "users.orders.items"
    pub original_path: String,
    /// The full unsanitized name before truncation
    pub full_name: String,
    /// The final `PostgreSQL` identifier after truncation
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
    /// Resolved `PostgreSQL` column names (with hash suffix)
    pub resolved_names: Vec<String>,
}

/// Per-table registry for column name collision detection and resolution.
///
/// Usage:
/// 1. `register()` all original field names
/// 2. `build(table_name)` to detect collisions
/// 3. `resolve(original)` to get the final `PostgreSQL` column name
#[derive(Debug, Default)]
pub struct ColumnNameRegistry {
    /// `sanitized_name` → original names that map to it
    candidates: HashMap<String, Vec<String>>,
    /// `original_name` → resolved pg column name
    resolved: HashMap<String, String>,
    /// collision records (for warnings)
    collisions: Vec<ColumnCollision>,
}

impl ColumnNameRegistry {
    #[must_use]
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
                let base = floor_char_boundary(sanitized, max_base);
                let mut resolved_names = Vec::new();
                for original in originals {
                    let hash = short_hash(original);
                    let resolved = format!("{base}_{hash}");
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

    /// Return the resolved `PostgreSQL` column name for an original JSON field.
    #[must_use]
    pub fn resolve(&self, original: &str) -> String {
        self.resolved
            .get(original)
            .cloned()
            .unwrap_or_else(|| NamingRegistry::column_name(original))
    }

    /// Return all detected collisions (populated after `build()`).
    #[must_use]
    pub fn collisions(&self) -> &[ColumnCollision] {
        &self.collisions
    }
}

/// Manages safe `PostgreSQL` identifier generation.
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert a hierarchical path (e.g. `["users", "orders", "items"]`) to a
    /// safe `PostgreSQL` table name (<= 63 bytes, lowercase, no special chars).
    #[allow(dead_code)]
    pub fn table_name(&mut self, path: &[String]) -> String {
        let key = path.join(&PATH_SEP.to_string());
        if let Some(cached) = self.cache.get(&key) {
            return cached.clone();
        }
        let joined = path.join("_");
        let sanitized = sanitize_identifier(&joined);
        let result = self.ensure_unique(&sanitized, &key);
        self.cache.insert(key, result.clone());
        result
    }

    /// Register and return a safe `PostgreSQL` table name from a pre-computed PATH_SEP-joined key
    /// (e.g. `"users\x00orders\x00items"`). Avoids two `path.join()` allocations compared to
    /// `table_name(&[String])` — use in hot loops where the key is already available.
    pub fn table_name_from_dot_key(&mut self, dot_key: &str) -> String {
        if let Some(cached) = self.cache.get(dot_key) {
            return cached.clone();
        }
        let joined = dot_key.replace(PATH_SEP, "_");
        let sanitized = sanitize_identifier(&joined);
        let result = self.ensure_unique(&sanitized, dot_key);
        self.cache.insert(dot_key.to_string(), result.clone());
        result
    }

    /// Read-only lookup of a pre-registered path. Must be called only after
    /// `table_name()` has been called for this path (i.e. after the pre-registration
    /// phase in `finalize()`). Safe to call from multiple threads.
    #[allow(dead_code)]
    #[must_use]
    pub fn table_name_lookup(&self, path: &[String]) -> String {
        let key = path.join(&PATH_SEP.to_string());
        debug_assert!(self.cache.contains_key(&key),
            "table_name_lookup called for unregistered path — pre-registration phase incomplete");
        self.cache.get(&key).cloned().unwrap_or_else(|| {
            sanitize_identifier(&path.join("_"))
        })
    }

    /// Read-only lookup from a pre-computed PATH_SEP-joined key.
    /// Use in the parallel schema-building phase where the key is already available.
    #[must_use]
    pub fn table_name_lookup_from_dot_key(&self, dot_key: &str) -> String {
        debug_assert!(self.cache.contains_key(dot_key),
            "table_name_lookup_from_dot_key called for unregistered key — pre-registration phase incomplete");
        self.cache.get(dot_key).cloned().unwrap_or_else(|| {
            sanitize_identifier(&dot_key.replace(PATH_SEP, "_"))
        })
    }

    /// Convert a JSON field name to a safe `PostgreSQL` column name.
    #[must_use]
    pub fn column_name(field: &str) -> String {
        let sanitized = sanitize_identifier(field);
        // Column names don't need global uniqueness tracking (they're per-table)
        // but we still need to truncate
        truncate_to_pg_limit(&sanitized, &sanitized)
    }

    fn ensure_unique(&mut self, sanitized: &str, original_key: &str) -> String {
        let truncated = truncate_table_name(sanitized, original_key, PG_TABLE_MAX_IDENT);

        // Record truncation if the name was shortened
        if truncated != sanitized {
            self.truncations.push(TruncatedName {
                original_path: original_key.to_string(),
                full_name: sanitized.to_string(),
                pg_name: truncated.clone(),
            });
        }

        if let Some(existing) = self.reverse.get(&truncated) {
            if existing == original_key {
                // Idempotent: this key is the sentinel owner of `truncated`.
                // Return from cache in case it was already promoted to a hash suffix.
                return self.cache.get(original_key).cloned().unwrap_or(truncated);
            }
            // Collision: two different paths produce the same truncated name.
            // Both get a hash suffix derived from their canonical key — deterministic
            // regardless of registration order.
            let existing_key = existing.clone();

            // Promote the existing key to its hash suffix if it still holds the plain name.
            if self.cache.get(&existing_key).is_some_and(|n| n == &truncated) {
                let promoted = self.hash_suffixed_name(&truncated, &existing_key);
                self.cache.insert(existing_key.clone(), promoted.clone());
                self.reverse.insert(promoted, existing_key);
                // `reverse[truncated]` is kept as a sentinel so future collisions are detected.
            }

            let candidate = self.hash_suffixed_name(&truncated, original_key);
            self.reverse.insert(candidate.clone(), original_key.to_string());
            return candidate;
        }
        self.reverse.insert(truncated.clone(), original_key.to_string());
        truncated
    }

    /// Build a hash-suffixed name from `base` using `original_key` as entropy source.
    /// Falls back to a counter if the hash itself collides in the registry (extremely rare).
    fn hash_suffixed_name(&self, base: &str, original_key: &str) -> String {
        let hash = short_hash(original_key);
        let prefix = floor_char_boundary(base, PG_TABLE_MAX_IDENT - HASH_SUFFIX_LEN);
        let candidate = format!("{prefix}_{hash}");

        if !self.reverse.contains_key(candidate.as_str()) {
            return candidate;
        }
        // Hash collision fallback: append counter until a free slot is found.
        let mut counter = 2usize;
        loop {
            let suffix = format!("_{counter}");
            let base_part = floor_char_boundary(&candidate, PG_TABLE_MAX_IDENT - suffix.len());
            let candidate2 = format!("{base_part}{suffix}");
            if !self.reverse.contains_key(candidate2.as_str()) {
                return candidate2;
            }
            counter += 1;
        }
    }

    /// Returns all table names that were truncated to fit the 63-byte `PostgreSQL` limit.
    #[must_use]
    pub fn truncated_names(&self) -> &[TruncatedName] {
        &self.truncations
    }
}

/// Sanitize a string to be a valid `PostgreSQL` identifier:
/// - Lowercase
/// - Replace non-alphanumeric (except underscore) with underscore
/// - Collapse consecutive underscores
/// - Remove leading/trailing underscores
// Fast path: all-ASCII input covers 99%+ of JSON field names and is ~2× faster
// than the Unicode path because it avoids `to_lowercase()` allocation and `chars()`.
fn sanitize_ascii(s: &str) -> String {
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
            _ => {
                if !last_was_underscore && !result.is_empty() {
                    result.push('_');
                    last_was_underscore = true;
                }
            }
        }
    }
    finalize_sanitized(&result)
}

// Slow path: Unicode input (e.g. Japanese field names like "ja:カルシウム")
fn sanitize_unicode(s: &str) -> String {
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
        } else if !last_was_underscore && !result.is_empty() {
            result.push('_');
            last_was_underscore = true;
        }
    }
    finalize_sanitized(&result)
}

/// - Prefix with `c_` if starts with a digit
#[must_use]
pub fn sanitize_identifier(s: &str) -> String {
    if s.is_ascii() { sanitize_ascii(s) } else { sanitize_unicode(s) }
}

fn finalize_sanitized(result: &str) -> String {
    let result = result.trim_end_matches('_').to_string();
    if result.is_empty() { return "col".to_string(); }
    if result.as_bytes().first().is_some_and(u8::is_ascii_digit) {
        return format!("c_{result}");
    }
    result
}

/// Truncate an identifier to `max_len` bytes.
/// If truncation is needed, replace the last `HASH_SUFFIX_LEN` bytes with a hash.
fn truncate_to_limit(sanitized: &str, original_key: &str, max_len: usize) -> String {
    if sanitized.len() <= max_len {
        return sanitized.to_string();
    }
    let hash = short_hash(original_key);
    let prefix_len = max_len - HASH_SUFFIX_LEN;
    let prefix = floor_char_boundary(sanitized, prefix_len);
    format!("{prefix}_{hash}")
}

fn truncate_to_pg_limit(sanitized: &str, original_key: &str) -> String {
    truncate_to_limit(sanitized, original_key, PG_MAX_IDENT)
}

fn floor_char_boundary(s: &str, max_bytes: usize) -> &str {
    &s[..s.floor_char_boundary(max_bytes)]
}

/// Truncate a table name to `max_len` bytes by progressively stripping leading path segments
/// (separated by [`PATH_SEP`]) before falling back to a hash suffix.
///
/// Strategy (in order):
/// 1. If `sanitized` already fits — return as-is.
/// 2. For each `start` in `1..segments.len()`: re-sanitize `segments[start..]` joined by `_`.
///    Return the first result that fits.
/// 3. Hash fallback: keep direct parent + leaf segment, truncated to fit, with a hash suffix
///    that preserves uniqueness across different original paths.
fn truncate_table_name(sanitized: &str, original_key: &str, max_len: usize) -> String {
    if sanitized.len() <= max_len {
        return sanitized.to_string();
    }

    let hash = short_hash(original_key);

    // Phase 1 — strip leading segments one at a time
    let segs: Vec<&str> = original_key.split(PATH_SEP).collect();
    for start in 1..segs.len() {
        let joined = segs[start..].join("_");
        let candidate = sanitize_identifier(&joined);
        if candidate.len() <= max_len {
            return candidate;
        }
    }

    // Phase 2 — hash fallback
    // Single-segment path: no parent concept, format = "{leaf_truncated}_{hash}"
    if segs.len() < 2 {
        let leaf = sanitize_identifier(segs[0]);
        let leaf_used = floor_char_boundary(&leaf, max_len - HASH_SUFFIX_LEN);
        return format!("{leaf_used}_{hash}");
    }

    // Multi-segment: format = "{parent_truncated}_{leaf_truncated}_{hash}"
    // Budget: parent_len + 1 (sep) + leaf_len + HASH_SUFFIX_LEN ≤ max_len
    // content_budget = max_len - HASH_SUFFIX_LEN covers "parent_sep_leaf"
    let content_budget = max_len - HASH_SUFFIX_LEN;
    let parent_sanitized = sanitize_identifier(segs[segs.len() - 2]);
    let leaf_sanitized = sanitize_identifier(segs[segs.len() - 1]);

    // Reserve at least 1 byte for leaf + 1 for the separator between parent and leaf
    let parent_max = content_budget.saturating_sub(2);
    let parent_used = floor_char_boundary(&parent_sanitized, parent_max);

    let leaf_budget = content_budget.saturating_sub(parent_used.len() + 1);
    if leaf_budget == 0 {
        return format!("{parent_used}_{hash}");
    }
    let leaf_used = floor_char_boundary(&leaf_sanitized, leaf_budget);
    format!("{parent_used}_{leaf_used}_{hash}")
}

/// Compute a 7-char hex hash of a string using FNV-1a 64-bit.
/// Implemented inline for stability — output is guaranteed identical across
/// dependency updates and must not change without a snapshot format version bump.
fn short_hash(s: &str) -> String {
    const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;
    let h = s.bytes().fold(FNV_OFFSET, |acc, b| {
        (acc ^ u64::from(b)).wrapping_mul(FNV_PRIME)
    });
    format!("{:07x}", h & 0x0fff_ffff)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- floor_char_boundary ---

    // --- truncate_table_name ---

    fn key(segs: &[&str]) -> String { segs.join(&PATH_SEP.to_string()) }

    #[test]
    fn truncate_table_no_truncation_needed() {
        let k = key(&["users", "orders"]);
        assert_eq!(truncate_table_name("users_orders", &k, 53), "users_orders");
    }

    #[test]
    fn truncate_table_phase1_removes_one_segment() {
        // "openfoodfacts_products_images_selected_nutrition_new_lc_sizes" = 61 chars > 53
        // segments[1..] = ["selected_nutrition_new_lc_sizes"] = 31 chars ≤ 53
        let k = key(&["openfoodfacts_products_images", "selected_nutrition_new_lc_sizes"]);
        let sanitized = "openfoodfacts_products_images_selected_nutrition_new_lc_sizes";
        let result = truncate_table_name(sanitized, &k, 53);
        assert_eq!(result, "selected_nutrition_new_lc_sizes");
    }

    #[test]
    fn truncate_table_phase1_removes_two_segments() {
        // max_len=15 so only the last segment ("end") fits alone
        let k = key(&["root", "medium_segment", "end"]);
        // start=1: "medium_segment_end" = 18 > 15
        // start=2: "end" = 3 ≤ 15 ✓
        let sanitized = "root_medium_segment_end";
        let result = truncate_table_name(sanitized, &k, 15);
        assert_eq!(result, "end");
    }

    #[test]
    fn truncate_table_phase2_fallback_hash() {
        // max_len=15: even the leaf alone (20 chars) > 15 → Phase 2
        let k = key(&["root", "parent_long", "leaf_is_too_long_yes"]);
        let sanitized = "root_parent_long_leaf_is_too_long_yes";
        let result = truncate_table_name(sanitized, &k, 15);
        assert!(result.len() <= 15, "result '{}' is {} chars", result, result.len());
        // Must end with the hash of the original key
        let h = short_hash(&k);
        assert!(result.ends_with(&format!("_{h}")), "result '{result}' must end with _{h}");
        // Must contain parent prefix "paren" (5 chars from "parent_long")
        assert!(result.starts_with("paren"), "result '{result}' must start with parent prefix");
    }

    #[test]
    fn truncate_table_phase2_single_segment() {
        // No PATH_SEP — single segment longer than max_len → leaf-only fallback
        let k = "this_is_a_single_and_very_long_segment";
        let result = truncate_table_name(k, k, 15);
        assert!(result.len() <= 15, "result '{}' is {} chars", result, result.len());
        let h = short_hash(k);
        assert!(result.ends_with(&format!("_{h}")), "must end with hash");
        // Format: {leaf_truncated}_{hash} — no double-parent
        assert!(result.starts_with("this_is"), "must start with leaf prefix");
    }

    #[test]
    fn truncate_table_phase2_result_fits_real_pg_limit() {
        // Real-world: path with leaf > 53 chars (padded to 55)
        let leaf = "a".repeat(55);
        let k = format!("root{PATH_SEP}{leaf}");
        let sanitized = format!("root_{}", "a".repeat(55));
        let result = truncate_table_name(&sanitized, &k, PG_TABLE_MAX_IDENT);
        assert!(result.len() <= PG_TABLE_MAX_IDENT,
            "result '{}' is {} chars (> {})", result, result.len(), PG_TABLE_MAX_IDENT);
    }

    #[test]
    fn truncate_table_real_world_openfoodfacts() {
        // Real-world: 4-level deep path, first trim finds fit at level 2
        let k = key(&["openfoodfacts_org", "products", "nutriments", "energy_kcal_100g_details"]);
        // "openfoodfacts_org_products_nutriments_energy_kcal_100g_details" = 62 > 53
        // start=1: "products_nutriments_energy_kcal_100g_details" = 44 ≤ 53 ✓
        let sanitized = "openfoodfacts_org_products_nutriments_energy_kcal_100g_details";
        let result = truncate_table_name(sanitized, &k, 53);
        assert_eq!(result, "products_nutriments_energy_kcal_100g_details");
    }

    #[test]
    fn truncate_table_two_different_paths_produce_different_names() {
        // Even after trimming to the same tail, different original_keys → different hashes
        let k1 = key(&["prefix_a", "parent_long", "leaf_is_too_long_yes"]);
        let k2 = key(&["prefix_b", "parent_long", "leaf_is_too_long_yes"]);
        let s = "prefix_x_parent_long_leaf_is_too_long_yes";
        let r1 = truncate_table_name(s, &k1, 15);
        let r2 = truncate_table_name(s, &k2, 15);
        assert_ne!(r1, r2, "different original paths must produce different fallback names");
    }

    #[test]
    fn truncate_table_utf8_segment_no_panic() {
        // Japanese chars in the parent segment — sanitize_identifier strips non-ASCII,
        // so floor_char_boundary always receives ASCII output (no UTF-8 boundary panic).
        let k = key(&["root", "ja:カルシウム", "details_very_very_long_name_yes_indeed"]);
        // "root_ja_details_very_very_long_name_yes_indeed" after full sanitization
        let sanitized = sanitize_identifier("root_ja:カルシウム_details_very_very_long_name_yes_indeed");
        let result = truncate_table_name(&sanitized, &k, 15);
        assert!(result.len() <= 15, "result '{result}' exceeds 15 chars");
        // Must be valid ASCII (no panics during construction)
        assert!(result.is_ascii(), "result '{result}' must be ASCII");
    }

    #[test]
    fn truncate_table_collision_post_trim_produces_deterministic_names() {
        // Two paths that both Phase-1 trim to the same name ("shared_suffix") → collision.
        // Both get a hash suffix derived from their canonical key — base is the trimmed name,
        // not the long pre-truncation form.
        let long_prefix_a = "a".repeat(40); // 40 chars → full name 54 chars > 53
        let long_prefix_b = "b".repeat(40); // 40 chars → full name 54 chars > 53
        let path_a = vec![long_prefix_a, "shared_suffix".to_string()];
        let path_b = vec![long_prefix_b, "shared_suffix".to_string()];
        let key_a = path_a.join(&PATH_SEP.to_string());
        let key_b = path_b.join(&PATH_SEP.to_string());
        let mut reg = NamingRegistry::new();
        reg.table_name(&path_a);
        reg.table_name(&path_b);
        // Read via lookup to get the final (post-promotion) names — mirrors finalizer pattern.
        let name_a = reg.table_name_lookup_from_dot_key(&key_a);
        let name_b = reg.table_name_lookup_from_dot_key(&key_b);
        assert_ne!(name_a, name_b, "colliding trimmed names must be deduplicated");
        assert!(name_a.len() <= PG_TABLE_MAX_IDENT, "name_a '{name_a}' too long");
        assert!(name_b.len() <= PG_TABLE_MAX_IDENT, "name_b '{name_b}' too long");
        // Both must start with the trimmed base — not the long pre-truncation sanitized form.
        assert!(name_a.starts_with("shared_suffix"), "name_a '{name_a}' must start with shared_suffix");
        assert!(name_b.starts_with("shared_suffix"), "name_b '{name_b}' must start with shared_suffix");
    }

    #[test]
    #[allow(clippy::similar_names)] // name_a*/name_b* deliberately parallel, testing insertion-order independence
    fn ensure_unique_collision_is_deterministic() {
        // Two paths that both Phase-1 trim to "shared_suffix" must get the SAME names
        // regardless of registration order — verifies hash-based (not counter-based) collision.
        //
        // Mirrors the finalizer pattern: register all paths first (return values discarded),
        // then read via lookup — which returns the final (post-promotion) names.
        let long_prefix_a = "a".repeat(40);
        let long_prefix_b = "b".repeat(40);
        let path_a = vec![long_prefix_a, "shared_suffix".to_string()];
        let path_b = vec![long_prefix_b, "shared_suffix".to_string()];
        let key_a = path_a.join(&PATH_SEP.to_string());
        let key_b = path_b.join(&PATH_SEP.to_string());

        // Order 1: register A then B, then lookup both
        let mut reg1 = NamingRegistry::new();
        reg1.table_name(&path_a);
        reg1.table_name(&path_b);
        let name_a1 = reg1.table_name_lookup_from_dot_key(&key_a);
        let name_b1 = reg1.table_name_lookup_from_dot_key(&key_b);

        // Order 2: register B then A, then lookup both
        let mut reg2 = NamingRegistry::new();
        reg2.table_name(&path_b);
        reg2.table_name(&path_a);
        let name_a2 = reg2.table_name_lookup_from_dot_key(&key_a);
        let name_b2 = reg2.table_name_lookup_from_dot_key(&key_b);

        assert_eq!(name_a1, name_a2, "path A must get same name regardless of insertion order");
        assert_eq!(name_b1, name_b2, "path B must get same name regardless of insertion order");
        assert_ne!(name_a1, name_b1, "paths must produce distinct names");
        assert!(name_a1.len() <= PG_TABLE_MAX_IDENT, "name_a '{name_a1}' too long");
        assert!(name_b1.len() <= PG_TABLE_MAX_IDENT, "name_b '{name_b1}' too long");
    }

    #[test]
    #[allow(clippy::similar_names)] // name_a*/name_b* deliberately parallel, testing insertion-order independence
    fn direct_sanitization_collision_produces_distinct_deterministic_names() {
        // Two short paths that sanitize to the same name WITHOUT truncation — exercises the
        // collision path in ensure_unique where truncated == sanitized (no Phase-1 strip).
        // "foo_bar" and "foo-bar" both sanitize to "foo_bar"; "users.id" and "users_id" → "users_id".
        let path_a = vec!["foo_bar".to_string()];
        let path_b = vec!["foo-bar".to_string()]; // hyphen → underscore → same sanitized name
        let key_a = path_a.join(&PATH_SEP.to_string());
        let key_b = path_b.join(&PATH_SEP.to_string());

        // Order 1: A then B
        let mut reg1 = NamingRegistry::new();
        reg1.table_name(&path_a);
        reg1.table_name(&path_b);
        let name_a1 = reg1.table_name_lookup_from_dot_key(&key_a);
        let name_b1 = reg1.table_name_lookup_from_dot_key(&key_b);

        // Order 2: B then A
        let mut reg2 = NamingRegistry::new();
        reg2.table_name(&path_b);
        reg2.table_name(&path_a);
        let name_a2 = reg2.table_name_lookup_from_dot_key(&key_a);
        let name_b2 = reg2.table_name_lookup_from_dot_key(&key_b);

        assert_ne!(name_a1, name_b1, "colliding sanitized names must be distinct");
        assert_eq!(name_a1, name_a2, "path A must get same name regardless of order");
        assert_eq!(name_b1, name_b2, "path B must get same name regardless of order");
        assert!(name_a1.starts_with("foo_bar"), "name_a '{name_a1}' must start with foo_bar");
        assert!(name_b1.starts_with("foo_bar"), "name_b '{name_b1}' must start with foo_bar");
        assert!(name_a1.len() <= PG_TABLE_MAX_IDENT);
        assert!(name_b1.len() <= PG_TABLE_MAX_IDENT);
    }

    #[test]
    fn ensure_unique_counter_no_overflow_at_100() {
        // Shared suffix of 51 chars → truncated = 51 chars → base_len = 50 with old static formula.
        // At counter=100: base(50) + "_100"(4) = 54 > PG_TABLE_MAX_IDENT(53) before fix.
        let shared: String = "s".repeat(51);
        let mut reg = NamingRegistry::new();
        for i in 0..102usize {
            // Long prefix ensures sanitized > 53 chars → Phase-1 strips it → truncated = shared (51 chars).
            let prefix = format!("{}{i:04}", "a".repeat(40));
            let name = reg.table_name(&[prefix, shared.clone()]);
            assert!(
                name.len() <= PG_TABLE_MAX_IDENT,
                "collision #{i}: '{name}' len={} > PG_TABLE_MAX_IDENT={PG_TABLE_MAX_IDENT}",
                name.len()
            );
        }
    }

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
    fn truncate_to_limit_utf8_no_panic() {
        // 54 ASCII + 'é' (2 bytes) + padding: byte 55 is 0xA9 (continuation byte) = prefix_len boundary.
        // PG_MAX_IDENT=63, HASH_SUFFIX_LEN=8 → prefix_len=55. Before fix: &s[..55] panics.
        let s = format!("{}{}{}", "a".repeat(54), "é", "b".repeat(15));
        assert!(s.len() > PG_MAX_IDENT, "precondition: must exceed limit");
        let result = truncate_to_pg_limit(&s, &s);
        assert!(result.len() <= PG_MAX_IDENT);
        assert!(std::str::from_utf8(result.as_bytes()).is_ok(), "result must be valid UTF-8");
    }

    #[test]
    fn column_registry_build_utf8_no_panic() {
        // Inject a multi-byte key directly into candidates (latent scenario: sanitize_identifier
        // is ASCII-only today, but this guard protects against future Unicode extension).
        // max_base = PG_MAX_IDENT - HASH_SUFFIX_LEN = 55. Key straddles that boundary.
        let multi = format!("{}{}", "a".repeat(54), "é"); // len=56 > max_base=55
        let mut reg = ColumnNameRegistry::new();
        reg.candidates.insert(multi, vec!["field_a".to_string(), "field_b".to_string()]);
        reg.build("test_table"); // must not panic
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
        assert!(r1.starts_with("ja_"), "got: {r1}");
        assert!(r2.starts_with("ja_"), "got: {r2}");

        // Both must fit within 63 chars
        assert!(r1.len() <= 63);
        assert!(r2.len() <= 63);
    }

    #[test]
    fn test_long_table_name() {
        let mut reg = NamingRegistry::new();
        let path: Vec<String> = (0..10).map(|i| format!("level{i}")).collect();
        let name = reg.table_name(&path);
        // Must fit PG_TABLE_MAX_IDENT (53), not just PG_MAX_IDENT (63):
        // derived identifiers like fk_{name}_parent need the 10-char budget.
        assert!(name.len() <= PG_TABLE_MAX_IDENT, "name too long: {} chars", name.len());
    }

    // --- dot-key fast path must produce identical output to the path-slice version ---

    #[test]
    fn test_table_name_from_dot_key_matches_path_version() {
        let path = vec!["users".to_string(), "orders".to_string(), "items".to_string()];
        let key = path.join(&PATH_SEP.to_string());

        let mut reg_path = NamingRegistry::new();
        let via_path = reg_path.table_name(&path);

        let mut reg_key = NamingRegistry::new();
        let via_key = reg_key.table_name_from_dot_key(&key);

        assert_eq!(via_path, via_key, "key and path versions must agree");
        assert_eq!(via_key, "users_orders_items");
    }

    #[test]
    fn test_table_name_from_dot_key_special_chars() {
        // Field names with hyphens, mixed case — sanitization must produce same result
        let path = vec!["myRoot".to_string(), "some-field".to_string()];
        let key = path.join(&PATH_SEP.to_string());

        let mut reg_path = NamingRegistry::new();
        let via_path = reg_path.table_name(&path);

        let mut reg_key = NamingRegistry::new();
        let via_key = reg_key.table_name_from_dot_key(&key);

        assert_eq!(via_path, via_key);
    }

    #[test]
    fn test_table_name_from_dot_key_long_path() {
        let path: Vec<String> = (0..10).map(|i| format!("level{i}")).collect();
        let key = path.join(&PATH_SEP.to_string());

        let mut reg_path = NamingRegistry::new();
        let via_path = reg_path.table_name(&path);

        let mut reg_key = NamingRegistry::new();
        let via_key = reg_key.table_name_from_dot_key(&key);

        assert_eq!(via_path, via_key);
        assert!(via_key.len() <= PG_MAX_IDENT);
    }

    #[test]
    fn test_table_name_lookup_from_dot_key_matches_path_version() {
        let path = vec!["products".to_string(), "nutrients".to_string()];
        let key = path.join(&PATH_SEP.to_string());

        let mut reg = NamingRegistry::new();
        reg.table_name(&path); // pre-register via path version
        let via_path = reg.table_name_lookup(&path);
        let via_key = reg.table_name_lookup_from_dot_key(&key);

        assert_eq!(via_path, via_key);
    }

    // --- PG_TABLE_MAX_IDENT budget: all derived identifiers must fit in 63 chars ---

    #[test]
    fn table_name_max_fits_pk_constraint() {
        // pk_{name} must be <= 63: budget is 60 chars for name
        let mut reg = NamingRegistry::new();
        // Use a path that produces a long name (> 53 chars without the fix)
        let path: Vec<String> = vec![
            "openfoodfacts_products_images".to_string(),
            "selected_nutrition_new_lc_sizes".to_string(),
        ];
        let name = reg.table_name(&path);
        assert!(
            format!("pk_{name}").len() <= 63,
            "pk_{} is {} chars (> 63)", name, format!("pk_{name}").len()
        );
    }

    #[test]
    fn table_name_max_fits_fk_constraint() {
        // fk_{name}_parent must be <= 63: budget is 53 chars for name (most restrictive)
        let mut reg = NamingRegistry::new();
        let path: Vec<String> = vec![
            "openfoodfacts_products_images".to_string(),
            "selected_nutrition_new_lc_sizes".to_string(),
        ];
        let name = reg.table_name(&path);
        assert!(
            format!("fk_{name}_parent").len() <= 63,
            "fk_{}_parent is {} chars (> 63)", name, format!("fk_{name}_parent").len()
        );
    }

    #[test]
    fn table_name_max_fits_j2s_id_column() {
        // j2s_{name}_id must be <= 63: budget is 56 chars for name
        let mut reg = NamingRegistry::new();
        let path: Vec<String> = vec![
            "openfoodfacts_products_images".to_string(),
            "selected_nutrition_new_lc_sizes".to_string(),
        ];
        let name = reg.table_name(&path);
        assert!(
            format!("j2s_{name}_id").len() <= 63,
            "j2s_{}_id is {} chars (> 63)", name, format!("j2s_{name}_id").len()
        );
    }

    #[test]
    fn table_name_fits_pg_table_budget() {
        // All table names must be <= PG_TABLE_MAX_IDENT (53)
        let mut reg = NamingRegistry::new();
        let path: Vec<String> = vec![
            "openfoodfacts_products_images".to_string(),
            "selected_nutrition_new_lc_sizes".to_string(),
        ];
        let name = reg.table_name(&path);
        assert!(
            name.len() <= PG_TABLE_MAX_IDENT,
            "table name '{}' is {} chars (> {})", name, name.len(), PG_TABLE_MAX_IDENT
        );
    }

    #[test]
    fn short_table_name_unchanged_by_tighter_budget() {
        // Names already within 53 chars must not be modified
        let mut reg = NamingRegistry::new();
        let name = reg.table_name(&["users".to_string(), "orders".to_string()]);
        assert_eq!(name, "users_orders");
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
