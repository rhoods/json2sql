//! Vérifie que le header `//!` `Fonctions :` de chaque fichier `.rs` de `src/`,
//! `json2sql-worker/src/` et `json2sql-ui/src/` reste synchronisé avec le code réel (issue #33).
//!
//! Ce fichier n'entre pas lui-même dans le périmètre de la convention (`tests/` est hors scope).

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;

/// Recursively collects all `.rs` files under the given root directories.
fn collect_rs_files(roots: &[&Path]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        walk(root, &mut files);
    }
    files
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Returns the leading `//!` header's `Fonctions :` block as flattened text (comment markers
/// stripped, lines joined by spaces), or `None` if the file has no such block.
fn fonctions_block_text(content: &str) -> Option<String> {
    let header: Vec<&str> = content
        .lines()
        .take_while(|line| line.starts_with("//!"))
        .collect();
    let start = header.iter().position(|line| line.contains("Fonctions"))?;
    Some(
        header[start..]
            .iter()
            .map(|line| line.trim_start_matches("//!").trim())
            .collect::<Vec<_>>()
            .join(" "),
    )
}

/// True if the file's leading `//!` doc-comment block contains a `Fonctions :` marker line.
fn has_fonctions_block(content: &str) -> bool {
    fonctions_block_text(content).is_some()
}

/// Result of extracting and existence-checking the names documented in a `Fonctions :` block.
#[derive(Debug, Default)]
struct DocCheckResult {
    /// Documented names with a matching `fn` definition found in the file.
    existing: HashSet<String>,
    /// Documented names with no matching `fn` definition — stale header entry.
    missing: HashSet<String>,
}

/// Parses the `Fonctions :` block and, for each documented name, checks whether a matching
/// `fn` definition exists in `content`. Only backtick tokens immediately preceding the entry's
/// em-dash separator (`—`) are treated as names — incidental backtick mentions inside a
/// description (e.g. `` `Ok(None)` ``, `` `run()` ``) are ignored because they don't precede one.
fn check_documented_names(content: &str) -> DocCheckResult {
    let mut result = DocCheckResult::default();
    let Some(block) = fonctions_block_text(content) else {
        return result;
    };

    let entry_re = Regex::new(r"(`[^`]+`(?:\s*,\s*`[^`]+`)*)\s*(?:\(`impl[^`]*`\)\s*)?—").unwrap();
    let name_re = Regex::new(r"`([^`]+)`").unwrap();

    for entry in entry_re.captures_iter(&block) {
        for name_match in name_re.captures_iter(&entry[1]) {
            let raw = &name_match[1];
            // Strip call/argument decoration, e.g. `InferredStrategy::from(&UserOverride)` → `InferredStrategy::from`.
            let name = raw.split('(').next().unwrap_or(raw).trim();
            if name.is_empty() {
                continue;
            }
            if fn_exists(content, name) {
                result.existing.insert(name.to_string());
            } else {
                result.missing.insert(name.to_string());
            }
        }
    }
    result
}

/// Existence-only check (not full attribution to the right `impl` block — that's the
/// code-side extraction's responsibility). Tolerant to generic parameters between the
/// function name and its argument list (`fn foo<T>(` still matches `foo`).
fn fn_exists(content: &str, name: &str) -> bool {
    let target = name.rsplit("::").next().unwrap_or(name);
    let pattern = format!(r"fn\s+{}\s*(?:<[^>]*>)?\s*\(", regex::escape(target));
    Regex::new(&pattern).is_ok_and(|re| re.is_match(content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_file(dir: &Path, relative: &str, content: &str) {
        let path = dir.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
    }

    #[test]
    fn finds_rs_files_recursively_across_roots() {
        let root_a = TempDir::new().unwrap();
        let root_b = TempDir::new().unwrap();
        write_file(root_a.path(), "top.rs", "// top");
        write_file(root_a.path(), "nested/inner.rs", "// inner");
        write_file(root_b.path(), "other.rs", "// other");
        write_file(root_a.path(), "notes.txt", "not rust");

        let found = collect_rs_files(&[root_a.path(), root_b.path()]);

        let names: Vec<_> = found
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap())
            .collect();
        assert_eq!(found.len(), 3);
        assert!(names.contains(&"top.rs"));
        assert!(names.contains(&"inner.rs"));
        assert!(names.contains(&"other.rs"));
    }

    #[test]
    fn detects_fonctions_block_in_leading_header() {
        let content = "//! Responsabilité du fichier.\n//!\n//! Fonctions :\n//! - `foo` — fait un truc.\n\nfn foo() {}\n";
        assert!(has_fonctions_block(content));
    }

    #[test]
    fn ignores_header_without_fonctions_marker() {
        let content = "//! Sous-modules :\n//! - [`scoring`] — description.\n\npub mod scoring;\n";
        assert!(!has_fonctions_block(content));
    }

    #[test]
    fn ignores_file_with_no_header_at_all() {
        let content = "use std::fs;\n\nfn foo() {}\n";
        assert!(!has_fonctions_block(content));
    }

    #[test]
    fn ignores_fonctions_mentioned_outside_the_leading_header() {
        // "Fonctions" appearing after the header block (e.g. inside a regular code comment)
        // must not count — the marker is only valid inside the contiguous `//!` block at the
        // very top of the file.
        let content = "//! Responsabilité du fichier.\n\nuse std::fs;\n\n// Liste des Fonctions ici, mais hors du header.\nfn foo() {}\n";
        assert!(!has_fonctions_block(content));
    }

    #[test]
    fn documented_simple_function_with_backing_fn_is_existing() {
        let content = "//! Fonctions :\n//! - `foo` — fait un truc.\n\nfn foo() {}\n";
        let result = check_documented_names(content);
        assert!(result.existing.contains("foo"));
        assert!(result.missing.is_empty());
    }

    #[test]
    fn documented_name_without_backing_fn_is_missing() {
        let content = "//! Fonctions :\n//! - `ghost_fn` — n'existe plus dans le code.\n\nfn foo() {}\n";
        let result = check_documented_names(content);
        assert!(result.missing.contains("ghost_fn"));
        assert!(result.existing.is_empty());
    }

    #[test]
    fn impl_method_entry_with_decorated_name_is_cleaned_and_matched() {
        // Real pattern from src/schema/table_schema.rs:31
        let content = "//! Fonctions :\n//! - `InferredStrategy::from(&UserOverride)` (`impl From`) — projette un override.\n\nimpl From<UserOverride> for InferredStrategy {\n    fn from(value: UserOverride) -> Self { todo!() }\n}\n";
        let result = check_documented_names(content);
        assert!(result.existing.contains("InferredStrategy::from"));
        assert!(!result.existing.contains("InferredStrategy::from(&UserOverride)"));
        assert!(result.missing.is_empty());
    }

    #[test]
    fn grouped_comma_separated_entries_are_extracted_individually() {
        let content = "//! Fonctions :\n//! - `foo`, `bar` — deux fonctions liées.\n\nfn foo() {}\nfn bar() {}\n";
        let result = check_documented_names(content);
        assert!(result.existing.contains("foo"));
        assert!(result.existing.contains("bar"));
    }

    #[test]
    fn section_grouped_multiline_entries_like_runner_rs_are_all_extracted() {
        // Real pattern from src/pass2/runner.rs — multiple entries per bullet, section-labeled,
        // wrapped across lines, separated by `;`.
        let content = "//! Fonctions (par section) :\n//! - Orchestration : `run` — pipeline complet ;\n//!   `build_pass2_result` — assemble le résultat.\n\nfn run() {}\nfn build_pass2_result() {}\n";
        let result = check_documented_names(content);
        assert!(result.existing.contains("run"));
        assert!(result.existing.contains("build_pass2_result"));
    }

    #[test]
    fn incidental_backtick_mentions_in_description_are_not_extracted_as_names() {
        // Real pattern from src/schema/table_schema.rs:40 — `effective_strategy().absorbs_children()`
        // appears in the description, not as a separate documented entry.
        let content = "//! Fonctions :\n//! - `TableSchema::absorbs_children` — délègue à `effective_strategy().absorbs_children()`.\n\nimpl TableSchema {\n    fn absorbs_children(&self) -> bool { todo!() }\n}\n";
        let result = check_documented_names(content);
        assert!(result.existing.contains("TableSchema::absorbs_children"));
        assert!(!result.existing.contains("effective_strategy"));
        assert!(!result.missing.contains("effective_strategy"));
    }

    #[test]
    fn generic_function_is_recognized_as_existing() {
        // Real pattern from json2sql-ui/src/screens/mod.rs:38 — `fn use_elapsed_timer<F>(...)`.
        let content = "//! Fonctions :\n//! - `use_elapsed_timer` — hook Dioxus.\n\npub fn use_elapsed_timer<F>(is_done: F) -> u32 { todo!() }\n";
        let result = check_documented_names(content);
        assert!(result.existing.contains("use_elapsed_timer"));
        assert!(result.missing.is_empty());
    }

    #[test]
    fn file_without_fonctions_block_yields_empty_result() {
        let content = "use std::fs;\n\nfn foo() {}\n";
        let result = check_documented_names(content);
        assert!(result.existing.is_empty());
        assert!(result.missing.is_empty());
    }
}
