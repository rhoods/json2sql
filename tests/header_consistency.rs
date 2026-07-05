//! Vérifie que le header `//!` `Fonctions :` de chaque fichier `.rs` de `src/`,
//! `json2sql-worker/src/` et `json2sql-ui/src/` reste synchronisé avec le code réel (issue #33).
//!
//! Ce fichier n'entre pas lui-même dans le périmètre de la convention (`tests/` est hors scope).

use std::fs;
use std::path::{Path, PathBuf};

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

/// True if the file's leading `//!` doc-comment block contains a `Fonctions :` marker line.
fn has_fonctions_block(content: &str) -> bool {
    content
        .lines()
        .take_while(|line| line.starts_with("//!"))
        .any(|line| line.contains("Fonctions"))
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
}
