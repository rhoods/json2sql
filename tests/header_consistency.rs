//! Vérifie que le bloc `//!` `Fonctions :` de chaque fichier `.rs` de `src/`,
//! `json2sql-worker/src/` et `json2sql-ui/src/` reste synchronisé avec le code réel (issue #33).
//!
//! Ce fichier n'entre pas lui-même dans le périmètre de la convention (`tests/` est hors scope).
//!
//! Format attendu (voir `docs/technical/modules.md`) : une entrée par ligne, taguée par nature,
//! nom entre backticks (requis par `clippy::doc_markdown` sous `#![deny(clippy::pedantic)]` du
//! crate `json2sql-ui`) — `` - fn `{nom}` ``, `` - fn `{Type}::{méthode}` ``, `` - struct `{Nom}` ``,
//! `` - enum `{Nom}` ``, suivie de ` — description`. Les libellés de section libres (non taggés)
//! entre le marqueur `Fonctions :` et les entrées sont ignorés par le parseur — ils ne servent
//! qu'à la lisibilité humaine des gros fichiers.

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

/// Returns the leading `//!` header's lines from the `Fonctions :` marker onward (comment
/// markers still attached), or `None` if the file has no such block.
fn fonctions_block_lines(content: &str) -> Option<Vec<&str>> {
    let header: Vec<&str> = content
        .lines()
        .take_while(|line| line.starts_with("//!"))
        .collect();
    let start = header.iter().position(|line| line.contains("Fonctions"))?;
    Some(header[start..].to_vec())
}

/// True if the file's leading `//!` doc-comment block contains a `Fonctions :` marker line.
fn has_fonctions_block(content: &str) -> bool {
    fonctions_block_lines(content).is_some()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Fn,
    Struct,
    Enum,
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::Fn => "fn",
            Kind::Struct => "struct",
            Kind::Enum => "enum",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Item {
    kind: Kind,
    name: String,
}

/// Parses the `Fonctions :` block: only lines shaped `` - fn `{name}` ``, `` - fn `{Type}::{method}` ``,
/// `` - struct `{Name}` ``, `` - enum `{Name}` `` are entries. Any other line (free-text section
/// labels, blank lines) is ignored — no prose parsing, no decoration stripping needed by construction.
fn documented_items(content: &str) -> HashSet<Item> {
    let mut items = HashSet::new();
    let Some(lines) = fonctions_block_lines(content) else {
        return items;
    };
    let entry_re = Regex::new(r"^-\s+(fn|struct|enum)\s+`([^`]+)`").unwrap();
    for line in lines {
        let stripped = line.trim_start_matches("//!").trim_start();
        if let Some(cap) = entry_re.captures(stripped) {
            let kind = match &cap[1] {
                "fn" => Kind::Fn,
                "struct" => Kind::Struct,
                "enum" => Kind::Enum,
                _ => unreachable!(),
            };
            items.insert(Item {
                kind,
                name: cap[2].to_string(),
            });
        }
    }
    items
}

/// If a raw string literal (`r"..."`, `r#"..."#`, `br"..."`, `br#"..."#`, any number of `#`)
/// starts at `i`, returns the index right after its closing delimiter. `#` inside a raw string
/// is not an escape character — closing requires a `"` followed by exactly as many `#` as the
/// opening had, which is why a plain in-string scan (treating `\` as an escape) misreads them.
fn skip_raw_string(bytes: &[u8], i: usize) -> Option<usize> {
    let r_pos = match bytes.get(i) {
        Some(b'r') => i,
        Some(b'b') if bytes.get(i + 1) == Some(&b'r') => i + 1,
        _ => return None,
    };
    let mut j = r_pos + 1;
    let mut hashes = 0usize;
    while bytes.get(j) == Some(&b'#') {
        hashes += 1;
        j += 1;
    }
    if bytes.get(j) != Some(&b'"') {
        return None; // 'r'/'br' was just the start of an identifier, not a raw string
    }
    let mut k = j + 1;
    loop {
        match bytes.get(k) {
            None => return None, // malformed / truncated raw string
            Some(b'"') => {
                let mut h = 0;
                while h < hashes && bytes.get(k + 1 + h) == Some(&b'#') {
                    h += 1;
                }
                if h == hashes {
                    return Some(k + 1 + hashes);
                }
                k += 1;
            }
            _ => k += 1,
        }
    }
}

/// Finds the index of the `}` matching the `{` at `open_pos`, tracking brace depth while
/// skipping braces that appear inside string, char, raw string, or `//`/`///`/`//!` line
/// comment content. A doc comment can legitimately say `` `{` `` or `'{'` in prose (e.g. "Open
/// `path`, consuming the opening `{` of the root wrapper object.") — none of that is real
/// syntax, so the whole comment is skipped wholesale rather than trying to special-case
/// backticks. Lifetimes (`'a`, `'static`, `'_`) are deliberately NOT treated as char literals —
/// only `'x'`/`'\x'`-shaped sequences are.
fn find_matching_brace(bytes: &[u8], open_pos: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut i = open_pos;
    let mut in_string = false;
    while i < bytes.len() {
        let c = bytes[i];
        if in_string {
            if c == b'\\' {
                i += 2;
                continue;
            }
            if c == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if c == b'/' && bytes.get(i + 1) == Some(&b'/') {
            i = bytes[i..]
                .iter()
                .position(|&b| b == b'\n')
                .map_or(bytes.len(), |rel| i + rel);
            continue;
        }
        if c == b'r' || c == b'b' {
            if let Some(next) = skip_raw_string(bytes, i) {
                i = next;
                continue;
            }
        }
        match c {
            b'"' => {
                in_string = true;
                i += 1;
            }
            b'\'' => {
                if bytes.get(i + 1) == Some(&b'\\') && bytes.get(i + 3) == Some(&b'\'') {
                    i += 4; // escaped char literal, e.g. '\n', '\'', '\\'
                } else if bytes.get(i + 1).is_some() && bytes.get(i + 2) == Some(&b'\'') {
                    i += 3; // simple char literal, e.g. '{', 'a'
                } else {
                    i += 1; // lifetime apostrophe ('a, 'static, '_) — not a literal
                }
            }
            b'{' => {
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    None
}

/// Removes every `#[cfg(test)] mod ... { ... }` block from `content` (brace-depth aware),
/// so downstream extraction never sees test-only functions. Only recognizes `#[cfg(test)]`
/// when it is the first token on its line — this is what tells a real attribute apart from
/// a prose mention inside a `///`/`//!` doc comment (e.g. "the bin target doesn't see
/// `#[cfg(test)]` call sites"), which never starts a line.
fn strip_test_modules(content: &str) -> String {
    let cfg_re = Regex::new(r"(?m)^[ \t]*#\[cfg\(test\)\]").unwrap();
    let mut result = content.to_string();
    while let Some(cfg_match) = cfg_re.find(&result) {
        let cfg_pos = cfg_match.start();
        let Some(mod_rel) = result[cfg_match.end()..].find("mod ") else {
            break;
        };
        let mod_pos = cfg_match.end() + mod_rel;
        let Some(brace_rel) = result[mod_pos..].find('{') else {
            break;
        };
        let open_pos = mod_pos + brace_rel;
        let Some(close_pos) = find_matching_brace(result.as_bytes(), open_pos) else {
            break;
        };
        result.replace_range(cfg_pos..=close_pos, "");
    }
    result
}

/// Parsed header of an `impl` block declaration: the receiver type name (verbatim, including
/// balanced `<...>` for concrete generic self-types like `Arc<Mutex<MemSink>>`) and the byte
/// offset (relative to the start of `after`) of the impl block's opening `{`.
struct ImplHeader {
    type_name: String,
    header_end: usize,
}

/// Parses the text right after `impl ` (already past the keyword and its following
/// whitespace) up to its opening `{`, tracking `<...>` depth so generics never get mistaken
/// for the header's own terminator. `impl Trait for Type` yields `Type` (never `Trait`) —
/// the trait name, if present, is simply skipped over once ` for ` is seen at depth 0.
fn parse_impl_header(after: &str) -> Option<ImplHeader> {
    let mut depth = 0i32;
    let mut type_start = 0usize;
    let mut type_end: Option<usize> = None;
    let chars: Vec<(usize, char)> = after.char_indices().collect();
    let mut idx = 0;
    while idx < chars.len() {
        let (byte_pos, c) = chars[idx];
        if depth == 0 && type_start == 0 && after[byte_pos..].starts_with(" for ") {
            type_start = byte_pos + 5;
            idx += 5;
            continue;
        }
        if depth == 0 && type_end.is_none() && after[byte_pos..].starts_with("where") {
            type_end = Some(byte_pos);
        }
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            '{' if depth == 0 => {
                let end = type_end.unwrap_or(byte_pos);
                let ty = after[type_start..end].trim().to_string();
                return if ty.is_empty() {
                    None
                } else {
                    Some(ImplHeader {
                        type_name: ty,
                        header_end: byte_pos,
                    })
                };
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

/// Extracts every real fn/struct/enum item defined in `content`. Free functions and structs/
/// enums keep their bare name; `impl` methods are qualified `Type::method` (an
/// `impl Trait for Type` block yields `Type::method`, never `Trait::method`, and a generic
/// self-type like `Arc<Mutex<MemSink>>` is kept verbatim). Items nested inside any
/// `#[cfg(test)]` module are excluded. `impl` blocks are only recognized when `impl` is the
/// first token on its line — this is what tells a real impl block apart from an opaque
/// return-position `-> impl Trait`, which never starts a line. A `fn` that is indented but not
/// inside a recognized `impl` span (a trait's method *signature*, or a `fn`/`struct`/`enum`
/// nested inside another function's body) is not a definition to document, mirroring the
/// existing closures/nested-fns rule — struct/enum are only ever real items at column 0.
fn extract_code_items(content: &str) -> HashSet<Item> {
    let stripped = strip_test_modules(content);
    let bytes = stripped.as_bytes();
    let mut items = HashSet::new();

    struct ImplSpan {
        open: usize,
        close: usize,
        type_name: String,
    }
    let impl_start_re = Regex::new(r"(?m)^[ \t]*impl[ \t]+").unwrap();
    let mut impl_spans: Vec<ImplSpan> = Vec::new();
    for m in impl_start_re.find_iter(&stripped) {
        let after = &stripped[m.end()..];
        let Some(header) = parse_impl_header(after) else {
            continue;
        };
        let open_pos = m.end() + header.header_end;
        let Some(close_pos) = find_matching_brace(bytes, open_pos) else {
            continue;
        };
        impl_spans.push(ImplSpan {
            open: open_pos,
            close: close_pos,
            type_name: header.type_name,
        });
    }

    let fn_re = Regex::new(
        r"(?m)^([ \t]*)(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)",
    )
    .unwrap();
    for cap in fn_re.captures_iter(&stripped) {
        let m = cap.get(0).unwrap();
        let indented = !cap[1].is_empty();
        let name = &cap[2];
        if let Some(span) = impl_spans
            .iter()
            .find(|s| m.start() > s.open && m.start() < s.close)
        {
            items.insert(Item {
                kind: Kind::Fn,
                name: format!("{}::{name}", span.type_name),
            });
        } else if !indented {
            // Column 0: a real top-level free function.
            items.insert(Item {
                kind: Kind::Fn,
                name: name.to_string(),
            });
        }
        // Indented but not inside a recognized impl block: a fn nested inside another fn's
        // body, or a trait method *signature* (no body) inside `trait X { fn y(...); }` —
        // neither is a definition to document (mirrors the existing closures/nested-fns rule).
    }

    // Struct/enum are only real file-level items at column 0 — anything indented is a local
    // type nested inside a function body (e.g. a one-off `#[derive(Serialize)] struct Report`
    // used only for a JSON payload), not a file-level responsibility to document.
    let struct_re =
        Regex::new(r"(?m)^(?:pub(?:\([^)]*\))?\s+)?struct\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    for cap in struct_re.captures_iter(&stripped) {
        items.insert(Item {
            kind: Kind::Struct,
            name: cap[1].to_string(),
        });
    }

    let enum_re =
        Regex::new(r"(?m)^(?:pub(?:\([^)]*\))?\s+)?enum\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    for cap in enum_re.captures_iter(&stripped) {
        items.insert(Item {
            kind: Kind::Enum,
            name: cap[1].to_string(),
        });
    }

    items
}

/// Two-direction inconsistencies found for a single file.
struct FileInconsistency {
    file: PathBuf,
    /// Documented but not found in the real code.
    doc_missing: Vec<Item>,
    /// Defined in the real code but not documented.
    code_missing: Vec<Item>,
}

/// Runs the full doc↔code set-diff for one file. Returns `None` if the file has no
/// `Fonctions :` block (out of scope) or if both directions are consistent.
fn check_file(path: &Path, content: &str) -> Option<FileInconsistency> {
    if !has_fonctions_block(content) {
        return None;
    }
    let documented = documented_items(content);
    let code = extract_code_items(content);

    let mut doc_missing: Vec<Item> = documented.difference(&code).cloned().collect();
    let mut code_missing: Vec<Item> = code.difference(&documented).cloned().collect();
    doc_missing.sort_by(|a, b| a.name.cmp(&b.name));
    code_missing.sort_by(|a, b| a.name.cmp(&b.name));

    if doc_missing.is_empty() && code_missing.is_empty() {
        None
    } else {
        Some(FileInconsistency {
            file: path.to_path_buf(),
            doc_missing,
            code_missing,
        })
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// The actual CI check (issue #33): every `.rs` file under `src/`, `json2sql-worker/src/` and
/// `json2sql-ui/src/` that carries a `//! Fonctions :` header must stay in sync with its real
/// code, in both directions. Aggregates every inconsistency across every file into a single
/// failure instead of stopping at the first one.
#[test]
fn header_fonctions_blocks_match_real_code() {
    let root = workspace_root();
    let target_dirs = [
        root.join("src"),
        root.join("json2sql-worker/src"),
        root.join("json2sql-ui/src"),
    ];
    let dir_refs: Vec<&Path> = target_dirs.iter().map(PathBuf::as_path).collect();

    let mut inconsistencies: Vec<FileInconsistency> = collect_rs_files(&dir_refs)
        .into_iter()
        .filter_map(|path| {
            let content = fs::read_to_string(&path).ok()?;
            check_file(&path, &content)
        })
        .collect();
    inconsistencies.sort_by(|a, b| a.file.cmp(&b.file));

    if inconsistencies.is_empty() {
        return;
    }

    let mut message = String::from("Header //! Fonctions désynchronisé du code réel :\n");
    for inc in &inconsistencies {
        let display_path = inc.file.strip_prefix(&root).unwrap_or(&inc.file).display();
        for item in &inc.doc_missing {
            message.push_str(&format!(
                "  {display_path}: documenté mais absent du code : {} `{}`\n",
                item.kind.label(),
                item.name
            ));
        }
        for item in &inc.code_missing {
            message.push_str(&format!(
                "  {display_path}: existe dans le code mais non documenté : {} `{}`\n",
                item.kind.label(),
                item.name
            ));
        }
    }
    panic!("{message}");
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

    fn item(kind: Kind, name: &str) -> Item {
        Item {
            kind,
            name: name.to_string(),
        }
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
        let content = "//! Responsabilité du fichier.\n//!\n//! Fonctions :\n//! - fn `foo` — fait un truc.\n\nfn foo() {}\n";
        assert!(has_fonctions_block(content));
    }

    #[test]
    fn ignores_header_without_fonctions_marker() {
        let content = "//! Sous-modules :\n//! - scoring — description.\n\npub mod scoring;\n";
        assert!(!has_fonctions_block(content));
    }

    #[test]
    fn ignores_file_with_no_header_at_all() {
        let content = "use std::fs;\n\nfn foo() {}\n";
        assert!(!has_fonctions_block(content));
    }

    #[test]
    fn ignores_fonctions_mentioned_outside_the_leading_header() {
        let content = "//! Responsabilité du fichier.\n\nuse std::fs;\n\n// Liste des Fonctions ici, mais hors du header.\nfn foo() {}\n";
        assert!(!has_fonctions_block(content));
    }

    #[test]
    fn parses_tagged_fn_entry() {
        let content = "//! Fonctions :\n//! - fn `foo` — fait un truc.\n";
        let items = documented_items(content);
        assert_eq!(items.len(), 1);
        assert!(items.contains(&item(Kind::Fn, "foo")));
    }

    #[test]
    fn parses_tagged_method_struct_and_enum_entries() {
        let content = "//! Fonctions :\n//! - fn `TableSchema::new` — crée un schéma vide.\n//! - struct `TableSchema` — schéma finalisé.\n//! - enum `ChildKind` — nature de la relation.\n";
        let items = documented_items(content);
        assert!(items.contains(&item(Kind::Fn, "TableSchema::new")));
        assert!(items.contains(&item(Kind::Struct, "TableSchema")));
        assert!(items.contains(&item(Kind::Enum, "ChildKind")));
    }

    #[test]
    fn free_text_section_labels_are_ignored_not_parsed_as_entries() {
        let content = "//! Fonctions :\n//! Orchestration :\n//! - fn `run` — pipeline complet.\n//! Flusher :\n//! - fn `run_flusher` — boucle principale.\n";
        let items = documented_items(content);
        assert_eq!(items.len(), 2);
        assert!(items.contains(&item(Kind::Fn, "run")));
        assert!(items.contains(&item(Kind::Fn, "run_flusher")));
    }

    #[test]
    fn extracts_free_functions_public_and_private() {
        let content = "pub fn foo() {}\nfn bar() {}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "foo")));
        assert!(items.contains(&item(Kind::Fn, "bar")));
    }

    #[test]
    fn excludes_functions_inside_cfg_test_module() {
        let content = "fn foo() {}\n\n#[cfg(test)]\nmod tests {\n    fn baz() {}\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "foo")));
        assert!(!items.contains(&item(Kind::Fn, "baz")));
    }

    #[test]
    fn cfg_test_mentioned_in_prose_is_not_mistaken_for_a_real_attribute() {
        // Real pattern from src/schema/table_schema.rs — a /// doc comment mentions the literal
        // text `#[cfg(test)]` mid-sentence, which must not be treated as a real attribute (it
        // never starts its own line, unlike the genuine `#[cfg(test)]` further down).
        let content = "impl Foo {\n    /// The bin target doesn't see `#[cfg(test)]` call sites, hence the `allow`.\n    fn bar() {}\n}\n\nimpl Baz {\n    fn quux() {}\n}\n\n#[cfg(test)]\nmod tests {\n    fn hidden() {}\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "Foo::bar")));
        assert!(items.contains(&item(Kind::Fn, "Baz::quux")));
        assert!(!items.contains(&item(Kind::Fn, "hidden")));
    }

    #[test]
    fn qualifies_impl_methods_as_type_method() {
        let content = "impl TableSchema {\n    fn new() -> Self { todo!() }\n    fn is_root(&self) -> bool { todo!() }\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "TableSchema::new")));
        assert!(items.contains(&item(Kind::Fn, "TableSchema::is_root")));
        assert!(!items.contains(&item(Kind::Fn, "new")));
    }

    #[test]
    fn trait_impl_method_qualifies_as_type_method_not_trait_method() {
        let content = "impl From<UserOverride> for InferredStrategy {\n    fn from(value: UserOverride) -> Self { todo!() }\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "InferredStrategy::from")));
        assert!(!items.contains(&item(Kind::Fn, "From::from")));
    }

    #[test]
    fn qualified_trait_path_does_not_leak_into_the_type_name() {
        // Real pattern from src/schema/table_schema.rs — impl std::fmt::Display for KeyShape.
        let content = "impl std::fmt::Display for KeyShape {\n    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { todo!() }\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "KeyShape::fmt")));
        assert!(!items.iter().any(|i| i.name.contains("std::fmt::Display")));
    }

    #[test]
    fn generic_self_type_is_kept_verbatim_with_balanced_brackets() {
        // Real pattern from src/pass2/sink.rs — impl RowSink for Arc<Mutex<MemSink>>.
        let content = "impl RowSink for Arc<Mutex<MemSink>> {\n    fn write_row(&mut self, row: &[u8]) -> Result<()> { todo!() }\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "Arc<Mutex<MemSink>>::write_row")));
    }

    #[test]
    fn opaque_return_position_impl_trait_is_not_mistaken_for_an_impl_block() {
        // Real pattern from src/schema/table_schema.rs — pub fn data_columns(&self) -> impl Iterator<Item = &ColumnSchema>
        // appearing INSIDE a real impl block; the opaque return type must not itself be
        // treated as a second impl block declaration (it never starts a line).
        let content = "impl TableSchema {\n    pub fn data_columns(&self) -> impl Iterator<Item = i32> {\n        std::iter::empty()\n    }\n    pub fn is_root(&self) -> bool { todo!() }\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "TableSchema::data_columns")));
        assert!(items.contains(&item(Kind::Fn, "TableSchema::is_root")));
        assert!(!items.iter().any(|i| i.name.starts_with("Iterator::")));
    }

    #[test]
    fn extracts_struct_and_enum_definitions() {
        let content = "pub struct TableSchema {\n    name: String,\n}\n\npub enum ChildKind {\n    Object,\n    ObjectArray,\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Struct, "TableSchema")));
        assert!(items.contains(&item(Kind::Enum, "ChildKind")));
    }

    #[test]
    fn brace_in_char_literal_does_not_break_impl_block_matching() {
        let content = "impl TableSchema {\n    fn new() -> Self {\n        let c = '{';\n        let d = '}';\n        todo!()\n    }\n    fn second() {}\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "TableSchema::new")));
        assert!(items.contains(&item(Kind::Fn, "TableSchema::second")));
    }

    #[test]
    fn brace_in_string_literal_does_not_break_impl_block_matching() {
        let content = "impl TableSchema {\n    fn new() -> Self {\n        let s = \"{ not a real brace\";\n        todo!()\n    }\n    fn second() {}\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "TableSchema::new")));
        assert!(items.contains(&item(Kind::Fn, "TableSchema::second")));
    }

    #[test]
    fn lifetime_parameter_is_not_misread_as_char_literal() {
        let content = "impl Observer {\n    fn observe_array_field<'s>(&self, x: &'s str) -> bool { todo!() }\n    fn second() {}\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "Observer::observe_array_field")));
        assert!(items.contains(&item(Kind::Fn, "Observer::second")));
    }

    #[test]
    fn raw_byte_string_with_embedded_quotes_does_not_break_brace_matching() {
        // Real pattern from src/io/reader.rs tests — br#"..."# containing quotes that are NOT
        // string-close characters (raw strings have no escapes). A plain in-string scan that
        // treats every `"` as a real toggle loses track of the real brace depth.
        let content = r##"impl Reader {
    fn parse(&self) -> bool {
        let json = br#"{"key": "value"}"#;
        json.len() > 0
    }
    fn second() {}
}
"##;
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "Reader::parse")));
        assert!(items.contains(&item(Kind::Fn, "Reader::second")));
    }

    #[test]
    fn brace_mentioned_in_a_doc_comment_does_not_break_impl_block_matching() {
        // Real pattern from src/io/reader.rs:704 — a /// doc comment says
        // "consuming the opening `{` of the root wrapper object.": a lone, unmatched brace
        // inside backticks in prose. Line comments must be skipped wholesale, not scanned for
        // real syntax, or this throws off the whole impl block's depth count.
        let content = "impl JsonRootWrapperReader {\n    /// Open `path`, consuming the opening `{` of the root wrapper object.\n    fn open(&self) -> bool { true }\n    fn second() {}\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "JsonRootWrapperReader::open")));
        assert!(items.contains(&item(Kind::Fn, "JsonRootWrapperReader::second")));
    }

    #[test]
    fn fn_nested_inside_another_functions_body_is_not_documented() {
        // Real pattern from json2sql-ui/src/screens/mod.rs — `dfs` is a local helper nested
        // inside `tree_display_order`, not a file-level item, even though it's indented.
        let content = "pub fn tree_display_order() -> Vec<usize> {\n    fn dfs(i: usize) -> usize {\n        i\n    }\n    dfs(0);\n    Vec::new()\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "tree_display_order")));
        assert!(!items.contains(&item(Kind::Fn, "dfs")));
    }

    #[test]
    fn trait_method_signature_without_body_is_not_documented() {
        // Real pattern from src/pass2/sink.rs — `trait RowSink { fn write_row(...); }` declares
        // the method signature only; each `impl RowSink for X` provides the real definition
        // (already documented separately), so the trait's own signature must not count.
        let content = "pub trait RowSink {\n    fn write_row(&mut self, row: &[u8]);\n}\n\nimpl RowSink for MemSink {\n    fn write_row(&mut self, row: &[u8]) { }\n}\n";
        let items = extract_code_items(content);
        assert!(items.contains(&item(Kind::Fn, "MemSink::write_row")));
        assert!(!items.contains(&item(Kind::Fn, "write_row")));
    }

    #[test]
    fn struct_or_enum_nested_inside_a_function_body_is_not_documented() {
        // Real pattern from src/anomaly/reporter.rs — a one-off `struct Report` used only to
        // shape a JSON payload inside `to_json`, and json2sql-ui's `enum ImportMsg` nested
        // inside a component function — neither is a file-level item.
        let content = "fn to_json() -> String {\n    struct Report {\n        n: u32,\n    }\n    enum ImportMsg { Cancel }\n    String::new()\n}\n";
        let items = extract_code_items(content);
        assert!(!items.contains(&item(Kind::Struct, "Report")));
        assert!(!items.contains(&item(Kind::Enum, "ImportMsg")));
    }

    #[test]
    fn consistent_file_yields_no_inconsistency() {
        let content = "//! Fonctions :\n//! - fn `foo` — fait un truc.\n\nfn foo() {}\n";
        assert!(check_file(Path::new("ok.rs"), content).is_none());
    }

    #[test]
    fn file_without_header_is_skipped_even_if_functions_are_undocumented() {
        let content = "fn foo() {}\nfn bar() {}\n";
        assert!(check_file(Path::new("no_header.rs"), content).is_none());
    }

    #[test]
    fn stale_doc_entry_is_reported_as_doc_missing() {
        let content = "//! Fonctions :\n//! - fn `ghost` — n'existe plus.\n//! - fn `foo` — fait un truc.\n\nfn foo() {}\n";
        let inc = check_file(Path::new("stale.rs"), content).unwrap();
        assert_eq!(inc.doc_missing, vec![item(Kind::Fn, "ghost")]);
        assert!(inc.code_missing.is_empty());
    }

    #[test]
    fn undocumented_real_function_is_reported_as_code_missing() {
        let content = "//! Fonctions :\n//! - fn `foo` — fait un truc.\n\nfn foo() {}\nfn bar() {}\n";
        let inc = check_file(Path::new("undocumented.rs"), content).unwrap();
        assert!(inc.doc_missing.is_empty());
        assert_eq!(inc.code_missing, vec![item(Kind::Fn, "bar")]);
    }

    #[test]
    fn struct_documented_but_absent_is_reported_distinctly_from_fn() {
        // A struct and a fn can legally share the same name in Rust (different namespaces) —
        // the kind tag must keep them distinct in the diff.
        let content = "//! Fonctions :\n//! - struct `Foo` — un type.\n\nfn Foo() {}\n";
        let inc = check_file(Path::new("kind_mismatch.rs"), content).unwrap();
        assert_eq!(inc.doc_missing, vec![item(Kind::Struct, "Foo")]);
        assert_eq!(inc.code_missing, vec![item(Kind::Fn, "Foo")]);
    }

    #[test]
    fn aggregation_across_files_collects_every_inconsistency_not_just_the_first() {
        let root = TempDir::new().unwrap();
        write_file(
            root.path(),
            "a.rs",
            "//! Fonctions :\n//! - fn `ghost_a` — stale.\n\nfn real_a() {}\n",
        );
        write_file(
            root.path(),
            "b.rs",
            "//! Fonctions :\n//! - fn `ghost_b` — stale.\n\nfn real_b() {}\n",
        );
        write_file(
            root.path(),
            "clean.rs",
            "//! Fonctions :\n//! - fn `ok` — ok.\n\nfn ok() {}\n",
        );

        let files = collect_rs_files(&[root.path()]);
        let inconsistencies: Vec<FileInconsistency> = files
            .into_iter()
            .filter_map(|path| {
                let content = fs::read_to_string(&path).unwrap();
                check_file(&path, &content)
            })
            .collect();

        assert_eq!(inconsistencies.len(), 2);
        let all_doc_missing: Vec<_> = inconsistencies
            .iter()
            .flat_map(|i| i.doc_missing.clone())
            .collect();
        assert!(all_doc_missing.contains(&item(Kind::Fn, "ghost_a")));
        assert!(all_doc_missing.contains(&item(Kind::Fn, "ghost_b")));
    }
}
