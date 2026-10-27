//! Extraction des définitions Rust (fn, struct, enum, const, trait) d'un fichier source, partagée par
//! `tests/header_consistency.rs` (#33) et `tests/guides_consistency.rs` (#96). Inclus par
//! `#[path = "common/code_items.rs"] mod code_items;` plutôt que via `tests/common/mod.rs`, que
//! compilent les tests PostgreSQL.
//!
//! Lecture par regex et comptage d'accolades, pas d'analyse syntaxique : les règles de ce qui
//! compte comme définition sont sur `extract_code_items`.
#![allow(dead_code)] // chaque test n'utilise qu'une partie du module

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use regex::Regex;

/// Recursively collects all `.rs` files under the given root directories.
pub fn collect_rs_files(roots: &[&Path]) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for root in roots {
        walk(root, &mut files);
    }
    files
}

pub fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Fn,
    Struct,
    Enum,
    Const,
    Trait,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::Fn => "fn",
            Kind::Struct => "struct",
            Kind::Enum => "enum",
            Kind::Const => "const",
            Kind::Trait => "trait",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Item {
    pub kind: Kind,
    pub name: String,
}

/// If a raw string literal (`r"..."`, `r#"..."#`, `br"..."`, `br#"..."#`, any number of `#`)
/// starts at `i`, returns the index right after its closing delimiter. `#` inside a raw string
/// is not an escape character — closing requires a `"` followed by exactly as many `#` as the
/// opening had, which is why a plain in-string scan (treating `\` as an escape) misreads them.
pub fn skip_raw_string(bytes: &[u8], i: usize) -> Option<usize> {
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
pub fn find_matching_brace(bytes: &[u8], open_pos: usize) -> Option<usize> {
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
pub fn strip_test_modules(content: &str) -> String {
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
pub struct ImplHeader {
    pub type_name: String,
    pub header_end: usize,
}

/// Parses the text right after `impl ` (already past the keyword and its following
/// whitespace) up to its opening `{`, tracking `<...>` depth so generics never get mistaken
/// for the header's own terminator. `impl Trait for Type` yields `Type` (never `Trait`) —
/// the trait name, if present, is simply skipped over once ` for ` is seen at depth 0.
pub fn parse_impl_header(after: &str) -> Option<ImplHeader> {
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

/// Extracts every real fn/struct/enum/const/trait item defined in `content` (consts follow the fn
/// rules, traits the struct/enum ones). Free functions and structs/
/// enums keep their bare name; `impl` methods are qualified `Type::method` (an
/// `impl Trait for Type` block yields `Type::method`, never `Trait::method`, and a generic
/// self-type like `Arc<Mutex<MemSink>>` is kept verbatim). Items nested inside any
/// `#[cfg(test)]` module are excluded. `impl` blocks are only recognized when `impl` is the
/// first token on its line — this is what tells a real impl block apart from an opaque
/// return-position `-> impl Trait`, which never starts a line. A `fn` that is indented but not
/// inside a recognized `impl` span (a trait's method *signature*, or a `fn`/`struct`/`enum`
/// nested inside another function's body) is not a definition to document, mirroring the
/// existing closures/nested-fns rule — struct/enum are only ever real items at column 0.
pub fn extract_code_items(content: &str) -> HashSet<Item> {
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
    // `const NAME:` — the `:` keeps `const fn` out (already matched above as a fn).
    let const_re =
        Regex::new(r"(?m)^([ \t]*)(?:pub(?:\([^)]*\))?\s+)?const\s+([A-Za-z_][A-Za-z0-9_]*)\s*:")
            .unwrap();
    for (re, kind) in [(&fn_re, Kind::Fn), (&const_re, Kind::Const)] {
        for cap in re.captures_iter(&stripped) {
            let m = cap.get(0).unwrap();
            let indented = !cap[1].is_empty();
            let name = &cap[2];
            if let Some(span) = impl_spans
                .iter()
                .find(|s| m.start() > s.open && m.start() < s.close)
            {
                items.insert(Item {
                    kind,
                    name: format!("{}::{name}", span.type_name),
                });
            } else if !indented {
                // Column 0: a real top-level free function or constant.
                items.insert(Item {
                    kind,
                    name: name.to_string(),
                });
            }
            // Indented but not inside a recognized impl block: a fn nested inside another fn's
            // body, or a trait method *signature* (no body) inside `trait X { fn y(...); }` —
            // neither is a definition to document (mirrors the existing closures/nested-fns rule).
        }
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
    let trait_re =
        Regex::new(r"(?m)^(?:pub(?:\([^)]*\))?\s+)?trait\s+([A-Za-z_][A-Za-z0-9_]*)").unwrap();
    for (re, kind) in [(&enum_re, Kind::Enum), (&trait_re, Kind::Trait)] {
        for cap in re.captures_iter(&stripped) {
            items.insert(Item {
                kind,
                name: cap[1].to_string(),
            });
        }
    }

    items
}
