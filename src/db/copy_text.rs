/// A string guaranteed safe for PostgreSQL COPY text format.
///
/// No tab (`\t`), newline (`\n`), carriage return (`\r`), backslash (`\\`),
/// or null byte (`\0`) in unescaped form — all have been replaced by their
/// COPY escape sequences.
///
/// **Obtain only via:**
/// - [`escape_copy_text`] — for arbitrary user-controlled strings
/// - [`CopyEscaped::from_safe_ascii`] — for values whose ASCII-safety is a
///   compile-time invariant (generated integers, booleans, UUIDs, etc.)
///
/// The inner `String` is private — external code cannot bypass the invariant
/// by constructing `CopyEscaped` directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyEscaped(String);

impl CopyEscaped {
    /// Wrap a value that is known to contain no COPY-unsafe bytes.
    ///
    /// Panics in debug builds if `s` contains any of `\t \n \r \\ \0`.
    /// In release builds the field is still private, so the only way to
    /// obtain a `CopyEscaped` is through this constructor or `escape_copy_text`.
    pub fn from_safe_ascii(s: impl Into<String>) -> Self {
        let s = s.into();
        debug_assert!(
            !s.contains(|c| matches!(c, '\t' | '\n' | '\r' | '\\' | '\0')),
            "from_safe_ascii called with unsafe value: {:?}",
            s
        );
        CopyEscaped(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CopyEscaped {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Escape a string for PostgreSQL COPY text format.
///
/// Escapes `\t`, `\n`, `\r`, `\\`. Returns `None` if `s` contains a null
/// byte — PostgreSQL rejects null bytes in text columns and callers should
/// treat this as an anomaly (→ NULL) rather than silently stripping.
pub fn escape_copy_text(s: &str) -> Option<CopyEscaped> {
    // Fast-path: scan for any char that requires action before allocating.
    // Most values in practice contain none of these — avoid the allocation entirely.
    let first_special = s.find(|c| matches!(c, '\0' | '\\' | '\t' | '\n' | '\r'));
    let escape_start = match first_special {
        None => return Some(CopyEscaped(s.to_owned())), // nothing to escape
        Some(i) if s.as_bytes()[i] == b'\0' => return None, // null byte
        Some(i) => i,
    };

    // Slow-path: allocate and escape from the first special char onward.
    // +16: small headroom for a handful of escape expansions (\\ doubles one char).
    let mut out = String::with_capacity(s.len() + 16);
    out.push_str(&s[..escape_start]);
    for c in s[escape_start..].chars() {
        match c {
            '\0' => return None,
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    Some(CopyEscaped(out))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_copy_text() {
        assert_eq!(escape_copy_text("hello\tworld").unwrap().as_str(), "hello\\tworld");
        assert_eq!(escape_copy_text("line1\nline2").unwrap().as_str(), "line1\\nline2");
        assert_eq!(escape_copy_text("back\\slash").unwrap().as_str(), "back\\\\slash");
        assert_eq!(escape_copy_text("plain text").unwrap().as_str(), "plain text");
        // Fast-path null byte (first char)
        assert_eq!(escape_copy_text("\x00abc"), None);
        // Slow-path null byte (after a special char that triggers allocation)
        assert_eq!(escape_copy_text("hello\\\x00world"), None);
        // Slow-path null byte (after clean prefix, no prior special char — fast-path finds \0 first)
        assert_eq!(escape_copy_text("abc\x00def"), None);
    }

    #[test]
    fn test_from_safe_ascii_roundtrip() {
        // Accepts &str without intermediate allocation
        let s = CopyEscaped::from_safe_ascii("42");
        assert_eq!(s.as_str(), "42");
        // Also accepts String
        let s2 = CopyEscaped::from_safe_ascii("ok".to_string());
        assert_eq!(s2.as_str(), "ok");
    }
}
