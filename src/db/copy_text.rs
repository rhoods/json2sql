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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyEscaped(pub(crate) String);

impl CopyEscaped {
    /// Wrap a value that is known to contain no COPY-unsafe bytes.
    ///
    /// # Safety (logical, not `unsafe`)
    /// Caller guarantees `s` contains none of `\t \n \r \\ \0`.
    /// A `debug_assert!` enforces this in debug builds.
    pub fn from_safe_ascii(s: String) -> Self {
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
    if s.contains('\0') {
        return None;
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
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
        assert_eq!(escape_copy_text("hello\tworld").map(|e| e.0), Some("hello\\tworld".to_string()));
        assert_eq!(escape_copy_text("line1\nline2").map(|e| e.0), Some("line1\\nline2".to_string()));
        assert_eq!(escape_copy_text("back\\slash").map(|e| e.0), Some("back\\\\slash".to_string()));
        assert_eq!(escape_copy_text("null\x00byte"), None);
        assert_eq!(escape_copy_text("plain text").map(|e| e.0), Some("plain text".to_string()));
    }

    #[test]
    fn test_from_safe_ascii_roundtrip() {
        let s = CopyEscaped::from_safe_ascii("42".to_string());
        assert_eq!(s.as_str(), "42");
    }
}
