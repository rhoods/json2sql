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

impl std::ops::Deref for CopyEscaped {
    type Target = str;
    fn deref(&self) -> &str {
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
        assert_eq!(escape_copy_text("hello\tworld").as_deref(), Some("hello\\tworld"));
        assert_eq!(escape_copy_text("line1\nline2").as_deref(), Some("line1\\nline2"));
        assert_eq!(escape_copy_text("back\\slash").as_deref(), Some("back\\\\slash"));
        assert_eq!(escape_copy_text("null\x00byte"), None);
        assert_eq!(escape_copy_text("plain text").as_deref(), Some("plain text"));
    }

    #[test]
    fn test_from_safe_ascii_roundtrip() {
        let s = CopyEscaped::from_safe_ascii("42".to_string());
        assert_eq!(s.as_str(), "42");
    }
}
