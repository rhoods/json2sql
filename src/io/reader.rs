//! JSON file streaming: auto-detects format (array, NDJSON, or root wrapper object),
//! iterates top-level objects, and reports byte-level progress.
//!
//! [`JsonReader`] is the primary iterator; it parses via `simd_json` for performance.
//! [`JsonFormat`] is detected by scanning the first object of the file:
//! - `[` → [`JsonFormat::Array`]
//! - `{` whose root-level values are all arrays, no second root object → [`JsonFormat::RootWrapper`]
//! - `{` with a second root object following → [`JsonFormat::Lines`] (NDJSON)

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde_json::Value;

use crate::error::{J2sError, Result};

/// Map a simd-json parse error to `J2sError`.
#[allow(clippy::needless_pass_by_value)] // map_err(simd_err) requires fn(Error) -> J2sError signature
fn simd_err(e: simd_json::Error) -> J2sError {
    J2sError::InvalidInput(format!("JSON parse error: {e}"))
}

/// Detected format of the input file.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum JsonFormat {
    /// Top-level JSON array: `[{...}, {...}, ...]`
    Array,
    /// One JSON object per line (JSON-Lines / NDJSON)
    Lines,
    /// Single root object whose values are all arrays: `{"K1": [{...}], "K2": [{...}]}`
    /// The `Vec<String>` holds the key names in order of discovery.
    RootWrapper(Vec<String>),
}

/// Detect the format by scanning the first root-level structure of the file.
pub fn detect_format(path: &Path) -> Result<JsonFormat> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(512 * 1024, file);
    match fmt_skip_ws(&mut reader)? {
        None => Err(J2sError::InvalidInput("File appears to be empty".to_string())),
        Some(b'[') => Ok(JsonFormat::Array),
        Some(b'{') => fmt_detect_root_object(&mut reader),
        Some(other) => Err(J2sError::InvalidInput(format!(
            "Expected '[' or '{{' as first character, found '{}'",
            other as char
        ))),
    }
}

// ---------------------------------------------------------------------------
// Format-detection helpers (fmt_ prefix — only used by detect_format)
// ---------------------------------------------------------------------------

fn fmt_read_one(reader: &mut impl BufRead) -> Result<Option<u8>> {
    let mut b = [0u8; 1];
    match reader.read_exact(&mut b) {
        Ok(()) => Ok(Some(b[0])),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(J2sError::Io(e)),
    }
}

/// Skip whitespace; return next non-whitespace byte, or `None` at EOF.
fn fmt_skip_ws(reader: &mut impl BufRead) -> Result<Option<u8>> {
    loop {
        match fmt_read_one(reader)? {
            None => return Ok(None),
            Some(b) if b.is_ascii_whitespace() => {}
            Some(b) => return Ok(Some(b)),
        }
    }
}

/// Skip whitespace and commas; return next other byte, or `None` at EOF.
fn fmt_skip_ws_comma(reader: &mut impl BufRead) -> Result<Option<u8>> {
    loop {
        match fmt_read_one(reader)? {
            None => return Ok(None),
            Some(b',') | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r') => {}
            Some(b) => return Ok(Some(b)),
        }
    }
}

/// Read a JSON string until the closing `"` (opening `"` already consumed).
fn fmt_read_string(reader: &mut impl BufRead) -> Result<String> {
    let mut s = Vec::new();
    let mut escape_next = false;
    loop {
        match fmt_read_one(reader)? {
            None => return Err(J2sError::InvalidInput("Unexpected EOF inside JSON string".into())),
            Some(b) => {
                if escape_next {
                    s.push(b);
                    escape_next = false;
                } else if b == b'\\' {
                    s.push(b);
                    escape_next = true;
                } else if b == b'"' {
                    break;
                } else {
                    s.push(b);
                }
            }
        }
    }
    String::from_utf8(s).map_err(|e| J2sError::InvalidInput(format!("Non-UTF-8 JSON key: {e}")))
}

/// Skip a complete JSON value whose first byte was `first_byte` (already consumed).
fn fmt_skip_value(reader: &mut impl BufRead, first_byte: u8) -> Result<()> {
    match first_byte {
        b'{' => fmt_skip_container(reader, b'}'),
        b'[' => fmt_skip_container(reader, b']'),
        b'"' => fmt_skip_string_rest(reader),
        _ => fmt_skip_primitive_rest(reader),
    }
}

/// Scan a byte chunk for JSON structure boundaries, accumulating bytes into `buf`.
///
/// Returns `(bytes_consumed, Some(b))` when depth reaches 0 (b is the closing bracket),
/// or `(chunk.len(), None)` if the closer was not found in this chunk.
/// The caller must check `chunk.is_empty()` before calling and handle EOF.
/// The check `b == closer` and the mismatched-bracket error are the caller's responsibility.
fn scan_chunk_into(
    chunk: &[u8],
    buf: &mut Vec<u8>,
    depth: &mut u32,
    in_string: &mut bool,
    escape_next: &mut bool,
) -> (usize, Option<u8>) {
    for (i, &b) in chunk.iter().enumerate() {
        if *escape_next {
            *escape_next = false;
            buf.push(b);
            continue;
        }
        if *in_string {
            buf.push(b);
            match b {
                b'\\' => *escape_next = true,
                b'"' => *in_string = false,
                _ => {}
            }
        } else {
            match b {
                b'"' => { *in_string = true; buf.push(b); }
                b'{' | b'[' => { *depth += 1; buf.push(b); }
                b'}' | b']' => {
                    *depth -= 1;
                    buf.push(b);
                    if *depth == 0 {
                        return (i + 1, Some(b));
                    }
                }
                _ => { buf.push(b); }
            }
        }
    }
    (chunk.len(), None)
}

/// Skip until the matching `closer` (`}` or `]`), opening brace already consumed.
/// Reads in buffer-sized chunks via `fill_buf`/`consume` for large-file performance.
fn fmt_skip_container(reader: &mut impl BufRead, closer: u8) -> Result<()> {
    let mut scratch = Vec::new();
    let mut depth: u32 = 1;
    let mut in_string = false;
    let mut escape_next = false;
    loop {
        let (n, found) = {
            let buf = reader.fill_buf().map_err(J2sError::Io)?;
            if buf.is_empty() {
                return Err(J2sError::InvalidInput("Unexpected EOF inside JSON container".into()));
            }
            scratch.clear();
            scan_chunk_into(buf, &mut scratch, &mut depth, &mut in_string, &mut escape_next)
        }; // buf borrow ends before consume
        reader.consume(n);
        if let Some(b) = found {
            return if b == closer {
                Ok(())
            } else {
                Err(J2sError::InvalidInput(format!(
                    "Mismatched bracket: expected '{}', got '{}'",
                    closer as char, b as char
                )))
            };
        }
    }
}

/// Skip the rest of a JSON string (opening `"` already consumed).
fn fmt_skip_string_rest(reader: &mut impl BufRead) -> Result<()> {
    let mut escape_next = false;
    loop {
        match fmt_read_one(reader)? {
            None => return Err(J2sError::InvalidInput("Unexpected EOF inside JSON string".into())),
            Some(b) => {
                if escape_next {
                    escape_next = false;
                } else {
                    match b {
                        b'\\' => escape_next = true,
                        b'"' => return Ok(()),
                        _ => {}
                    }
                }
            }
        }
    }
}

/// Skip a primitive value (number, bool, null — first byte already consumed).
fn fmt_skip_primitive_rest(reader: &mut impl BufRead) -> Result<()> {
    loop {
        let buf = reader.fill_buf().map_err(J2sError::Io)?;
        if buf.is_empty() {
            break;
        }
        match buf[0] {
            b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r' => break,
            _ => {
                reader.consume(1);
            }
        }
    }
    Ok(())
}

/// Determine whether a `{`-started file is NDJSON or a root wrapper object.
///
/// Called after the opening `{` has been consumed.
///
/// - If another root object follows → [`JsonFormat::Lines`] (NDJSON).
/// - If all top-level values are arrays → [`JsonFormat::RootWrapper`].
/// - If top-level values are mixed (some arrays, some not) → error.
/// - If no arrays at all (single plain object) → [`JsonFormat::Lines`] (backward compat).
fn fmt_detect_root_object(reader: &mut impl BufRead) -> Result<JsonFormat> {
    let mut array_keys: Vec<String> = Vec::new();
    let mut has_non_array = false;
    let mut first_non_array_key: Option<String> = None;

    loop {
        match fmt_skip_ws_comma(reader)? {
            None => {
                return Err(J2sError::InvalidInput(
                    "Unexpected EOF inside root JSON object".into(),
                ))
            }
            Some(b'"') => {
                let key = fmt_read_string(reader)?;

                // Expect ':'
                match fmt_skip_ws(reader)? {
                    Some(b':') => {}
                    Some(b) => {
                        return Err(J2sError::InvalidInput(format!(
                            "Expected ':' after key '{key}', found '{}'",
                            b as char
                        )))
                    }
                    None => {
                        return Err(J2sError::InvalidInput(format!(
                            "Unexpected EOF after key '{key}'"
                        )))
                    }
                }

                match fmt_skip_ws(reader)? {
                    None => {
                        return Err(J2sError::InvalidInput(format!(
                            "Unexpected EOF after ':' for key '{key}'"
                        )))
                    }
                    Some(b'[') => {
                        array_keys.push(key);
                        fmt_skip_container(reader, b']')?;
                    }
                    Some(first) => {
                        if first_non_array_key.is_none() {
                            first_non_array_key = Some(key);
                        }
                        has_non_array = true;
                        fmt_skip_value(reader, first)?;
                    }
                }
            }
            Some(b'}') => {
                // Root object closed. Peek for NDJSON marker (any non-whitespace follows).
                return match fmt_skip_ws(reader)? {
                    Some(_) => Ok(JsonFormat::Lines),
                    None => {
                        if has_non_array && !array_keys.is_empty() {
                            Err(J2sError::InvalidInput(format!(
                                "Root wrapper object contains non-array value for key '{}'. \
                                 Only root objects where all values are arrays are supported.",
                                first_non_array_key.as_deref().unwrap_or("?")
                            )))
                        } else if !array_keys.is_empty() {
                            Ok(JsonFormat::RootWrapper(array_keys))
                        } else {
                            // Single root object with no array values — treat as NDJSON (single row).
                            Ok(JsonFormat::Lines)
                        }
                    }
                };
            }
            Some(other) => {
                return Err(J2sError::InvalidInput(format!(
                    "Unexpected character '{}' while scanning root JSON object",
                    other as char
                )));
            }
        }
    }
}

/// Returns the file size in bytes (for progress bar).
pub fn file_size(path: &Path) -> Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

// ---------------------------------------------------------------------------
// JSON-Lines iterator
// ---------------------------------------------------------------------------

/// Streaming iterator over JSON-Lines (NDJSON) files.
pub struct JsonLinesReader {
    reader: BufReader<File>,
    line_buf: Vec<u8>,
    bytes_read: u64,
}

impl JsonLinesReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::with_capacity(512 * 1024, file),
            line_buf: Vec::with_capacity(4096),
            bytes_read: 0,
        })
    }

    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl JsonLinesReader {
    /// Read the next non-blank line into `self.line_buf`, returning `(start, end)` of the
    /// trimmed content. Skips blank lines. Returns `None` on EOF, `Err` on I/O error.
    fn fill_next_line(&mut self) -> Option<Result<(usize, usize)>> {
        loop {
            self.line_buf.clear();
            match self.reader.read_until(b'\n', &mut self.line_buf) {
                Ok(0) => return None,
                Ok(n) => {
                    self.bytes_read += n as u64;
                    let start = self.line_buf.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(self.line_buf.len());
                    let end = self.line_buf.iter().rposition(|b| !b.is_ascii_whitespace()).map_or(0, |i| i + 1);
                    if start >= end { continue; }
                    return Some(Ok((start, end)));
                }
                Err(e) => return Some(Err(J2sError::Io(e))),
            }
        }
    }

    /// Return the raw bytes of the next JSON object without parsing.
    pub fn next_raw(&mut self) -> Option<Result<Vec<u8>>> {
        match self.fill_next_line()? {
            Ok((start, end)) => Some(Ok(self.line_buf[start..end].to_vec())),
            Err(e) => Some(Err(e)),
        }
    }
}

impl Iterator for JsonLinesReader {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.fill_next_line()? {
            Ok((start, end)) => {
                let slice = &mut self.line_buf[start..end];
                Some(simd_json::from_slice(slice).map_err(simd_err))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// JSON Array iterator — streaming, no full load
// ---------------------------------------------------------------------------

/// Streaming iterator over a top-level JSON array `[{...}, {...}]`.
/// Reads one element at a time using a mini depth-tracking tokenizer.
pub struct JsonArrayReader {
    reader: BufReader<File>,
    bytes_read: u64,
    opened: bool, // have we consumed the opening `[`?
    done: bool,
    buf: Vec<u8>, // reusable scratch buffer
}

impl JsonArrayReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::with_capacity(512 * 1024, file),
            bytes_read: 0,
            opened: false,
            done: false,
            buf: Vec::with_capacity(4096),
        })
    }

    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Read exactly one byte. Returns None on EOF.
    fn read_byte(&mut self) -> Option<std::io::Result<u8>> {
        let mut b = [0u8; 1];
        match self.reader.read_exact(&mut b) {
            Ok(()) => {
                self.bytes_read += 1;
                Some(Ok(b[0]))
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => None,
            Err(e) => Some(Err(e)),
        }
    }

    /// Skip whitespace and commas at the top level until we find the
    /// first byte of the next value (or `]` for end-of-array).
    /// Returns the first significant byte, or None on EOF.
    fn skip_to_next_value(&mut self) -> Option<Result<u8>> {
        loop {
            match self.read_byte()? {
                Err(e) => return Some(Err(J2sError::Io(e))),
                Ok(b) => match b {
                    b' ' | b'\t' | b'\n' | b'\r' | b',' => {}
                    b']' => return None, // end of array
                    other => return Some(Ok(other)),
                },
            }
        }
    }

    /// Collect a complete JSON value starting with `first_byte` into `self.buf`.
    fn collect_value(&mut self, first_byte: u8) -> Result<()> {
        self.buf.clear();
        self.buf.push(first_byte);
        match first_byte {
            b'{' => self.collect_container(b'}'),
            b'[' => self.collect_container(b']'),
            b'"' => self.collect_string(),
            _ => self.collect_primitive(),
        }
    }

    /// Collect the rest of a `{...}` or `[...]` container (opener already in buf).
    fn collect_container(&mut self, closer: u8) -> Result<()> {
        let mut depth: u32 = 1;
        let mut in_string = false;
        let mut escape_next = false;
        loop {
            let (n, found) = {
                let chunk = self.reader.fill_buf().map_err(J2sError::Io)?;
                if chunk.is_empty() {
                    return Err(J2sError::InvalidInput("Unexpected EOF inside JSON value".into()));
                }
                scan_chunk_into(chunk, &mut self.buf, &mut depth, &mut in_string, &mut escape_next)
            }; // chunk borrow ends before consume
            self.reader.consume(n);
            self.bytes_read += n as u64;
            if let Some(b) = found {
                return if b == closer {
                    Ok(())
                } else {
                    Err(J2sError::InvalidInput(format!(
                        "Mismatched bracket: expected '{}', got '{}'",
                        closer as char, b as char
                    )))
                };
            }
        }
    }

    /// Collect the rest of a `"..."` string (opening `"` already in buf).
    fn collect_string(&mut self) -> Result<()> {
        let mut escape_next = false;
        loop {
            let b = match self.read_byte() {
                None => return Err(J2sError::InvalidInput("Unexpected EOF inside JSON string".to_string())),
                Some(Err(e)) => return Err(J2sError::Io(e)),
                Some(Ok(b)) => b,
            };
            self.buf.push(b);
            if escape_next {
                escape_next = false;
            } else {
                match b {
                    b'\\' => escape_next = true,
                    b'"' => return Ok(()),
                    _ => {}
                }
            }
        }
    }

    /// Collect a primitive (number, bool, null) — read until a delimiter.
    fn collect_primitive(&mut self) -> Result<()> {
        loop {
            // Peek at next byte using fill_buf
            let next = {
                let buf = match self.reader.fill_buf() {
                    Ok(b) => b,
                    Err(e) => return Err(J2sError::Io(e)),
                };
                if buf.is_empty() {
                    break; // EOF — value is complete
                }
                buf[0]
            };
            match next {
                b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r' => break,
                b => {
                    self.buf.push(b);
                    self.reader.consume(1);
                    self.bytes_read += 1;
                }
            }
        }
        Ok(())
    }
}

impl JsonArrayReader {
    /// Return the raw bytes of the next JSON object without parsing.
    pub fn next_raw(&mut self) -> Option<Result<Vec<u8>>> {
        if self.done { return None; }

        if !self.opened {
            loop {
                match self.read_byte()? {
                    Err(e) => return Some(Err(J2sError::Io(e))),
                    Ok(b'[') => { self.opened = true; break; }
                    Ok(b) if b.is_ascii_whitespace() => {}
                    Ok(b) => return Some(Err(J2sError::InvalidInput(format!("Expected '[', found '{}'", b as char)))),
                }
            }
        }

        let first_byte = match self.skip_to_next_value()? {
            Err(e) => return Some(Err(e)),
            Ok(b) => b,
        };

        if let Err(e) = self.collect_value(first_byte) {
            return Some(Err(e));
        }

        Some(Ok(self.buf.clone()))
    }
}

impl Iterator for JsonArrayReader {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        // On first call: skip to the opening `[`
        if !self.opened {
            loop {
                match self.read_byte()? {
                    Err(e) => return Some(Err(J2sError::Io(e))),
                    Ok(b'[') => {
                        self.opened = true;
                        break;
                    }
                    Ok(b) if b.is_ascii_whitespace() => {}
                    Ok(b) => {
                        return Some(Err(J2sError::InvalidInput(format!(
                            "Expected '[', found '{}'",
                            b as char
                        ))))
                    }
                }
            }
        }

        // Find the first byte of the next element
        let first_byte = match self.skip_to_next_value()? {
            Err(e) => return Some(Err(e)),
            Ok(b) => b,
        };

        // Collect the complete JSON value
        if let Err(e) = self.collect_value(first_byte) {
            return Some(Err(e));
        }

        // Parse
        Some(simd_json::from_slice(&mut self.buf).map_err(simd_err))
    }
}

// ---------------------------------------------------------------------------
// Root-wrapper object iterator — streams elements across N named arrays
// ---------------------------------------------------------------------------

/// Streaming iterator over a root wrapper object: `{"K1": [{...}], "K2": [{...}]}`.
///
/// Each named array is streamed in order. `current_key()` identifies which key is active.
/// `bytes_read()` is cumulative across keys (never resets between arrays).
pub struct JsonRootWrapperReader {
    reader: BufReader<File>,
    keys: Vec<String>,
    current_key_idx: usize,
    bytes_read: u64,
    in_array: bool,
    done: bool,
    buf: Vec<u8>,
}

impl JsonRootWrapperReader {
    /// Open `path`, consuming the opening `{` of the root wrapper object.
    /// `keys` must be the key names in file order (as returned by [`detect_format`]).
    pub fn open(path: &Path, keys: Vec<String>) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(512 * 1024, file);
        let mut bytes_read = 0u64;
        // Skip to opening '{'
        loop {
            let mut b = [0u8; 1];
            match reader.read_exact(&mut b) {
                Ok(()) => {
                    bytes_read += 1;
                    if b[0].is_ascii_whitespace() {
                        continue;
                    }
                    if b[0] != b'{' {
                        return Err(J2sError::InvalidInput(format!(
                            "Expected '{{' as first character, found '{}'",
                            b[0] as char
                        )));
                    }
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(J2sError::InvalidInput("File appears to be empty".into()));
                }
                Err(e) => return Err(J2sError::Io(e)),
            }
        }
        Ok(Self {
            reader,
            keys,
            current_key_idx: 0,
            bytes_read,
            in_array: false,
            done: false,
            buf: Vec::with_capacity(4096),
        })
    }

    /// The key whose array is currently being streamed, or `None` when exhausted.
    #[must_use]
    pub fn current_key(&self) -> Option<&str> {
        if self.done {
            None
        } else {
            self.keys.get(self.current_key_idx).map(String::as_str)
        }
    }

    /// Total bytes consumed from the file since open (cumulative across keys).
    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Return the raw bytes of the next JSON element across all wrapper arrays.
    pub fn next_raw(&mut self) -> Option<Result<Vec<u8>>> {
        loop {
            if self.done {
                return None;
            }
            if !self.in_array {
                if self.current_key_idx >= self.keys.len() {
                    self.done = true;
                    return None;
                }
                if let Err(e) = self.advance_to_next_array() {
                    return Some(Err(e));
                }
                self.in_array = true;
            }
            let first_byte = match self.skip_to_next_value() {
                None => {
                    // Current array exhausted — advance to next key
                    self.in_array = false;
                    self.current_key_idx += 1;
                    continue;
                }
                Some(Err(e)) => return Some(Err(e)),
                Some(Ok(b)) => b,
            };
            if let Err(e) = self.collect_value(first_byte) {
                return Some(Err(e));
            }
            return Some(Ok(self.buf.clone()));
        }
    }

    // -- internal helpers --

    fn read_byte_tracked(&mut self) -> Result<Option<u8>> {
        let mut b = [0u8; 1];
        match self.reader.read_exact(&mut b) {
            Ok(()) => {
                self.bytes_read += 1;
                Ok(Some(b[0]))
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(J2sError::Io(e)),
        }
    }

    /// Advance the reader to the `[` of the next wrapper key's array.
    fn advance_to_next_array(&mut self) -> Result<()> {
        // Skip whitespace/commas to '"' (key start)
        loop {
            match self.read_byte_tracked()? {
                None => {
                    return Err(J2sError::InvalidInput(
                        "Unexpected EOF seeking next wrapper key".into(),
                    ))
                }
                Some(b) if b.is_ascii_whitespace() || b == b',' => {}
                Some(b'"') => break,
                Some(b'}') => {
                    return Err(J2sError::InvalidInput(
                        "Root object closed before all expected keys were found".into(),
                    ))
                }
                Some(b) => {
                    return Err(J2sError::InvalidInput(format!(
                        "Expected key '\"', found '{}' in wrapper object",
                        b as char
                    )))
                }
            }
        }
        // Skip key string (opening '"' already consumed above)
        self.skip_string_rest()?;
        // Skip whitespace to ':'
        loop {
            match self.read_byte_tracked()? {
                None => return Err(J2sError::InvalidInput("Unexpected EOF after wrapper key".into())),
                Some(b) if b.is_ascii_whitespace() => {}
                Some(b':') => break,
                Some(b) => {
                    return Err(J2sError::InvalidInput(format!(
                        "Expected ':' after wrapper key, found '{}'",
                        b as char
                    )))
                }
            }
        }
        // Skip whitespace to '['
        loop {
            match self.read_byte_tracked()? {
                None => return Err(J2sError::InvalidInput("Unexpected EOF before array start".into())),
                Some(b) if b.is_ascii_whitespace() => {}
                Some(b'[') => return Ok(()),
                Some(b) => {
                    return Err(J2sError::InvalidInput(format!(
                        "Expected '[' for wrapper key value, found '{}'",
                        b as char
                    )))
                }
            }
        }
    }

    /// Skip whitespace/commas inside an array; return next value byte or `None` on `]`.
    fn skip_to_next_value(&mut self) -> Option<Result<u8>> {
        loop {
            match self.read_byte_tracked() {
                Err(e) => return Some(Err(e)),
                Ok(None) => {
                    return Some(Err(J2sError::InvalidInput(
                        "Unexpected EOF inside wrapper array".into(),
                    )))
                }
                Ok(Some(b)) => match b {
                    b' ' | b'\t' | b'\n' | b'\r' | b',' => {}
                    b']' => return None,
                    other => return Some(Ok(other)),
                },
            }
        }
    }

    fn collect_value(&mut self, first_byte: u8) -> Result<()> {
        self.buf.clear();
        self.buf.push(first_byte);
        match first_byte {
            b'{' => self.collect_container(b'}'),
            b'[' => self.collect_container(b']'),
            b'"' => self.collect_string(),
            _ => self.collect_primitive(),
        }
    }

    fn collect_container(&mut self, closer: u8) -> Result<()> {
        let mut depth: u32 = 1;
        let mut in_string = false;
        let mut escape_next = false;
        loop {
            let (n, found) = {
                let chunk = self.reader.fill_buf().map_err(J2sError::Io)?;
                if chunk.is_empty() {
                    return Err(J2sError::InvalidInput("Unexpected EOF inside JSON value".into()));
                }
                scan_chunk_into(chunk, &mut self.buf, &mut depth, &mut in_string, &mut escape_next)
            }; // chunk borrow ends before consume
            self.reader.consume(n);
            self.bytes_read += n as u64;
            if let Some(b) = found {
                return if b == closer {
                    Ok(())
                } else {
                    Err(J2sError::InvalidInput(format!(
                        "Mismatched bracket: expected '{}', got '{}'",
                        closer as char, b as char
                    )))
                };
            }
        }
    }

    fn collect_string(&mut self) -> Result<()> {
        let mut escape_next = false;
        loop {
            let b = match self.read_byte_tracked()? {
                None => return Err(J2sError::InvalidInput("Unexpected EOF inside JSON string".into())),
                Some(b) => b,
            };
            self.buf.push(b);
            if escape_next {
                escape_next = false;
            } else {
                match b {
                    b'\\' => escape_next = true,
                    b'"' => return Ok(()),
                    _ => {}
                }
            }
        }
    }

    fn collect_primitive(&mut self) -> Result<()> {
        loop {
            let buf = self.reader.fill_buf().map_err(J2sError::Io)?;
            if buf.is_empty() {
                break;
            }
            match buf[0] {
                b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r' => break,
                b => {
                    self.buf.push(b);
                    self.reader.consume(1);
                    self.bytes_read += 1;
                }
            }
        }
        Ok(())
    }

    fn skip_string_rest(&mut self) -> Result<()> {
        let mut escape_next = false;
        loop {
            match self.read_byte_tracked()? {
                None => return Err(J2sError::InvalidInput("Unexpected EOF inside JSON string".into())),
                Some(b) => {
                    if escape_next {
                        escape_next = false;
                    } else {
                        match b {
                            b'\\' => escape_next = true,
                            b'"' => return Ok(()),
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}

impl Iterator for JsonRootWrapperReader {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_raw()? {
            Err(e) => Some(Err(e)),
            Ok(mut bytes) => Some(simd_json::from_slice(&mut bytes).map_err(simd_err)),
        }
    }
}

// ---------------------------------------------------------------------------
// Unified entry point
// ---------------------------------------------------------------------------

pub enum JsonReader {
    Lines(JsonLinesReader),
    Array(JsonArrayReader),
    RootWrapper(JsonRootWrapperReader),
}

impl JsonReader {
    /// Return the raw bytes of the next JSON object without parsing.
    /// Used by `run_parallel` to distribute parsing work to worker threads.
    pub fn next_raw(&mut self) -> Option<Result<Vec<u8>>> {
        match self {
            Self::Lines(r) => r.next_raw(),
            Self::Array(r) => r.next_raw(),
            Self::RootWrapper(r) => r.next_raw(),
        }
    }

    pub fn open(path: &Path) -> Result<(Self, JsonFormat)> {
        let format = detect_format(path)?;
        let reader = match format.clone() {
            JsonFormat::Lines => Self::Lines(JsonLinesReader::open(path)?),
            JsonFormat::Array => Self::Array(JsonArrayReader::open(path)?),
            JsonFormat::RootWrapper(keys) => {
                Self::RootWrapper(JsonRootWrapperReader::open(path, keys)?)
            }
        };
        Ok((reader, format))
    }

    /// The wrapper key currently being streamed, or `None` for non-wrapper formats.
    #[must_use]
    pub fn current_key(&self) -> Option<&str> {
        match self {
            Self::RootWrapper(r) => r.current_key(),
            _ => None,
        }
    }

    #[must_use]
    pub fn bytes_read(&self) -> u64 {
        match self {
            Self::Lines(r) => r.bytes_read(),
            Self::Array(r) => r.bytes_read(),
            Self::RootWrapper(r) => r.bytes_read(),
        }
    }
}

impl Iterator for JsonReader {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Lines(r) => r.next(),
            Self::Array(r) => r.next(),
            Self::RootWrapper(r) => r.next(),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp_file(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        f
    }

    /// next_raw() on a JSON array must return the same bytes that parse to the same value as next().
    #[test]
    fn test_next_raw_array_parity() {
        let json = br#"[{"a": 1}, {"b": 2}]"#;
        let f = tmp_file(json);

        let mut r1 = JsonArrayReader::open(f.path()).unwrap();
        let parsed: Vec<serde_json::Value> = std::iter::from_fn(|| r1.next()).collect::<Result<_>>().unwrap();

        let mut r2 = JsonArrayReader::open(f.path()).unwrap();
        let raw_parsed: Vec<serde_json::Value> = std::iter::from_fn(|| r2.next_raw())
            .collect::<Result<Vec<Vec<u8>>>>().unwrap()
            .into_iter()
            .map(|mut b| simd_json::from_slice(&mut b).unwrap())
            .collect();

        assert_eq!(parsed, raw_parsed);
    }

    /// Strings containing { } must not confuse the boundary detector.
    #[test]
    fn test_next_raw_curly_in_string() {
        let json = br#"[{"key": "value {nested} here", "n": 42}, {"x": "{second}"}]"#;
        let f = tmp_file(json);

        let mut r = JsonArrayReader::open(f.path()).unwrap();
        let raws: Vec<Vec<u8>> = std::iter::from_fn(|| r.next_raw())
            .collect::<Result<_>>().unwrap();

        assert_eq!(raws.len(), 2);
        let v0: serde_json::Value = simd_json::from_slice(&mut raws[0].clone()).unwrap();
        assert_eq!(v0["key"], "value {nested} here");
        assert_eq!(v0["n"], 42);
        let v1: serde_json::Value = simd_json::from_slice(&mut raws[1].clone()).unwrap();
        assert_eq!(v1["x"], "{second}");
    }

    /// Escaped quote inside a string must not end the string prematurely.
    #[test]
    fn test_next_raw_escaped_quote_in_string() {
        let json = br#"[{"q": "say \"hello\""}]"#;
        let f = tmp_file(json);

        let mut r = JsonArrayReader::open(f.path()).unwrap();
        let mut raw = r.next_raw().unwrap().unwrap();
        let v: serde_json::Value = simd_json::from_slice(&mut raw).unwrap();
        assert_eq!(v["q"], r#"say "hello""#);
    }

    /// next_raw() on NDJSON returns one raw line per object.
    #[test]
    fn test_next_raw_ndjson_parity() {
        let json = b"{\"a\":1}\n{\"b\":2}\n";
        let f = tmp_file(json);

        let mut r = JsonLinesReader::open(f.path()).unwrap();
        let raws: Vec<Vec<u8>> = std::iter::from_fn(|| r.next_raw())
            .collect::<Result<_>>().unwrap();

        assert_eq!(raws.len(), 2);
        let v0: serde_json::Value = simd_json::from_slice(&mut raws[0].clone()).unwrap();
        assert_eq!(v0["a"], 1);
    }

    // --- detect_format tests ---

    #[test]
    fn test_detect_format_array() {
        let f = tmp_file(b"[{\"a\": 1}]");
        assert_eq!(detect_format(f.path()).unwrap(), JsonFormat::Array);
    }

    #[test]
    fn test_detect_format_array_with_leading_whitespace() {
        let f = tmp_file(b"  \n  [{\"a\": 1}]");
        assert_eq!(detect_format(f.path()).unwrap(), JsonFormat::Array);
    }

    #[test]
    fn test_detect_format_ndjson_two_objects() {
        let f = tmp_file(b"{\"a\": 1}\n{\"b\": 2}");
        assert_eq!(detect_format(f.path()).unwrap(), JsonFormat::Lines);
    }

    #[test]
    fn test_detect_format_wrapper_mono_key() {
        let f = tmp_file(b"{\"FoundationFoods\": [{\"id\": 1}]}");
        assert_eq!(
            detect_format(f.path()).unwrap(),
            JsonFormat::RootWrapper(vec!["FoundationFoods".to_string()])
        );
    }

    #[test]
    fn test_detect_format_wrapper_multi_key() {
        let f = tmp_file(b"{\"K1\": [{\"a\": 1}], \"K2\": [{\"b\": 2}]}");
        assert_eq!(
            detect_format(f.path()).unwrap(),
            JsonFormat::RootWrapper(vec!["K1".to_string(), "K2".to_string()])
        );
    }

    #[test]
    fn test_detect_format_ndjson_first_object_has_array_value() {
        // First object has an array value — must NOT be mistaken for a wrapper.
        let f = tmp_file(b"{\"items\": [1, 2]}\n{\"items\": [3]}");
        assert_eq!(detect_format(f.path()).unwrap(), JsonFormat::Lines);
    }

    #[test]
    fn test_detect_format_single_object_non_array_values_is_ndjson() {
        // Single root object with no array values → treated as NDJSON (one row).
        let f = tmp_file(b"{\"a\": 1}");
        assert_eq!(detect_format(f.path()).unwrap(), JsonFormat::Lines);
    }

    #[test]
    fn test_detect_format_wrapper_mixed_values_returns_error() {
        // Mixed: some arrays, some non-arrays → clear error.
        let f = tmp_file(b"{\"meta\": {\"v\": 1}, \"foods\": [{\"id\": 1}]}");
        let err = detect_format(f.path()).unwrap_err().to_string();
        assert!(
            err.contains("meta") && err.contains("non-array"),
            "error should name the key and say non-array, got: {err}"
        );
    }

    #[test]
    fn test_detect_format_empty_file_error() {
        let f = tmp_file(b"");
        assert!(detect_format(f.path()).is_err());
    }

    #[test]
    fn test_detect_format_invalid_first_byte_error() {
        let f = tmp_file(b"xyz");
        assert!(detect_format(f.path()).is_err());
    }

    #[test]
    fn test_detect_format_wrapper_nested_array_in_objects() {
        // Array values containing objects with nested arrays must not confuse the scanner.
        let f = tmp_file(b"{\"Foods\": [{\"tags\": [\"a\", \"b\"]}, {\"n\": 2}]}");
        assert_eq!(
            detect_format(f.path()).unwrap(),
            JsonFormat::RootWrapper(vec!["Foods".to_string()])
        );
    }

    #[test]
    fn test_detect_format_wrapper_key_with_escaped_quotes() {
        let f = tmp_file(b"{\"key\\\"name\": [{\"x\": 1}]}");
        let fmt = detect_format(f.path()).unwrap();
        assert_eq!(fmt, JsonFormat::RootWrapper(vec!["key\\\"name".to_string()]));
    }

    // --- JsonRootWrapperReader tests ---

    #[test]
    fn test_wrapper_reader_mono_key_streams_all_elements() {
        let json = br#"{"Foods": [{"id": 1}, {"id": 2}, {"id": 3}]}"#;
        let f = tmp_file(json);
        let mut r = JsonRootWrapperReader::open(f.path(), vec!["Foods".to_string()]).unwrap();
        let items: Vec<serde_json::Value> =
            std::iter::from_fn(|| r.next()).collect::<Result<_>>().unwrap();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0]["id"], 1);
        assert_eq!(items[2]["id"], 3);
    }

    #[test]
    fn test_wrapper_reader_multi_key_streams_all_elements() {
        let json = br#"{"K1": [{"a": 1}, {"a": 2}], "K2": [{"b": 10}]}"#;
        let f = tmp_file(json);
        let keys = vec!["K1".to_string(), "K2".to_string()];
        let items: Vec<serde_json::Value> =
            std::iter::from_fn(|| JsonRootWrapperReader::open(f.path(), keys.clone()).unwrap().next())
                .take(0) // just checking open doesn't panic
                .collect::<Result<_>>().unwrap();
        drop(items);

        let mut r = JsonRootWrapperReader::open(f.path(), keys).unwrap();
        let all: Vec<serde_json::Value> =
            std::iter::from_fn(|| r.next()).collect::<Result<_>>().unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0]["a"], 1);
        assert_eq!(all[1]["a"], 2);
        assert_eq!(all[2]["b"], 10);
    }

    #[test]
    fn test_wrapper_reader_current_key_tracks_active_key() {
        let json = br#"{"K1": [{"a": 1}], "K2": [{"b": 2}]}"#;
        let f = tmp_file(json);
        let mut r =
            JsonRootWrapperReader::open(f.path(), vec!["K1".to_string(), "K2".to_string()])
                .unwrap();
        // Before first element: current_key is K1 (not yet advanced)
        assert_eq!(r.current_key(), Some("K1"));
        let _first = r.next_raw().unwrap().unwrap();
        assert_eq!(r.current_key(), Some("K1"));
        let _second = r.next_raw().unwrap().unwrap(); // K2's first element
        assert_eq!(r.current_key(), Some("K2"));
        assert!(r.next_raw().is_none());
        assert_eq!(r.current_key(), None);
    }

    #[test]
    fn test_wrapper_reader_bytes_read_cumulative() {
        let json = br#"{"K1": [{"a": 1}], "K2": [{"b": 2}]}"#;
        let f = tmp_file(json);
        let mut r =
            JsonRootWrapperReader::open(f.path(), vec!["K1".to_string(), "K2".to_string()])
                .unwrap();
        let _ = r.next_raw().unwrap().unwrap(); // consume K1's element
        let bytes_after_k1 = r.bytes_read();
        let _ = r.next_raw().unwrap().unwrap(); // consume K2's element
        let bytes_after_k2 = r.bytes_read();
        assert!(
            bytes_after_k2 > bytes_after_k1,
            "bytes_read must increase: k1={bytes_after_k1} k2={bytes_after_k2}"
        );
    }

    #[test]
    fn test_wrapper_reader_empty_array_is_skipped() {
        let json = br#"{"Empty": [], "Full": [{"x": 42}]}"#;
        let f = tmp_file(json);
        let mut r =
            JsonRootWrapperReader::open(f.path(), vec!["Empty".to_string(), "Full".to_string()])
                .unwrap();
        let all: Vec<serde_json::Value> =
            std::iter::from_fn(|| r.next()).collect::<Result<_>>().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0]["x"], 42);
    }

    #[test]
    fn test_wrapper_reader_next_raw_parses_correctly() {
        let json = br#"{"Items": [{"n": 1, "s": "hello"}, {"n": 2, "s": "world"}]}"#;
        let f = tmp_file(json);
        let mut r = JsonRootWrapperReader::open(f.path(), vec!["Items".to_string()]).unwrap();
        let raws: Vec<Vec<u8>> =
            std::iter::from_fn(|| r.next_raw()).collect::<Result<_>>().unwrap();
        assert_eq!(raws.len(), 2);
        let v0: serde_json::Value = simd_json::from_slice(&mut raws[0].clone()).unwrap();
        let v1: serde_json::Value = simd_json::from_slice(&mut raws[1].clone()).unwrap();
        assert_eq!(v0["s"], "hello");
        assert_eq!(v1["s"], "world");
    }

    #[test]
    fn test_wrapper_reader_nested_arrays_in_objects() {
        let json = br#"{"Foods": [{"id": 1, "tags": ["a", "b"]}, {"id": 2, "tags": []}]}"#;
        let f = tmp_file(json);
        let mut r = JsonRootWrapperReader::open(f.path(), vec!["Foods".to_string()]).unwrap();
        let all: Vec<serde_json::Value> =
            std::iter::from_fn(|| r.next()).collect::<Result<_>>().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0]["tags"], serde_json::json!(["a", "b"]));
    }

    // --- JsonReader unified entry point tests ---

    #[test]
    fn test_json_reader_open_wrapper_returns_root_wrapper_format() {
        let f = tmp_file(br#"{"Foods": [{"id": 1}]}"#);
        let (_reader, fmt) = JsonReader::open(f.path()).unwrap();
        assert_eq!(fmt, JsonFormat::RootWrapper(vec!["Foods".to_string()]));
    }

    #[test]
    fn test_json_reader_wrapper_streams_all_elements() {
        let f = tmp_file(br#"{"K1": [{"a": 1}, {"a": 2}], "K2": [{"b": 10}]}"#);
        let (mut reader, _fmt) = JsonReader::open(f.path()).unwrap();
        let all: Vec<serde_json::Value> =
            std::iter::from_fn(|| reader.next()).collect::<Result<_>>().unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_json_reader_current_key_wrapper() {
        let f = tmp_file(br#"{"Foods": [{"id": 1}]}"#);
        let (mut reader, _fmt) = JsonReader::open(f.path()).unwrap();
        assert_eq!(reader.current_key(), Some("Foods"));
        let _ = reader.next_raw().unwrap().unwrap();
        // Still "Foods" until the next call discovers the array is exhausted
        assert_eq!(reader.current_key(), Some("Foods"));
        assert!(reader.next_raw().is_none());
        assert_eq!(reader.current_key(), None);
    }

    #[test]
    fn test_json_reader_current_key_returns_none_for_ndjson() {
        let f = tmp_file(br#"{"a": 1}"#);
        let (reader, _fmt) = JsonReader::open(f.path()).unwrap();
        assert_eq!(reader.current_key(), None);
    }

    #[test]
    fn test_json_reader_wrapper_bytes_read_increases() {
        let f = tmp_file(br#"{"K1": [{"a": 1}], "K2": [{"b": 2}]}"#);
        let (mut reader, _fmt) = JsonReader::open(f.path()).unwrap();
        let b0 = reader.bytes_read();
        let _ = reader.next_raw().unwrap().unwrap();
        let b1 = reader.bytes_read();
        let _ = reader.next_raw().unwrap().unwrap();
        let b2 = reader.bytes_read();
        assert!(b1 > b0 && b2 > b1, "bytes_read must monotonically increase: {b0} < {b1} < {b2}");
    }

    #[test]
    fn test_wrapper_reader_bytes_read_tracks_nested_container_content() {
        // bytes_read must account for all bytes of nested containers, not just openers
        let json = br#"{"Items": [{"nested": {"x": 1, "y": 2}}, {"z": 3}]}"#;
        let f = tmp_file(json);
        let mut r = JsonRootWrapperReader::open(f.path(), vec!["Items".to_string()]).unwrap();
        let b0 = r.bytes_read();
        let row1 = r.next_raw().unwrap().unwrap();
        let b1 = r.bytes_read();
        let row2 = r.next_raw().unwrap().unwrap();
        let b2 = r.bytes_read();
        assert!(b1 > b0, "bytes_read must increase after first nested object");
        assert!(b2 > b1, "bytes_read must increase after second object");
        assert!(b2 >= row1.len() as u64 + row2.len() as u64,
            "bytes_read {b2} must be >= sum of row lengths {}", row1.len() + row2.len());
    }

    #[test]
    fn test_array_reader_bytes_read_tracks_container_content() {
        // bytes_read must account for all bytes of each scanned object, not just the opener
        let json = b"[{\"key\": \"value\", \"nested\": {\"x\": 123}}, {\"b\": 2}]";
        let f = tmp_file(json);
        let mut r = JsonArrayReader::open(f.path()).unwrap();
        let b0 = r.bytes_read();
        let row1 = r.next_raw().unwrap().unwrap();
        let b1 = r.bytes_read();
        let row2 = r.next_raw().unwrap().unwrap();
        let b2 = r.bytes_read();
        assert!(b1 > b0, "bytes_read must increase after first object");
        assert!(b2 > b1, "bytes_read must increase after second object");
        // bytes_read after all rows must approach total file size
        assert!(b2 >= row1.len() as u64 + row2.len() as u64,
            "bytes_read {b2} must be at least sum of row lengths {}", row1.len() + row2.len());
    }

    // -------------------------------------------------------------------------
    // collect_container regression tests (via next_raw)
    // -------------------------------------------------------------------------

    #[test]
    fn test_array_reader_collect_container_deeply_nested_roundtrip() {
        // Verifies collect_container correctly handles multi-level depth;
        // any state corruption (e.g. wrong depth count) would truncate or extend the output.
        let obj = r#"{"a":{"b":{"c":{"d":[1,2,3],"e":true},"f":"hello"},"g":null}}"#;
        let json = format!("[{obj}]");
        let f = tmp_file(json.as_bytes());
        let mut r = JsonArrayReader::open(f.path()).unwrap();
        let raw = r.next_raw().unwrap().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&raw).expect("must parse");
        assert_eq!(parsed["a"]["b"]["c"]["d"][2], 3);
        assert_eq!(parsed["a"]["b"]["f"], "hello");
        assert!(r.next_raw().is_none(), "only one element");
    }

    #[test]
    fn test_array_reader_collect_container_brackets_inside_strings() {
        // Brackets and braces inside string values must not affect depth tracking.
        let obj = r#"{"key":"value with } and ] and { and [ inside","num":42}"#;
        let json = format!("[{obj}]");
        let f = tmp_file(json.as_bytes());
        let mut r = JsonArrayReader::open(f.path()).unwrap();
        let raw = r.next_raw().unwrap().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&raw).expect("must parse");
        assert_eq!(parsed["num"], 42);
        assert!(parsed["key"].as_str().unwrap().contains('}'));
        assert!(r.next_raw().is_none(), "only one element");
    }

    #[test]
    fn test_wrapper_reader_collect_container_escaped_quote_in_value() {
        // Escaped quote inside a string must not close the string prematurely,
        // ensuring escape_next state is handled correctly.
        let json = br#"{"Items": [{"msg": "say \"hello\" now", "ok": true}]}"#;
        let f = tmp_file(json);
        let mut r = JsonRootWrapperReader::open(f.path(), vec!["Items".to_string()]).unwrap();
        let raw = r.next_raw().unwrap().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&raw).expect("must parse");
        assert_eq!(parsed["ok"], true);
        assert!(parsed["msg"].as_str().unwrap().contains("hello"));
    }

    // -------------------------------------------------------------------------
    // scan_chunk_into tests
    // -------------------------------------------------------------------------

    #[test]
    fn scan_chunk_into_accumulates_bytes_up_to_closer() {
        let chunk = b"ello}world";
        let mut buf = Vec::new();
        let mut depth = 1u32;
        let mut in_string = false;
        let mut escape_next = false;
        let (n, found) = scan_chunk_into(chunk, &mut buf, &mut depth, &mut in_string, &mut escape_next);
        assert_eq!(found, Some(b'}'));
        assert_eq!(n, 5); // "ello}" = 5 bytes
        assert_eq!(&buf, b"ello}");
        assert_eq!(depth, 0);
    }

    #[test]
    fn scan_chunk_into_no_closer_accumulates_full_chunk() {
        let chunk = b"hello world";
        let mut buf = Vec::new();
        let mut depth = 1u32;
        let mut in_string = false;
        let mut escape_next = false;
        let (n, found) = scan_chunk_into(chunk, &mut buf, &mut depth, &mut in_string, &mut escape_next);
        assert_eq!(found, None);
        assert_eq!(n, chunk.len());
        assert_eq!(&buf, b"hello world");
    }

    #[test]
    fn scan_chunk_into_escape_next_carries_across_boundary() {
        // Start already inside a string; chunk ends with '\' → escape_next must carry to next chunk
        let chunk1 = b"ab\\";
        let mut buf = Vec::new();
        let mut depth = 1u32;
        let mut in_string = true;
        let mut escape_next = false;
        let (n1, found1) = scan_chunk_into(chunk1, &mut buf, &mut depth, &mut in_string, &mut escape_next);
        assert_eq!(found1, None);
        assert_eq!(n1, 3);
        assert!(escape_next, "escape_next must be set after trailing backslash inside string");
        assert!(in_string, "still inside string");

        // Second chunk: first byte '"' is the escaped char — must NOT close the string
        let chunk2 = b"\"cd\"";
        let (n2, found2) = scan_chunk_into(chunk2, &mut buf, &mut depth, &mut in_string, &mut escape_next);
        assert_eq!(found2, None);
        assert_eq!(n2, 4);
        assert!(!escape_next, "escape consumed");
        assert!(!in_string, "last unescaped '\"' closes the string");
    }

    #[test]
    fn scan_chunk_into_nested_containers_increment_depth() {
        // Opening '{' already consumed by caller — chunk is the content after it
        let chunk = b"\"k\":[1,2]}";
        let mut buf = Vec::new();
        let mut depth = 1u32;
        let mut in_string = false;
        let mut escape_next = false;
        let (n, found) = scan_chunk_into(chunk, &mut buf, &mut depth, &mut in_string, &mut escape_next);
        assert_eq!(found, Some(b'}'));
        assert_eq!(n, chunk.len());
        assert_eq!(&buf, chunk);
        assert_eq!(depth, 0);
    }

    #[test]
    fn scan_chunk_into_bracket_inside_string_not_counted() {
        // '}' inside a string must not decrement depth
        let chunk = b"\"}\"}";
        let mut buf = Vec::new();
        let mut depth = 1u32;
        let mut in_string = false;
        let mut escape_next = false;
        let (n, found) = scan_chunk_into(chunk, &mut buf, &mut depth, &mut in_string, &mut escape_next);
        assert_eq!(found, Some(b'}'));
        assert_eq!(n, 4); // full chunk consumed including real '}'
        assert_eq!(depth, 0);
    }

    // --- JsonFormat serde round-trip tests ---

    #[test]
    fn test_json_format_serde_lines() {
        let fmt = JsonFormat::Lines;
        let json = serde_json::to_string(&fmt).unwrap();
        let back: JsonFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(fmt, back);
    }

    #[test]
    fn test_json_format_serde_array() {
        let fmt = JsonFormat::Array;
        let json = serde_json::to_string(&fmt).unwrap();
        let back: JsonFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(fmt, back);
    }

    #[test]
    fn test_json_format_serde_root_wrapper() {
        let fmt = JsonFormat::RootWrapper(vec!["K1".to_string(), "K2".to_string()]);
        let json = serde_json::to_string(&fmt).unwrap();
        let back: JsonFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(fmt, back);
    }

    #[test]
    fn test_json_format_serde_root_wrapper_empty_keys() {
        let fmt = JsonFormat::RootWrapper(vec![]);
        let json = serde_json::to_string(&fmt).unwrap();
        let back: JsonFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(fmt, back);
    }
}
