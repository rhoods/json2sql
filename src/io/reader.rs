use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

use serde_json::Value;

use crate::error::{J2sError, Result};

/// Map a simd-json parse error to J2sError.
fn simd_err(e: simd_json::Error) -> J2sError {
    J2sError::InvalidInput(format!("JSON parse error: {}", e))
}

/// Detected format of the input file.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JsonFormat {
    /// Top-level JSON array: `[{...}, {...}, ...]`
    Array,
    /// One JSON object per line (JSON-Lines / NDJSON)
    Lines,
}

/// Detect the format by peeking at the first non-whitespace byte.
pub fn detect_format(path: &Path) -> Result<JsonFormat> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buf = [0u8; 1024];
    let n = reader.read(&mut buf)?;
    for &b in &buf[..n] {
        if b.is_ascii_whitespace() {
            continue;
        }
        return match b {
            b'[' => Ok(JsonFormat::Array),
            b'{' => Ok(JsonFormat::Lines),
            other => Err(J2sError::InvalidInput(format!(
                "Expected '[' or '{{' as first character, found '{}'",
                other as char
            ))),
        };
    }
    Err(J2sError::InvalidInput("File appears to be empty".to_string()))
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

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl Iterator for JsonLinesReader {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.line_buf.clear();
            match self.reader.read_until(b'\n', &mut self.line_buf) {
                Ok(0) => return None,
                Ok(n) => {
                    self.bytes_read += n as u64;
                    // Trim ASCII whitespace without allocation
                    let start = self.line_buf.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(self.line_buf.len());
                    let end = self.line_buf.iter().rposition(|b| !b.is_ascii_whitespace()).map(|i| i + 1).unwrap_or(0);
                    if start >= end {
                        continue;
                    }
                    let slice = &mut self.line_buf[start..end];
                    return Some(simd_json::from_slice(slice).map_err(simd_err));
                }
                Err(e) => return Some(Err(J2sError::Io(e))),
            }
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

    pub fn bytes_read(&self) -> u64 {
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
                    b' ' | b'\t' | b'\n' | b'\r' | b',' => continue,
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
            let b = match self.read_byte() {
                None => return Err(J2sError::InvalidInput("Unexpected EOF inside JSON value".to_string())),
                Some(Err(e)) => return Err(J2sError::Io(e)),
                Some(Ok(b)) => b,
            };
            self.buf.push(b);

            if escape_next {
                escape_next = false;
                continue;
            }

            if in_string {
                match b {
                    b'\\' => escape_next = true,
                    b'"' => in_string = false,
                    _ => {}
                }
            } else {
                match b {
                    b'"' => in_string = true,
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            // Verify it's the right closer
                            if b != closer {
                                return Err(J2sError::InvalidInput(
                                    format!("Mismatched bracket: expected '{}', got '{}'",
                                        closer as char, b as char)
                                ));
                            }
                            return Ok(());
                        }
                    }
                    _ => {}
                }
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
                    Ok(b) if b.is_ascii_whitespace() => continue,
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
// Unified entry point
// ---------------------------------------------------------------------------

pub enum JsonReader {
    Lines(JsonLinesReader),
    Array(JsonArrayReader),
}

impl JsonReader {
    pub fn open(path: &Path) -> Result<(Self, JsonFormat)> {
        let format = detect_format(path)?;
        let reader = match format {
            JsonFormat::Lines => JsonReader::Lines(JsonLinesReader::open(path)?),
            JsonFormat::Array => JsonReader::Array(JsonArrayReader::open(path)?),
        };
        Ok((reader, format))
    }

    pub fn bytes_read(&self) -> u64 {
        match self {
            JsonReader::Lines(r) => r.bytes_read(),
            JsonReader::Array(r) => r.bytes_read(),
        }
    }
}

impl Iterator for JsonReader {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            JsonReader::Lines(r) => r.next(),
            JsonReader::Array(r) => r.next(),
        }
    }
}
