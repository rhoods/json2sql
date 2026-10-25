//! Static word embeddings of column names (#78, T2): an optional hint, never a decision.
//!
//! A `Model2Vec` model is a folder with `model.safetensors` (one `F32` tensor `embeddings`, vocabulary by dimension)
//! and `tokenizer.json` (a `WordPiece` vocabulary). The user provides it; json2sql never downloads it. A text is
//! lowercased and stripped of accents, cut into words at spaces and punctuation (each sign is a word), each word is
//! cut into `WordPiece` sub-words (longest prefix first, `##` for the continuations), the rows of the table are added up
//! and the sum is normalized. A word longer than 100 characters, or with a piece the vocabulary lacks, is ignored,
//! like `Model2Vec` ignores `[UNK]`. Only `WordPiece` is read: the multilingual model uses another tokenizer (`Unigram`)
//! and is refused with a clear message.
//!
//! The normalizer and the pre-tokenizer of `tokenizer.json` must be the BERT ones (lowercase, accents stripped), or the
//! model is refused.
//!
//! Limits, on purpose: accents are stripped with a table for the Latin letters of U+00C0..U+017F and the combining
//! marks U+0300..U+036F (the standard library has no Unicode normalization), so Vietnamese or other scripts keep
//! theirs; CJK ideographs are not split into one word each; control characters are not removed.
//!
//! Fonctions :
//! - struct `StaticEmbedder` — un modèle chargé : vocabulaire `WordPiece` et table de vecteurs.
//! - fn `StaticEmbedder::load` — charge un dossier de modèle ; refuse tout ce qui n'est pas du `WordPiece` en `F32`.
//! - fn `StaticEmbedder::embed` — vecteur normalisé d'un texte, nul si aucun mot n'est connu.
//! - fn `StaticEmbedder::pieces` — sous-mots d'un mot (plus long préfixe d'abord), `None` s'il est inconnu.
//! - fn `read_tokenizer` — vocabulaire `WordPiece` d'un `tokenizer.json`.
//! - fn `read_vocab` — table jeton vers identifiant d'un modèle `WordPiece`.
//! - fn `check_text_pipeline` — refuse un tokenizer dont le normaliseur ou le pré-découpage ne sont pas ceux de BERT sans casse.
//! - fn `read_table` — tenseur `embeddings` d'un fichier safetensors.
//! - struct `Layout` — emplacement et taille du tenseur `embeddings` dans le fichier.
//! - fn `read_header` — en-tête JSON d'un fichier safetensors et début de ses données.
//! - fn `embeddings_tensor` — le tenseur `embeddings` d'un en-tête, qui doit être le seul et en `F32`.
//! - fn `tensor_layout` — emplacement du tenseur `embeddings` : une matrice dont les décalages correspondent à sa forme.
//! - fn `matrix_shape` — lignes et dimension d'un tenseur qui est une matrice.
//! - fn `numbers` — les entiers d'un champ tableau JSON.
//! - fn `read_floats` — des flottants `f32` petit-boutiens lus et convertis par morceaux.
//! - fn `clean` — minuscules, marques et accents retirés.
//! - fn `base_letter` — lettre de base d'une lettre accentuée minuscule.
//! - fn `words` — mots d'un texte : coupe aux espaces et à la ponctuation.
//! - fn `is_punctuation` — vrai pour la ponctuation ASCII et celle des blocs Unicode retenus.
//! - fn `invalid` — erreur « modèle invalide » qui nomme le fichier.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use serde_json::Value;

use crate::error::{J2sError, Result};

/// A safetensors header is a few hundred bytes; anything bigger than this is not one.
const MAX_HEADER_BYTES: u64 = 16 << 20;
/// Bytes of table read and converted at a time.
const CHUNK_BYTES: usize = 1 << 20;

/// A `Model2Vec` static embedding model.
pub struct StaticEmbedder {
    vocab: HashMap<String, u32>,
    prefix: String,
    max_chars: usize,
    table: Vec<f32>,
    dim: usize,
}

impl StaticEmbedder {
    /// Loads the model in `dir` (`model.safetensors` and `tokenizer.json`).
    ///
    /// # Errors
    /// A missing or unreadable file, a tokenizer that is not `WordPiece`, a tensor that is not a single `F32`
    /// matrix, a table that is shorter than its header says, or a vocabulary that points outside the table.
    pub fn load(dir: &Path) -> Result<Self> {
        let (vocab, prefix, max_chars) = read_tokenizer(&dir.join("tokenizer.json"))?;
        let (table, dim) = read_table(&dir.join("model.safetensors"))?;
        let rows = table.len() / dim;
        if let Some((token, id)) = vocab.iter().find(|(_, &id)| id as usize >= rows) {
            return Err(invalid(
                dir,
                &format!("the vocabulary points outside the table: {token:?} is id {id} and the table has {rows} rows"),
            ));
        }
        Ok(Self {
            vocab,
            prefix,
            max_chars,
            table,
            dim,
        })
    }

    /// The normalized vector of `text`: the sum of the rows of its sub-words, all zeros when no word is known.
    pub fn embed(&self, text: &str) -> Vec<f64> {
        let mut sum = vec![0.0_f64; self.dim];
        for word in words(&clean(text)) {
            for id in self.pieces(&word).unwrap_or_default() {
                let start = id as usize * self.dim;
                for (total, x) in sum.iter_mut().zip(&self.table[start..start + self.dim]) {
                    *total += f64::from(*x);
                }
            }
        }
        let norm = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 0.0 {
            for x in &mut sum {
                *x /= norm;
            }
        }
        sum
    }

    /// The sub-words of a word, longest prefix first; `None` when the word is too long or a piece is unknown.
    fn pieces(&self, word: &str) -> Option<Vec<u32>> {
        let chars: Vec<char> = word.chars().collect();
        if chars.len() > self.max_chars {
            return None;
        }
        let (mut ids, mut start) = (Vec::new(), 0);
        while start < chars.len() {
            let (id, end) = (start + 1..=chars.len()).rev().find_map(|end| {
                let piece: String = chars[start..end].iter().collect();
                let key = if start == 0 {
                    piece
                } else {
                    format!("{}{piece}", self.prefix)
                };
                self.vocab.get(&key).map(|&id| (id, end))
            })?;
            ids.push(id);
            start = end;
        }
        Some(ids)
    }
}

/// The vocabulary, the continuation prefix and the longest word of a `tokenizer.json` of type `WordPiece`.
fn read_tokenizer(path: &Path) -> Result<(HashMap<String, u32>, String, usize)> {
    let text = std::fs::read_to_string(path).map_err(|e| invalid(path, &e.to_string()))?;
    let json: Value =
        serde_json::from_str(&text).map_err(|e| invalid(path, &format!("not valid JSON: {e}")))?;
    let model = &json["model"];
    let kind = model["type"].as_str().unwrap_or("missing");
    if kind != "WordPiece" {
        return Err(invalid(
            path,
            &format!("tokenizer type {kind} is not supported, only WordPiece is read (the multilingual models use Unigram)"),
        ));
    }
    check_text_pipeline(path, &json)?;
    let vocab = read_vocab(path, model)?;
    let prefix = model["continuing_subword_prefix"]
        .as_str()
        .unwrap_or("##")
        .to_string();
    let max_chars = model["max_input_chars_per_word"]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(100);
    Ok((vocab, prefix, max_chars))
}

/// Where the `embeddings` tensor lies in a safetensors file, and its size.
struct Layout {
    dim: usize,
    /// Offsets of the data, from the start of the data section.
    begin: u64,
    end: u64,
}

/// The token to id map of a `WordPiece` `model`.
fn read_vocab(path: &Path, model: &Value) -> Result<HashMap<String, u32>> {
    model["vocab"]
        .as_object()
        .ok_or_else(|| invalid(path, "the tokenizer has no vocabulary"))?
        .iter()
        .map(|(token, id)| {
            let id = id.as_u64().and_then(|id| u32::try_from(id).ok());
            id.map(|id| (token.clone(), id))
                .ok_or_else(|| invalid(path, &format!("token {token:?} has no valid id")))
        })
        .collect()
}

/// The text pipeline of the tokenizer must be the one this reader implements: a `BertNormalizer` that lowercases
/// and strips accents, then a `BertPreTokenizer`. A cased model or another normalizer would be embedded differently
/// from what `Model2Vec` computes, with scores that look right and are not.
fn check_text_pipeline(path: &Path, json: &Value) -> Result<()> {
    let normalizer = &json["normalizer"];
    let kind = normalizer["type"].as_str().unwrap_or("missing");
    if kind != "BertNormalizer" {
        return Err(invalid(
            path,
            &format!("normalizer type {kind} is not supported, only BertNormalizer is read"),
        ));
    }
    for (field, why) in [
        ("lowercase", "cased models"),
        ("strip_accents", "models that keep accents"),
    ] {
        if normalizer[field] == false {
            return Err(invalid(
                path,
                &format!("normalizer.{field} is false: {why} are not supported"),
            ));
        }
    }
    let kind = json["pre_tokenizer"]["type"].as_str().unwrap_or("missing");
    if kind != "BertPreTokenizer" {
        return Err(invalid(
            path,
            &format!("pre_tokenizer type {kind} is not supported, only BertPreTokenizer is read"),
        ));
    }
    Ok(())
}

/// The `embeddings` tensor of a safetensors file (a header length, a JSON header, then the data) and its dimension.
fn read_table(path: &Path) -> Result<(Vec<f32>, usize)> {
    let io = |e: std::io::Error| invalid(path, &e.to_string());
    let mut file = File::open(path).map_err(io)?;
    let file_len = file.metadata().map_err(io)?.len();
    let (header, data_start) = read_header(&mut file, path, file_len)?;
    let layout = tensor_layout(path, &header)?;
    if layout.end > file_len - data_start {
        return Err(invalid(path, "the file is shorter than its header says"));
    }
    file.seek(SeekFrom::Start(data_start + layout.begin))
        .map_err(io)?;
    let bytes = usize::try_from(layout.end - layout.begin).unwrap_or(0);
    Ok((read_floats(&mut file, bytes).map_err(io)?, layout.dim))
}

/// The JSON header of a safetensors file, and where its data section starts.
fn read_header(file: &mut File, path: &Path, file_len: u64) -> Result<(Value, u64)> {
    let io = |e: std::io::Error| invalid(path, &e.to_string());
    let mut length = [0_u8; 8];
    file.read_exact(&mut length).map_err(io)?;
    let header_len = u64::from_le_bytes(length);
    if header_len > MAX_HEADER_BYTES || header_len + 8 > file_len {
        return Err(invalid(
            path,
            "this is not a safetensors file (bad header length)",
        ));
    }
    let mut header = vec![0_u8; usize::try_from(header_len).unwrap_or(0)];
    file.read_exact(&mut header).map_err(io)?;
    let header =
        serde_json::from_slice(&header).map_err(|e| invalid(path, &format!("bad header: {e}")))?;
    Ok((header, header_len + 8))
}

/// The `embeddings` tensor of a header, which must be the only tensor and be in `F32`.
fn embeddings_tensor<'h>(path: &Path, header: &'h Value) -> Result<&'h Value> {
    let others = header.as_object().and_then(|h| {
        h.keys()
            .find(|k| *k != "embeddings" && *k != "__metadata__")
    });
    if let Some(name) = others {
        return Err(invalid(
            path,
            &format!("tensor {name} is not supported, the model must hold embeddings only"),
        ));
    }
    let tensor = &header["embeddings"];
    if tensor["dtype"] != "F32" {
        return Err(invalid(
            path,
            &format!(
                "dtype {} is not supported, only F32 is read",
                tensor["dtype"]
            ),
        ));
    }
    Ok(tensor)
}

/// The layout of the `embeddings` tensor: a matrix whose data offsets match its shape.
fn tensor_layout(path: &Path, header: &Value) -> Result<Layout> {
    let tensor = embeddings_tensor(path, header)?;
    let (rows, dim) = matrix_shape(path, tensor)?;
    let bytes = rows
        .checked_mul(dim)
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| u64::try_from(n).ok());
    match (numbers(tensor, "data_offsets").as_slice(), bytes) {
        ([begin, end], Some(bytes)) if end.checked_sub(*begin) == Some(bytes) => Ok(Layout {
            dim,
            begin: *begin,
            end: *end,
        }),
        _ => Err(invalid(path, "the data offsets do not match the shape")),
    }
}

/// The rows and the dimension of a tensor that is a matrix.
fn matrix_shape(path: &Path, tensor: &Value) -> Result<(usize, usize)> {
    match numbers(tensor, "shape")[..] {
        [rows, dim] if rows > 0 && dim > 0 => usize::try_from(rows)
            .and_then(|rows| usize::try_from(dim).map(|dim| (rows, dim)))
            .map_err(|_| invalid(path, "the shape is too large")),
        _ => Err(invalid(
            path,
            &format!(
                "shape {} is not a matrix of rows by dimensions",
                tensor["shape"]
            ),
        )),
    }
}

/// The integers of a JSON array field.
fn numbers(value: &Value, key: &str) -> Vec<u64> {
    value[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .collect()
}

/// `bytes` bytes of little-endian `f32` from the current position, converted a chunk at a time.
fn read_floats(file: &mut File, bytes: usize) -> std::io::Result<Vec<f32>> {
    let mut table = Vec::with_capacity(bytes / 4);
    let mut buffer = vec![0_u8; CHUNK_BYTES];
    let mut left = bytes;
    while left > 0 {
        let chunk = &mut buffer[..left.min(CHUNK_BYTES)];
        file.read_exact(chunk)?;
        table.extend(
            chunk
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b)),
        );
        left -= chunk.len();
    }
    Ok(table)
}

/// Lowercase, without combining marks, and with the Latin letters of U+00C0..U+017F unaccented.
fn clean(text: &str) -> String {
    text.to_lowercase()
        .chars()
        .filter(|c| !('\u{300}'..='\u{36f}').contains(c))
        .map(base_letter)
        .collect()
}

/// The ASCII letter under a lowercase accented Latin letter (generated from the NFD decomposition); other
/// characters stay as they are.
const fn base_letter(c: char) -> char {
    match c {
        'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => 'a',
        'ç' | 'ć' | 'ĉ' | 'ċ' | 'č' => 'c',
        'ď' => 'd',
        'è' | 'é' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => 'e',
        'ĝ' | 'ğ' | 'ġ' | 'ģ' => 'g',
        'ĥ' => 'h',
        'ì' | 'í' | 'î' | 'ï' | 'ĩ' | 'ī' | 'ĭ' | 'į' => 'i',
        'ĵ' => 'j',
        'ķ' => 'k',
        'ĺ' | 'ļ' | 'ľ' => 'l',
        'ñ' | 'ń' | 'ņ' | 'ň' => 'n',
        'ò' | 'ó' | 'ô' | 'õ' | 'ö' | 'ō' | 'ŏ' | 'ő' => 'o',
        'ŕ' | 'ŗ' | 'ř' => 'r',
        'ś' | 'ŝ' | 'ş' | 'š' => 's',
        'ţ' | 'ť' => 't',
        'ù' | 'ú' | 'û' | 'ü' | 'ũ' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => 'u',
        'ŵ' => 'w',
        'ý' | 'ÿ' | 'ŷ' => 'y',
        'ź' | 'ż' | 'ž' => 'z',
        _ => c,
    }
}

/// The words of a text: it is cut at whitespace, and every punctuation sign is a word of its own.
fn words(text: &str) -> Vec<String> {
    let (mut words, mut current) = (Vec::new(), String::new());
    for c in text.chars() {
        if c.is_whitespace() || is_punctuation(c) {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            if !c.is_whitespace() {
                words.push(c.to_string());
            }
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// ASCII punctuation and symbols, and the punctuation (category P) of Latin-1, General Punctuation, Supplemental
/// Punctuation, CJK Symbols and the fullwidth forms (generated from `unicodedata`).
const fn is_punctuation(c: char) -> bool {
    c.is_ascii_punctuation()
        || matches!(c,
            '\u{a1}' | '\u{a7}' | '\u{ab}' | '\u{b6}'..='\u{b7}' | '\u{bb}' | '\u{bf}' | '\u{2010}'..='\u{2027}'
            | '\u{2030}'..='\u{2043}' | '\u{2045}'..='\u{2051}' | '\u{2053}'..='\u{205e}' | '\u{2e00}'..='\u{2e2e}'
            | '\u{2e30}'..='\u{2e4f}' | '\u{2e52}'..='\u{2e5d}' | '\u{3001}'..='\u{3003}' | '\u{3008}'..='\u{3011}'
            | '\u{3014}'..='\u{301f}' | '\u{3030}' | '\u{303d}' | '\u{ff01}'..='\u{ff03}' | '\u{ff05}'..='\u{ff0a}'
            | '\u{ff0c}'..='\u{ff0f}' | '\u{ff1a}'..='\u{ff1b}' | '\u{ff1f}'..='\u{ff20}' | '\u{ff3b}'..='\u{ff3d}'
            | '\u{ff3f}' | '\u{ff5b}' | '\u{ff5d}' | '\u{ff5f}'..='\u{ff65}')
}

/// The error for a model file that cannot be used, naming it.
fn invalid(path: &Path, why: &str) -> J2sError {
    J2sError::InvalidInput(format!("embedding model {}: {why}", path.display()))
}

/// A toy model for the tests of the modules that read embeddings (after the code, as the header test reads it).
#[cfg(test)]
impl StaticEmbedder {
    /// One row per token, in order.
    pub(super) fn from_rows(rows: &[(&str, &[f32])]) -> Self {
        let dim = rows.first().map_or(0, |(_, row)| row.len());
        Self {
            vocab: rows
                .iter()
                .enumerate()
                .map(|(id, (token, _))| ((*token).to_string(), u32::try_from(id).unwrap_or(0)))
                .collect(),
            prefix: "##".to_string(),
            max_chars: 100,
            table: rows
                .iter()
                .flat_map(|(_, row)| row.iter().copied())
                .collect(),
            dim,
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use std::path::PathBuf;

    use serde_json::{json, Value};

    use super::*;

    const DIM: usize = 3;
    /// Token, id and row of the toy model.
    const TOKENS: [(&str, [f32; DIM]); 11] = [
        ("[UNK]", [9.0, 9.0, 9.0]),
        ("vitamin", [3.0, 0.0, 0.0]),
        ("##s", [0.0, 4.0, 0.0]),
        ("mineral", [0.0, 0.0, 5.0]),
        ("play", [1.0, 1.0, 0.0]),
        ("playing", [0.0, 1.0, 1.0]),
        ("##ing", [7.0, 0.0, 7.0]),
        ("a", [1.0, 0.0, 0.0]),
        ("##a", [0.0, 0.0, 1.0]),
        (",", [0.0, 2.0, 0.0]),
        ("cafe", [2.0, 2.0, 2.0]),
    ];

    fn tokenizer(kind: &str, vocab_size: usize) -> Value {
        let vocab: serde_json::Map<String, Value> = TOKENS
            .iter()
            .take(vocab_size)
            .enumerate()
            .map(|(id, (token, _))| ((*token).to_string(), json!(id)))
            .collect();
        json!({"normalizer": {"type": "BertNormalizer", "clean_text": true, "handle_chinese_chars": true,
                              "strip_accents": null, "lowercase": true},
               "pre_tokenizer": {"type": "BertPreTokenizer"},
               "model": {"type": kind, "unk_token": "[UNK]", "continuing_subword_prefix": "##",
                          "max_input_chars_per_word": 100, "vocab": vocab}})
    }

    /// A safetensors file with the given header and data bytes.
    fn safetensors(header: &Value, data: &[u8]) -> Vec<u8> {
        let header = header.to_string().into_bytes();
        let mut file = (header.len() as u64).to_le_bytes().to_vec();
        file.extend(header);
        file.extend(data);
        file
    }

    fn rows_as_bytes(rows: usize) -> Vec<u8> {
        TOKENS
            .iter()
            .take(rows)
            .flat_map(|(_, row)| row.iter().flat_map(|x| x.to_le_bytes()))
            .collect()
    }

    fn header(dtype: &str, rows: usize) -> Value {
        json!({"embeddings": {"dtype": dtype, "shape": [rows, DIM],
                               "data_offsets": [0, rows * DIM * 4]}})
    }

    /// A model folder holding `tokenizer` and `safetensors`.
    fn folder(tokenizer: &Value, safetensors: &[u8]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("tokenizer.json"), tokenizer.to_string()).unwrap();
        std::fs::write(dir.path().join("model.safetensors"), safetensors).unwrap();
        dir
    }

    fn toy() -> (tempfile::TempDir, StaticEmbedder) {
        let rows = TOKENS.len();
        let dir = folder(
            &tokenizer("WordPiece", rows),
            &safetensors(&header("F32", rows), &rows_as_bytes(rows)),
        );
        let embedder = StaticEmbedder::load(dir.path()).unwrap();
        (dir, embedder)
    }

    fn unit(v: [f64; DIM]) -> Vec<f64> {
        let norm = v.iter().map(|x| x * x).sum::<f64>().sqrt();
        v.iter().map(|x| x / norm).collect()
    }

    fn assert_close(got: &[f64], want: &[f64]) {
        assert_eq!(got.len(), want.len());
        for (g, w) in got.iter().zip(want) {
            assert!((g - w).abs() < 1e-9, "{got:?} != {want:?}");
        }
    }

    fn load_error(dir: &Path) -> String {
        StaticEmbedder::load(dir)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn a_known_word_is_its_row_normalized() {
        let (_dir, e) = toy();
        assert_close(&e.embed("vitamin"), &[1.0, 0.0, 0.0]);
    }

    #[test]
    fn sub_words_add_up_before_the_normalization() {
        let (_dir, e) = toy();
        assert_close(&e.embed("vitamins"), &[0.6, 0.8, 0.0]);
        assert_close(&e.embed("vitamin mineral"), &unit([3.0, 0.0, 5.0]));
    }

    #[test]
    fn the_longest_prefix_comes_first() {
        let (_dir, e) = toy();
        assert_close(&e.embed("playing"), &unit([0.0, 1.0, 1.0]));
    }

    #[test]
    fn a_word_with_an_unknown_piece_is_ignored_whole() {
        let (_dir, e) = toy();
        assert_close(&e.embed("vitamin zzz"), &[1.0, 0.0, 0.0]);
        assert_close(&e.embed("vitamin mineralq"), &[1.0, 0.0, 0.0]);
        assert_close(&e.embed("mineralq"), &[0.0, 0.0, 0.0]);
    }

    #[test]
    fn an_empty_or_unknown_text_is_the_zero_vector_of_the_right_size() {
        let (_dir, e) = toy();
        for text in ["", "   ", "zzz", "😀"] {
            assert_eq!(e.embed(text), vec![0.0; DIM], "{text:?}");
        }
    }

    #[test]
    fn case_accents_and_combining_marks_are_removed() {
        let (_dir, e) = toy();
        let want = unit([2.0, 2.0, 2.0]);
        for text in ["cafe", "Café", "CAFÉ", "cafe\u{301}", "ĉafe"] {
            assert_close(&e.embed(text), &want);
        }
    }

    #[test]
    fn every_punctuation_sign_is_a_word_of_its_own() {
        let (_dir, e) = toy();
        assert_close(&e.embed("vitamin,mineral"), &unit([3.0, 2.0, 5.0]));
        assert_close(&e.embed("vitamin_mineral"), &unit([3.0, 0.0, 5.0]));
        assert_close(&e.embed("vitamin—mineral"), &unit([3.0, 0.0, 5.0]));
    }

    #[test]
    fn a_word_of_a_hundred_characters_is_read_and_one_more_is_ignored() {
        let (_dir, e) = toy();
        assert_close(&e.embed(&"a".repeat(100)), &unit([1.0, 0.0, 99.0]));
        assert_eq!(e.embed(&"a".repeat(101)), vec![0.0; DIM]);
    }

    /// Every lowercase Latin letter of U+00C0..U+017F that Python's NFD decomposition unaccents, and its base letter
    /// (from `unicodedata`, written by `gen_unicode_tables.py`).
    const ACCENTED: (&str, &str) = (
        "àáâãäåçèéêëìíîïñòóôõöùúûüýÿāăąćĉċčďēĕėęěĝğġģĥĩīĭįĵķĺļľńņňōŏőŕŗřśŝşšţťũūŭůűųŵŷźżž",
        "aaaaaaceeeeiiiinooooouuuuyyaaaccccdeeeeegggghiiiijklllnnnooorrrssssttuuuuuuwyzzz",
    );

    #[test]
    fn every_accented_latin_letter_goes_back_to_its_base_letter_like_nfd_does() {
        assert_eq!(ACCENTED.0.chars().count(), ACCENTED.1.chars().count());
        for (accented, base) in ACCENTED.0.chars().zip(ACCENTED.1.chars()) {
            assert_eq!(clean(&accented.to_string()), base.to_string(), "{accented}");
        }
        assert_eq!(
            clean("ßøæđłœ"),
            "ßøæđłœ",
            "letters with no decomposition stay as they are"
        );
    }

    #[test]
    fn a_folder_without_its_files_is_refused_with_the_file_name() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_error(dir.path()).contains("tokenizer.json"));
        std::fs::write(
            dir.path().join("tokenizer.json"),
            tokenizer("WordPiece", 11).to_string(),
        )
        .unwrap();
        assert!(load_error(dir.path()).contains("model.safetensors"));
        assert!(load_error(&PathBuf::from("/nonexistent/model")).contains("/nonexistent/model"));
    }

    #[test]
    fn a_tokenizer_that_is_not_wordpiece_is_refused_and_says_what_is_read() {
        let dir = folder(
            &tokenizer("Unigram", 11),
            &safetensors(&header("F32", 11), &rows_as_bytes(11)),
        );
        let message = load_error(dir.path());
        assert!(
            message.contains("Unigram") && message.contains("WordPiece"),
            "{message}"
        );
    }

    #[test]
    fn a_tokenizer_that_is_cased_or_normalized_differently_is_refused_and_names_the_field() {
        let rows = TOKENS.len();
        let table = safetensors(&header("F32", rows), &rows_as_bytes(rows));
        let outcome = |edit: &dyn Fn(&mut Value)| {
            let mut tok = tokenizer("WordPiece", rows);
            edit(&mut tok);
            StaticEmbedder::load(folder(&tok, &table).path())
        };
        let refused = |edit: &dyn Fn(&mut Value)| {
            outcome(edit)
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
        };
        assert!(refused(&|t| t["normalizer"]["lowercase"] = json!(false)).contains("lowercase"));
        assert!(
            refused(&|t| t["normalizer"]["strip_accents"] = json!(false)).contains("strip_accents")
        );
        assert!(refused(&|t| t["normalizer"]["type"] = json!("Lowercase")).contains("normalizer"));
        assert!(refused(&|t| t["normalizer"] = Value::Null).contains("normalizer"));
        assert!(
            refused(&|t| t["pre_tokenizer"]["type"] = json!("Whitespace"))
                .contains("pre_tokenizer")
        );
        assert!(refused(&|t| t["pre_tokenizer"] = Value::Null).contains("pre_tokenizer"));
        assert!(outcome(&|t| t["normalizer"]["strip_accents"] = json!(true)).is_ok());
        assert!(
            outcome(&|t| t["normalizer"]["lowercase"] = Value::Null).is_ok(),
            "BERT lowercases by default"
        );
    }

    #[test]
    fn a_table_that_is_not_one_f32_matrix_is_refused() {
        let tok = tokenizer("WordPiece", 11);
        let half = folder(&tok, &safetensors(&header("F16", 11), &rows_as_bytes(11)));
        let message = load_error(half.path());
        assert!(
            message.contains("F16") && message.contains("F32"),
            "{message}"
        );
        let mut with_weights = header("F32", 11);
        with_weights["weights"] = json!({"dtype": "F32", "shape": [11], "data_offsets": [0, 44]});
        let weighted = folder(&tok, &safetensors(&with_weights, &rows_as_bytes(11)));
        assert!(load_error(weighted.path()).contains("weights"));
        let flat = json!({"embeddings": {"dtype": "F32", "shape": [33], "data_offsets": [0, 132]}});
        let vector = folder(&tok, &safetensors(&flat, &rows_as_bytes(11)));
        assert!(load_error(vector.path()).contains("shape"));
    }

    #[test]
    fn a_header_that_does_not_describe_the_data_is_refused() {
        let tok = tokenizer("WordPiece", 11);
        let lying =
            json!({"embeddings": {"dtype": "F32", "shape": [11, DIM], "data_offsets": [0, 100]}});
        let dir = folder(&tok, &safetensors(&lying, &rows_as_bytes(11)));
        assert!(load_error(dir.path()).contains("offsets"));
        let empty =
            json!({"embeddings": {"dtype": "F32", "shape": [0, DIM], "data_offsets": [0, 0]}});
        let dir = folder(&tok, &safetensors(&empty, &[]));
        assert!(load_error(dir.path()).contains("shape"));
    }

    #[test]
    fn offsets_near_the_end_of_the_integers_do_not_overflow() {
        let far = json!({"embeddings": {"dtype": "F32", "shape": [11, DIM],
                                        "data_offsets": [u64::MAX - 132, u64::MAX]}});
        let dir = folder(
            &tokenizer("WordPiece", 11),
            &safetensors(&far, &rows_as_bytes(11)),
        );
        assert!(load_error(dir.path()).contains("shorter"));
    }

    #[test]
    fn a_file_that_is_not_safetensors_is_refused() {
        let tok = tokenizer("WordPiece", 11);
        let huge = folder(&tok, &u64::MAX.to_le_bytes());
        assert!(load_error(huge.path()).contains("header length"));
        let text = folder(&tok, &[&5_u64.to_le_bytes()[..], b"not j"].concat());
        assert!(load_error(text.path()).contains("bad header"));
        let tiny = folder(&tok, b"abc");
        assert_ne!(load_error(tiny.path()).len(), 0);
    }

    #[test]
    fn a_table_shorter_than_its_header_says_is_refused() {
        for kept in [100, 131] {
            let short = &rows_as_bytes(11)[..kept];
            let dir = folder(
                &tokenizer("WordPiece", 11),
                &safetensors(&header("F32", 11), short),
            );
            assert!(
                load_error(dir.path()).contains("shorter"),
                "{kept} bytes of 132"
            );
        }
    }

    #[test]
    fn a_vocabulary_whose_last_id_is_one_past_the_table_is_refused() {
        let ten = folder(
            &tokenizer("WordPiece", 11),
            &safetensors(&header("F32", 10), &rows_as_bytes(10)),
        );
        assert!(
            load_error(ten.path()).contains("vocabulary"),
            "ids 0..=10 need 11 rows"
        );
    }

    #[test]
    fn a_vocabulary_that_points_outside_the_table_is_refused() {
        let dir = folder(
            &tokenizer("WordPiece", 11),
            &safetensors(&header("F32", 5), &rows_as_bytes(5)),
        );
        assert!(load_error(dir.path()).contains("vocabulary"));
    }
}
