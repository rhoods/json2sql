//! Bounded sample of the rows of one table, reread from the input after finalization (#78, T1c).
//!
//! The presence signal (see [`super::presence`]) needs the values, which Pass 1 does not keep. The input is
//! therefore read a second time: rows of the root table are taken at a regular stride computed from the root
//! row count, up to a budget, so that the sample spans a sorted file instead of reading its first rows. Same
//! file and same arguments give the same sample. Only the sampled rows are parsed; the others are skipped
//! as raw bytes. A value counts as present unless it is null, `""`, `[]` or `{}` (the rule the spike measured).
//!
//! Limits, on purpose: the path from the root to the table goes through objects only (an array on the way is
//! refused, so no silent mis-sampling of array tables), and a column is one key path inside the table's
//! object, which the caller derives from the schema.
//!
//! Fonctions :
//! - struct `SampleSource` — où lire : fichier, table racine, chemin vers la table, chemins des colonnes, bornes.
//! - fn `sample_presence` — présence des colonnes dans un échantillon à pas régulier des lignes de la table.
//! - fn `add_row` — analyse une ligne échantillonnée et ajoute l'objet de la table à la présence.
//! - fn `check` — refuse un nombre de lignes racine inconnu ou un budget nul.
//! - fn `check_root` — refuse une table racine qui n'est pas une clé du fichier enveloppe.
//! - fn `stride` — pas d'échantillonnage : une ligne sur `ceil(lignes / budget)`.
//! - fn `object_at` — objet de la table dans une ligne racine en suivant le chemin ; erreur si un tableau s'y trouve.
//! - fn `array_on_path` — erreur « tableau non pris en charge » sur le chemin d'une table.
//! - fn `value_at` — valeur d'une colonne dans l'objet de la table, en suivant son chemin de clés.
//! - fn `is_present` — vrai sauf pour null, la chaîne vide, le tableau vide et l'objet vide.

use std::path::Path;

use serde_json::{Map, Value};

use crate::error::{J2sError, Result};
use crate::io::reader::{simd_err, JsonFormat, JsonReader};

use super::presence::Presence;

/// Most rows sampled by default.
#[allow(dead_code)] // shortcut: the binary re-includes `schema` and nothing calls this until the proposal command (T1e); drop then.
pub const DEFAULT_BUDGET: usize = 20_000;

/// What to read and where the table's columns are.
#[allow(dead_code)] // shortcut: see `DEFAULT_BUDGET`
pub struct SampleSource<'a> {
    /// Input file, read again from its start.
    pub input: &'a Path,
    /// Name of the root table: the wrapper key for a multi-key wrapper file, whose other keys are ignored.
    pub root: &'a str,
    /// Keys to follow below a root row to reach the table's object (empty for the root table itself).
    pub path: &'a [String],
    /// One key path per column, from the table's object (two keys for a column flattened from a child).
    pub columns: &'a [Vec<String>],
    /// Rows of the ROOT table (Pass 1 `row_count`), even when sampling a child: the stride is computed from it.
    pub root_rows: u64,
    /// `--limit`: only the first rows of the root table take part.
    pub limit: Option<u64>,
    /// Most sampled rows.
    pub budget: usize,
}

/// Presence of `source.columns` in a regular-stride sample of the table's rows.
///
/// # Errors
/// An unknown (zero) root row count, a null budget, an unreadable or malformed input, a root that is not a key
/// of a wrapper file, or an array on the path from the root to the table.
#[allow(dead_code)] // shortcut: see `DEFAULT_BUDGET`
pub fn sample_presence(source: &SampleSource<'_>) -> Result<Presence> {
    check(source)?;
    let rows = source
        .limit
        .map_or(source.root_rows, |limit| limit.min(source.root_rows));
    let step = stride(rows, source.budget);
    let (mut reader, format) = JsonReader::open(source.input)?;
    check_root(&format, source.root)?;
    let mut presence = Presence::new(source.columns.len());
    let mut seen = 0_u64;
    while seen < rows {
        let Some(raw) = reader.next_raw() else { break };
        let mut bytes = raw?;
        if reader.current_key().is_some_and(|key| key != source.root) {
            continue;
        }
        if seen.is_multiple_of(step) {
            add_row(&mut presence, &mut bytes, source)?;
            if presence.rows() >= source.budget {
                break;
            }
        }
        seen += 1;
    }
    Ok(presence)
}

/// Parses one sampled root row and adds the table's object to the presence, when the row has one.
fn add_row(presence: &mut Presence, bytes: &mut [u8], source: &SampleSource<'_>) -> Result<()> {
    let row: Value = simd_json::from_slice(bytes).map_err(simd_err)?;
    if let Some(object) = object_at(&row, source.path)? {
        presence.push_row(
            source
                .columns
                .iter()
                .map(|column| value_at(object, column).is_some_and(is_present)),
        );
    }
    Ok(())
}

/// A stride cannot be computed without the root row count, and a sample needs room for one row.
fn check(source: &SampleSource<'_>) -> Result<()> {
    if source.root_rows == 0 {
        return Err(J2sError::InvalidInput(
            "the root row count is unknown (0): the sampling stride cannot be computed, count the rows first".into(),
        ));
    }
    if source.budget == 0 {
        return Err(J2sError::InvalidInput(
            "the sample budget must be at least one row".into(),
        ));
    }
    Ok(())
}

/// In a wrapper file the root table is one of its keys; any other name would sample nothing without a word.
fn check_root(format: &JsonFormat, root: &str) -> Result<()> {
    match format {
        JsonFormat::RootWrapper(keys) if !keys.iter().any(|key| key == root) => {
            Err(J2sError::InvalidInput(format!(
                "root table {root:?} is not a key of the wrapper file (keys: {keys:?})"
            )))
        }
        _ => Ok(()),
    }
}

/// One row out of `ceil(rows / budget)`, so that at most `budget` rows are taken.
fn stride(rows: u64, budget: usize) -> u64 {
    rows.div_ceil(u64::try_from(budget.max(1)).unwrap_or(u64::MAX))
        .max(1)
}

/// The table's object inside a root row, following `path` through objects. `None` when the row has no object
/// there (missing key, null, scalar): such a row has nothing to sample.
fn object_at<'v>(row: &'v Value, path: &[String]) -> Result<Option<&'v Map<String, Value>>> {
    let mut current = row;
    for key in path {
        if current.is_array() {
            return Err(array_on_path(path));
        }
        match current.get(key.as_str()) {
            Some(next) => current = next,
            None => return Ok(None),
        }
    }
    match current {
        Value::Object(object) => Ok(Some(object)),
        Value::Array(_) => Err(array_on_path(path)),
        _ => Ok(None),
    }
}

fn array_on_path(path: &[String]) -> J2sError {
    J2sError::InvalidInput(format!("unsupported: an array lies on the path to the table ({path:?}), only object paths are sampled"))
}

/// The value of a column inside the table's object, following its key path.
fn value_at<'v>(object: &'v Map<String, Value>, key_path: &[String]) -> Option<&'v Value> {
    let (first, rest) = key_path.split_first()?;
    rest.iter()
        .try_fold(object.get(first)?, |value, key| value.get(key.as_str()))
}

/// The spike's "non-empty value" rule: null, `""`, `[]` and `{}` are absent; `0` and `false` are present.
fn is_present(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(fields) => !fields.is_empty(),
        Value::Bool(_) | Value::Number(_) => true,
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use std::io::Write;

    use serde_json::json;

    use super::*;

    /// A temporary file holding `content`.
    fn file(content: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    fn jsonl(rows: &[Value]) -> tempfile::NamedTempFile {
        file(
            &rows
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }

    fn keys(names: &[&str]) -> Vec<Vec<String>> {
        names.iter().map(|n| vec![(*n).to_string()]).collect()
    }

    fn strings(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    /// Samples `columns` of the root table of `input` (or of the table at `path`), no limit.
    fn sample(
        input: &Path,
        path: &[String],
        columns: &[Vec<String>],
        root_rows: u64,
        budget: usize,
    ) -> Result<Presence> {
        sample_presence(&SampleSource {
            input,
            root: "t",
            path,
            columns,
            root_rows,
            limit: None,
            budget,
        })
    }

    fn counts(p: &Presence, columns: usize) -> Vec<usize> {
        (0..columns).map(|c| p.present_count(c)).collect()
    }

    #[test]
    fn a_value_is_present_unless_null_or_empty() {
        for empty in [json!(null), json!(""), json!([]), json!({})] {
            assert!(!is_present(&empty), "{empty}");
        }
        for filled in [
            json!("x"),
            json!(" "),
            json!(0),
            json!(false),
            json!([1]),
            json!({"a": 1}),
        ] {
            assert!(is_present(&filled), "{filled}");
        }
    }

    #[test]
    fn the_stride_keeps_the_sample_within_the_budget() {
        assert_eq!(stride(1000, 100), 10);
        assert_eq!(stride(1001, 100), 11);
        assert_eq!(stride(50, 100), 1);
        assert_eq!(stride(4_402_679, 20_000), 221);
        for rows in [1_u64, 99, 100, 101, 999, 12_345] {
            assert!(rows.div_ceil(stride(rows, 100)) <= 100, "{rows}");
        }
    }

    #[test]
    fn rows_are_taken_at_a_regular_stride_from_the_first() {
        let rows: Vec<Value> = (0..100)
            .map(|i| json!({"every_tenth": (i % 10 == 0).then_some(1), "fifth_of_ten": (i % 10 == 5).then_some(1), "always": 1}))
            .collect();
        let f = jsonl(&rows);
        let p = sample(
            f.path(),
            &[],
            &keys(&["every_tenth", "fifth_of_ten", "always"]),
            100,
            10,
        )
        .unwrap();
        assert_eq!(p.rows(), 10);
        assert_eq!(
            counts(&p, 3),
            [10, 0, 10],
            "rows 0, 10, 20... are sampled, row 5 never"
        );
    }

    #[test]
    fn the_sample_is_the_same_each_time() {
        let rows: Vec<Value> = (0..300)
            .map(|i| json!({"a": (i % 3 == 0).then_some(1), "b": (i % 7 == 0).then_some(1)}))
            .collect();
        let f = jsonl(&rows);
        let run = || {
            counts(
                &sample(f.path(), &[], &keys(&["a", "b"]), 300, 50).unwrap(),
                2,
            )
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn the_limit_bounds_the_rows_and_the_stride() {
        let rows: Vec<Value> = (0..100)
            .map(|i| json!({"late": (i >= 30).then_some(1), "early": (i < 30).then_some(1)}))
            .collect();
        let f = jsonl(&rows);
        let cols = keys(&["late", "early"]);
        let limited = sample_presence(&SampleSource {
            input: f.path(),
            root: "t",
            path: &[],
            columns: &cols,
            root_rows: 100,
            limit: Some(30),
            budget: 100,
        })
        .unwrap();
        assert_eq!(
            (limited.rows(), counts(&limited, 2)),
            (30, vec![0, 30]),
            "stride over 30 rows, nothing past them"
        );
    }

    #[test]
    fn a_child_table_follows_the_path_and_skips_rows_without_an_object() {
        let f = jsonl(&[
            json!({"id": 1, "properties": {"a": 1, "b": ""}}),
            json!({"id": 2, "properties": null}),
            json!({"id": 3}),
            json!({"id": 4, "properties": "oops"}),
            json!({"id": 5, "properties": {"a": null, "b": "x"}}),
        ]);
        let p = sample(
            f.path(),
            &strings(&["properties"]),
            &keys(&["a", "b"]),
            5,
            100,
        )
        .unwrap();
        assert_eq!((p.rows(), counts(&p, 2)), (2, vec![1, 1]));
    }

    #[test]
    fn a_flattened_column_is_a_two_key_path() {
        let f = jsonl(&[
            json!({"nutrients": {"fat": 3}}),
            json!({"nutrients": {"fat": ""}}),
            json!({"nutrients": null}),
        ]);
        let cols = vec![strings(&["nutrients", "fat"]), strings(&["missing", "fat"])];
        let p = sample(f.path(), &[], &cols, 3, 100).unwrap();
        assert_eq!((p.rows(), counts(&p, 2)), (3, vec![1, 0]));
    }

    #[test]
    fn an_array_on_the_path_is_refused() {
        let f = jsonl(&[
            json!({"id": 1, "items": [{"a": 1}]}),
            json!({"id": 2, "items": []}),
        ]);
        let err = sample(f.path(), &strings(&["items"]), &keys(&["a"]), 2, 100)
            .unwrap_err()
            .to_string();
        assert!(err.contains("array"), "{err}");
    }

    #[test]
    fn an_array_in_the_middle_of_the_path_is_refused_too() {
        let f = jsonl(&[
            json!({"id": 1, "items": [{"sub": {"a": 1}}]}),
            json!({"id": 2, "items": []}),
        ]);
        let err = sample(f.path(), &strings(&["items", "sub"]), &keys(&["a"]), 2, 100)
            .unwrap_err()
            .to_string();
        assert!(err.contains("array"), "{err}");
    }

    #[test]
    fn an_array_file_and_a_wrapper_file_give_the_same_sample_as_lines() {
        let rows: Vec<Value> = (0..40)
            .map(|i| json!({"a": (i % 2 == 0).then_some(1), "b": (i % 5 == 0).then_some(1)}))
            .collect();
        let lines = jsonl(&rows);
        let array = file(&Value::Array(rows.clone()).to_string());
        let wrapper = file(&json!({"t": rows}).to_string());
        let want = counts(
            &sample(lines.path(), &[], &keys(&["a", "b"]), 40, 20).unwrap(),
            2,
        );
        for other in [&array, &wrapper] {
            assert_eq!(
                counts(
                    &sample(other.path(), &[], &keys(&["a", "b"]), 40, 20).unwrap(),
                    2
                ),
                want
            );
        }
    }

    #[test]
    fn only_the_rows_of_the_root_key_are_sampled_in_a_multi_key_wrapper() {
        let f = file(
            &json!({"other": [{"z": 1}, {"z": 1}, {"z": 1}], "t": [{"a": 1}, {"a": 1}]})
                .to_string(),
        );
        let p = sample(f.path(), &[], &keys(&["a", "z"]), 2, 100).unwrap();
        assert_eq!(
            (p.rows(), counts(&p, 2)),
            (2, vec![2, 0]),
            "rows of the other key are not the root table's"
        );
    }

    #[test]
    fn a_root_that_is_not_a_wrapper_key_is_refused_instead_of_sampling_nothing() {
        let f = file(&json!({"products": [{"a": 1}], "other": [{"a": 1}]}).to_string());
        let err = sample(f.path(), &[], &keys(&["a"]), 1, 100)
            .unwrap_err()
            .to_string();
        assert!(err.contains("products") && err.contains("\"t\""), "{err}");
    }

    #[test]
    fn an_unknown_row_count_or_a_null_budget_are_refused() {
        let f = jsonl(&[json!({"a": 1})]);
        assert!(sample(f.path(), &[], &keys(&["a"]), 0, 100)
            .unwrap_err()
            .to_string()
            .contains("row count"));
        assert!(sample(f.path(), &[], &keys(&["a"]), 1, 0)
            .unwrap_err()
            .to_string()
            .contains("budget"));
    }

    #[test]
    fn a_missing_file_is_an_error() {
        assert!(sample(
            Path::new("/nonexistent/j2s-sample.jsonl"),
            &[],
            &keys(&["a"]),
            1,
            10
        )
        .is_err());
    }
}
