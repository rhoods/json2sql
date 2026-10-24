//! Sampled values of a table's columns, kept for the data signals (#78, T1d).
//!
//! One [`ValueProfile`] is filled row by row during the same pass that fills the presence bits, so the input is
//! read once. Per column it keeps what the two signals need and nothing more: the set of distinct short strings
//! (as 64-bit hashes: about 8 bytes per distinct value plus tree overhead), and for numbers their range, whether
//! they are all integers, whether they vary, and the values of a regular sub-sample of the rows for the rank
//! correlation. Empty cells (null, `""`, `[]`, `{}`) are ignored, like in the presence signal.
//!
//! Fonctions :
//! - enum `Nature` — nature d'une colonne d'après ses cellules : texte, numérique ou autre.
//! - struct `NumberSummary` — étendue, entiers et variation des nombres d'une colonne.
//! - struct `ValueProfile` — valeurs échantillonnées de toutes les colonnes d'une table.
//! - fn `ValueProfile::new` — profil vide pour un nombre de colonnes et un pas de sous-échantillon des rangs.
//! - fn `ValueProfile::observe_row` — ajoute une ligne : la valeur de chaque colonne, dans l'ordre.
//! - fn `ValueProfile::columns` — nombre de colonnes.
//! - fn `ValueProfile::nature` — nature d'une colonne.
//! - fn `ValueProfile::vocabulary` — valeurs distinctes d'une colonne texte, si elles tiennent dans la borne.
//! - fn `ValueProfile::numbers` — résumé des nombres d'une colonne.
//! - fn `ValueProfile::ranked` — nombres d'une colonne sur les lignes du sous-échantillon (NaN si absent).
//! - struct `ColumnValues` — valeurs échantillonnées d'une colonne.
//! - fn `ColumnValues::new` — colonne sans valeur.
//! - fn `ColumnValues::observe` — ajoute une cellule.
//! - fn `ColumnValues::observe_string` — ajoute une chaîne au vocabulaire.
//! - fn `ColumnValues::observe_number` — ajoute un nombre aux statistiques.
//! - fn `string_hash` — empreinte déterministe d'une chaîne.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use serde_json::Value;

use super::sample::is_present;

/// Rows kept for the rank correlation.
pub const RANK_SAMPLE: usize = 2_000;
/// Strings longer than this (bytes) are free text, not vocabulary.
const MAX_STRING_LEN: usize = 128;
/// A text column with more distinct values than this is an identifier or free text: its vocabulary is dropped.
const MAX_DISTINCT_STRINGS: usize = 20_000;
/// A column is text or numeric when at least this share (percent) of its non-empty cells is.
const NATURE_PERCENT: usize = 90;
/// A column needs at least this many non-empty cells to be called numeric.
const MIN_CELLS: usize = 20;

/// What a column holds, judged on its non-empty cells.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nature {
    /// At least 90 % strings.
    Text,
    /// At least 90 % numbers, among at least 20 non-empty cells.
    Numeric,
    /// Mixed, booleans, arrays, objects or too few cells.
    Other,
}

/// Range and shape of the numbers of a column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NumberSummary {
    pub min: f64,
    pub max: f64,
    /// Every number is an integer.
    pub all_integers: bool,
    /// At least two different numbers.
    pub varies: bool,
}

/// The sampled values of every column of a table.
#[derive(Debug, Clone)]
pub struct ValueProfile {
    columns: Vec<ColumnValues>,
    rank_step: usize,
    rows: usize,
}

/// The sampled values of one column.
#[derive(Debug, Clone)]
struct ColumnValues {
    /// Hashes of the distinct short strings.
    strings: BTreeSet<u64>,
    /// More distinct strings than the bound: the set was dropped.
    overflow: bool,
    string_cells: usize,
    number_cells: usize,
    other_cells: usize,
    summary: Option<NumberSummary>,
    /// Numbers on the rank sample rows, NaN where the cell is not a number.
    ranked: Vec<f64>,
}

impl ColumnValues {
    const fn new() -> Self {
        Self {
            strings: BTreeSet::new(),
            overflow: false,
            string_cells: 0,
            number_cells: 0,
            other_cells: 0,
            summary: None,
            ranked: Vec::new(),
        }
    }

    /// Adds one cell; `ranked_row` says whether this row belongs to the rank sample.
    fn observe(&mut self, cell: Option<&Value>, ranked_row: bool) {
        let mut number = f64::NAN;
        match cell.filter(|value| is_present(value)) {
            Some(Value::String(text)) => self.observe_string(text),
            Some(Value::Number(n)) => match n.as_f64() {
                Some(x) => {
                    number = x;
                    self.observe_number(x);
                }
                None => self.other_cells += 1,
            },
            Some(_) => self.other_cells += 1,
            None => {}
        }
        if ranked_row {
            self.ranked.push(number);
        }
    }

    fn observe_string(&mut self, text: &str) {
        self.string_cells += 1;
        if self.overflow || text.len() > MAX_STRING_LEN {
            return;
        }
        self.strings.insert(string_hash(text));
        if self.strings.len() > MAX_DISTINCT_STRINGS {
            self.overflow = true;
            self.strings.clear();
        }
    }

    #[allow(clippy::float_cmp)] // integrality is an exact property
    fn observe_number(&mut self, x: f64) {
        self.number_cells += 1;
        let integer = x.fract() == 0.0;
        let (min, max, all_integers) = self.summary.map_or((x, x, integer), |s| {
            (s.min.min(x), s.max.max(x), s.all_integers && integer)
        });
        self.summary = Some(NumberSummary {
            min,
            max,
            all_integers,
            varies: min < max,
        });
    }
}

impl ValueProfile {
    /// Empty profile of `columns` columns; every `rank_step`-th row feeds the rank sample.
    pub fn new(columns: usize, rank_step: usize) -> Self {
        Self {
            columns: (0..columns).map(|_| ColumnValues::new()).collect(),
            rank_step: rank_step.max(1),
            rows: 0,
        }
    }

    /// Adds a row: the value of every column in column order, `None` where the row has none.
    pub fn observe_row<'v>(&mut self, cells: impl IntoIterator<Item = Option<&'v Value>>) {
        let ranked_row = self.rows.is_multiple_of(self.rank_step);
        let mut cells = cells.into_iter();
        for column in &mut self.columns {
            column.observe(cells.next().flatten(), ranked_row);
        }
        self.rows += 1;
    }

    /// Number of columns.
    pub const fn columns(&self) -> usize {
        self.columns.len()
    }

    /// Text, numeric or other, judged on the non-empty cells.
    pub fn nature(&self, column: usize) -> Nature {
        let c = &self.columns[column];
        let non_empty = c.string_cells + c.number_cells + c.other_cells;
        if non_empty >= MIN_CELLS && c.number_cells * 100 >= NATURE_PERCENT * non_empty {
            Nature::Numeric
        } else if c.string_cells > 0 && c.string_cells * 100 >= NATURE_PERCENT * non_empty {
            Nature::Text
        } else {
            Nature::Other
        }
    }

    /// Hashes of the distinct short strings of a text column; `None` for other columns and for a text column
    /// with too many distinct values.
    pub fn vocabulary(&self, column: usize) -> Option<&BTreeSet<u64>> {
        let c = &self.columns[column];
        (self.nature(column) == Nature::Text && !c.overflow).then_some(&c.strings)
    }

    /// Range and shape of the numbers of a column, `None` without any number.
    pub fn numbers(&self, column: usize) -> Option<NumberSummary> {
        self.columns[column].summary
    }

    /// The column's numbers on the rank sample rows, NaN where the cell is not a number.
    pub fn ranked(&self, column: usize) -> &[f64] {
        &self.columns[column].ranked
    }
}

/// Deterministic 64-bit hash of a string (same value, same hash, in every run).
fn string_hash(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(clippy::disallowed_methods, clippy::cast_precision_loss, clippy::suboptimal_flops) // synthetic series in the tests
)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Observes `rows` (one array of cells per row) into a profile of `columns` columns.
    fn profile(columns: usize, rank_step: usize, rows: &[Vec<Value>]) -> ValueProfile {
        let mut p = ValueProfile::new(columns, rank_step);
        for row in rows {
            p.observe_row(row.iter().map(Some));
        }
        p
    }

    /// `n` rows of a single column holding `make(i)`.
    fn column(n: usize, make: impl Fn(usize) -> Value) -> ValueProfile {
        profile(1, 1, &(0..n).map(|i| vec![make(i)]).collect::<Vec<_>>())
    }

    #[test]
    fn a_column_of_numbers_is_numeric_and_one_of_strings_is_text() {
        assert_eq!(column(25, |i| json!(i)).nature(0), Nature::Numeric);
        assert_eq!(
            column(25, |i| json!(format!("v{i}"))).nature(0),
            Nature::Text
        );
        assert_eq!(
            column(25, |i| json!(i % 2 == 0)).nature(0),
            Nature::Other,
            "booleans are neither"
        );
        assert_eq!(
            column(25, |i| json!([i])).nature(0),
            Nature::Other,
            "arrays are neither"
        );
    }

    #[test]
    fn nature_needs_90_percent_and_enough_numbers() {
        // 18 numbers + 2 strings: exactly 90 % numbers
        let ninety = column(20, |i| if i < 18 { json!(i) } else { json!("x") });
        assert_eq!(ninety.nature(0), Nature::Numeric);
        // 17 numbers + 3 strings: 85 %
        let eighty_five = column(20, |i| if i < 17 { json!(i) } else { json!("x") });
        assert_eq!(eighty_five.nature(0), Nature::Other);
        // 19 numbers only: fewer than 20
        assert_eq!(column(19, |i| json!(i)).nature(0), Nature::Other);
    }

    #[test]
    fn empty_cells_are_ignored() {
        let rows: Vec<Vec<Value>> = (0..30)
            .map(|i| vec![if i % 2 == 0 { json!(i) } else { json!(null) }])
            .collect();
        let p = profile(1, 1, &rows);
        assert_eq!(
            p.nature(0),
            Nature::Other,
            "15 numbers: below the minimum, and the nulls do not count as other cells"
        );
        let rows: Vec<Vec<Value>> = (0..40)
            .map(|i| {
                vec![match i % 4 {
                    0 => json!(i),
                    1 => json!(""),
                    2 => json!([]),
                    _ => json!({}),
                }]
            })
            .collect();
        assert_eq!(profile(1, 1, &rows).numbers(0).map(|n| n.max), Some(36.0));
    }

    #[test]
    fn numbers_are_summarized_by_range_integrality_and_variation() {
        let ints = column(30, |i| json!(1990 + i));
        assert_eq!(
            ints.numbers(0),
            Some(NumberSummary {
                min: 1990.0,
                max: 2019.0,
                all_integers: true,
                varies: true
            })
        );
        let floats = column(30, |i| json!(i as f64 / 4.0 - 3.0));
        let s = floats.numbers(0).unwrap();
        assert!(!s.all_integers && s.min < 0.0);
        let constant = column(30, |_| json!(7));
        assert!(!constant.numbers(0).unwrap().varies);
        assert_eq!(
            column(30, |i| json!(format!("t{i}"))).numbers(0),
            None,
            "text columns have no number summary"
        );
    }

    #[test]
    fn the_vocabulary_holds_distinct_short_strings_only() {
        let rows: Vec<Vec<Value>> = ["a", "b", "a", "c", &"x".repeat(129), &"y".repeat(128)]
            .iter()
            .map(|s| vec![json!(s)])
            .collect();
        let p = profile(1, 1, &rows);
        assert_eq!(
            p.vocabulary(0).map(BTreeSet::len),
            Some(4),
            "a, b, c and the 128-byte string; the 129-byte one is free text"
        );
        assert_eq!(string_hash("a"), string_hash("a"));
        assert_ne!(string_hash("a"), string_hash("b"));
    }

    #[test]
    fn too_many_distinct_strings_drop_the_vocabulary() {
        let p = column(20_001, |i| json!(format!("value {i}")));
        assert_eq!(p.vocabulary(0), None);
        assert_eq!(p.nature(0), Nature::Text, "still a text column");
        let ok = column(20_000, |i| json!(format!("value {i}")));
        assert_eq!(ok.vocabulary(0).map(BTreeSet::len), Some(20_000));
    }

    #[test]
    fn the_rank_sample_takes_every_step_th_row_and_marks_missing_numbers() {
        let rows: Vec<Vec<Value>> = (0..10)
            .map(|i| vec![if i == 3 { json!("n/a") } else { json!(i) }])
            .collect();
        let p = profile(1, 3, &rows);
        let ranked = p.ranked(0);
        assert_eq!(ranked.len(), 4, "rows 0, 3, 6, 9");
        assert_eq!((ranked[0], ranked[2], ranked[3]), (0.0, 6.0, 9.0));
        assert!(ranked[1].is_nan(), "row 3 is a string");
    }

    #[test]
    fn a_short_row_leaves_the_last_columns_empty() {
        let mut p = ValueProfile::new(3, 1);
        p.observe_row([Some(&json!(1))]);
        p.observe_row([Some(&json!(2)), Some(&json!(3)), Some(&json!(4))]);
        assert_eq!(p.columns(), 3);
        assert_eq!(p.ranked(2).len(), 2);
        assert!(p.ranked(2)[0].is_nan() && (p.ranked(2)[1] - 4.0).abs() < 1e-12);
    }
}
