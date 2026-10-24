//! Numeric signals between columns (#78, T1d): rank correlation and precise shapes.
//!
//! Two numeric columns that move together (a minimum and a maximum of the same quantity, a population and
//! a GDP) have a high Spearman correlation: the Pearson correlation of their ranks on a regular sub-sample of
//! the rows (about 2 000), over the rows where both are numbers (at least 30). Ranks are computed once per
//! column, missing cells ranking lowest, as in the #78 spike. A column also has a *precise shape* when its
//! numbers look like years, epoch seconds or coordinates in degrees. Generic shapes (small integers, an order of
//! magnitude) make absurd groups and are not used; a shape is a hint, never a decision on its own.
//!
//! Fonctions :
//! - enum `Shape` — forme précise des nombres d'une colonne : année, epoch en secondes, degrés.
//! - struct `RankPair` — deux colonnes numériques fortement corrélées en rang.
//! - fn `shape` — forme précise d'une colonne numérique, si elle en a une.
//! - fn `rank_correlations` — paires de colonnes numériques dont la corrélation de rang atteint 0,9 en valeur absolue.
//! - fn `average_ranks` — rangs moyens des valeurs, les valeurs absentes (NaN) classées en dernier rang bas.
//! - fn `pearson_on` — corrélation de Pearson de deux listes de rangs sur les lignes données.

use super::values::{Nature, ValueProfile};

/// A pair needs at least this many rows where both columns are numbers.
pub const MIN_COMMON_ROWS: usize = 30;
/// Smallest absolute rank correlation reported.
pub const MIN_ABS_RHO: f64 = 0.9;

/// What the numbers of a column look like, when it is specific enough to hint at a theme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// Integers from 1800 to 2100.
    Year,
    /// Integers from 8e8 to 2.2e9 (1995 to 2039 in seconds).
    EpochSeconds,
    /// Non-integers from -180 to 180, some negative.
    Degrees,
}

/// Two numeric columns whose ranks move together.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RankPair {
    /// Index of the first column (smaller than `b`).
    pub a: usize,
    pub b: usize,
    /// Spearman correlation, from -1 to 1.
    pub rho: f64,
    /// Rows where both columns are numbers.
    pub rows: usize,
}

/// The precise shape of a numeric column that varies, `None` for any other column.
pub fn shape(profile: &ValueProfile, column: usize) -> Option<Shape> {
    if profile.nature(column) != Nature::Numeric {
        return None;
    }
    let numbers = profile.numbers(column).filter(|n| n.varies)?;
    if numbers.all_integers && numbers.min >= 1800.0 && numbers.max <= 2100.0 {
        Some(Shape::Year)
    } else if numbers.all_integers && numbers.min >= 8e8 && numbers.max <= 2.2e9 {
        Some(Shape::EpochSeconds)
    } else if !numbers.all_integers
        && numbers.min < 0.0
        && numbers.min >= -180.0
        && numbers.max <= 180.0
    {
        Some(Shape::Degrees)
    } else {
        None
    }
}

/// Every pair of numeric columns with at least [`MIN_COMMON_ROWS`] common numbers and a rank correlation of at
/// least [`MIN_ABS_RHO`] in absolute value, ordered by `(a, b)`.
pub fn rank_correlations(profile: &ValueProfile) -> Vec<RankPair> {
    let columns: Vec<usize> = (0..profile.columns())
        .filter(|&c| profile.nature(c) == Nature::Numeric)
        .collect();
    let ranks: Vec<Vec<f64>> = columns
        .iter()
        .map(|&c| average_ranks(profile.ranked(c)))
        .collect();
    let mut pairs = Vec::new();
    for (x, &a) in columns.iter().enumerate() {
        for (y, &b) in columns.iter().enumerate().skip(x + 1) {
            let both: Vec<usize> = (0..profile.ranked(a).len())
                .filter(|&row| !profile.ranked(a)[row].is_nan() && !profile.ranked(b)[row].is_nan())
                .collect();
            if both.len() < MIN_COMMON_ROWS {
                continue;
            }
            let rho = pearson_on(&ranks[x], &ranks[y], &both);
            if rho.abs() >= MIN_ABS_RHO {
                pairs.push(RankPair {
                    a,
                    b,
                    rho,
                    rows: both.len(),
                });
            }
        }
    }
    pairs
}

/// Average rank (from 1) of each value, ties sharing the mean of their positions; NaN ranks as the lowest value.
#[allow(clippy::cast_precision_loss)] // positions are far below 2^52
fn average_ranks(values: &[f64]) -> Vec<f64> {
    let key = |i: usize| {
        if values[i].is_nan() {
            f64::NEG_INFINITY
        } else {
            values[i] + 0.0
        }
    };
    let mut order: Vec<usize> = (0..values.len()).collect();
    order.sort_by(|&x, &y| key(x).total_cmp(&key(y)));
    let mut ranks = vec![0.0; values.len()];
    let mut start = 0;
    while start < order.len() {
        let mut end = start;
        while end + 1 < order.len() && key(order[end + 1]).total_cmp(&key(order[start])).is_eq() {
            end += 1;
        }
        for &row in &order[start..=end] {
            ranks[row] = (start + end) as f64 / 2.0 + 1.0;
        }
        start = end + 1;
    }
    ranks
}

/// Pearson correlation of `a` and `b` over the listed rows; 0 when either has no variance there.
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)] // plain sums, in the order of the spike
fn pearson_on(a: &[f64], b: &[f64], rows: &[usize]) -> f64 {
    let n = rows.len() as f64;
    let mean = |v: &[f64]| rows.iter().fold(0.0, |sum, &r| sum + v[r]) / n;
    let (mean_a, mean_b) = (mean(a), mean(b));
    let spread = |v: &[f64], m: f64| {
        rows.iter()
            .fold(0.0, |sum, &r| sum + (v[r] - m) * (v[r] - m))
            .sqrt()
    };
    let (spread_a, spread_b) = (spread(a, mean_a), spread(b, mean_b));
    if spread_a > 0.0 && spread_b > 0.0 {
        rows.iter()
            .fold(0.0, |sum, &r| sum + (a[r] - mean_a) * (b[r] - mean_b))
            / (spread_a * spread_b)
    } else {
        0.0
    }
}

#[cfg(test)]
#[cfg_attr(
    test,
    allow(clippy::disallowed_methods, clippy::cast_precision_loss, clippy::suboptimal_flops) // synthetic series in the tests
)]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    const EPS: f64 = 1e-12;

    /// A profile whose columns hold `columns[c][row]` (`None` = empty cell), every row in the rank sample.
    fn numbers(columns: &[Vec<Option<f64>>]) -> ValueProfile {
        let rows = columns.iter().map(Vec::len).max().unwrap_or(0);
        let mut p = ValueProfile::new(columns.len(), 1);
        for i in 0..rows {
            let cells: Vec<Option<Value>> = columns
                .iter()
                .map(|c| c.get(i).copied().flatten().map(|x| json!(x)))
                .collect();
            p.observe_row(cells.iter().map(Option::as_ref));
        }
        p
    }

    fn series(n: usize, f: impl Fn(f64) -> f64) -> Vec<Option<f64>> {
        (0..n).map(|i| Some(f(i as f64))).collect()
    }

    /// 30 values of one column and its shape.
    fn shape_of(values: Vec<Option<f64>>) -> Option<Shape> {
        shape(&numbers(&[values]), 0)
    }

    #[test]
    fn precise_shapes_are_years_epoch_seconds_and_degrees() {
        assert_eq!(shape_of(series(30, |i| 1990.0 + i)), Some(Shape::Year));
        assert_eq!(
            shape_of(series(30, |i| 1_500_000_000.0 + i * 86_400.0)),
            Some(Shape::EpochSeconds)
        );
        assert_eq!(
            shape_of(series(30, |i| i * 6.5 - 90.25)),
            Some(Shape::Degrees)
        );
    }

    #[test]
    fn generic_ranges_and_constants_have_no_shape() {
        assert_eq!(shape_of(series(30, |i| i)), None, "small integers");
        assert_eq!(shape_of(series(30, |i| i * 1.5)), None, "positive floats");
        assert_eq!(
            shape_of(series(30, |i| 1700.0 + i)),
            None,
            "integers below 1800"
        );
        assert_eq!(
            shape_of(series(30, |i| 2090.0 + i)),
            None,
            "integers above 2100"
        );
        assert_eq!(
            shape_of(series(30, |i| i * 12.3 - 100.3)),
            None,
            "floats beyond 180"
        );
        assert_eq!(shape_of(series(30, |_| 1999.0)), None, "constant");
        assert_eq!(
            shape_of(series(10, |i| 1990.0 + i)),
            None,
            "too few cells to be numeric"
        );
    }

    #[test]
    fn a_text_column_has_no_shape() {
        let mut p = ValueProfile::new(1, 1);
        for i in 0..30 {
            p.observe_row([Some(&json!(format!("2000-{i}")))]);
        }
        assert_eq!(shape(&p, 0), None);
    }

    #[test]
    fn tied_values_share_their_average_rank_and_missing_ones_rank_lowest() {
        assert_eq!(
            average_ranks(&[10.0, 20.0, 20.0, 30.0]),
            [1.0, 2.5, 2.5, 4.0]
        );
        assert_eq!(average_ranks(&[f64::NAN, 5.0, f64::NAN]), [1.5, 3.0, 1.5]);
        assert!(average_ranks(&[]).is_empty());
    }

    #[test]
    fn pearson_on_ranks_is_spearman() {
        // ranks 1..5 against 2, 1, 4, 3, 5: rho = 1 - 6 * (1 + 1 + 1 + 1 + 0) / 120 = 0.8
        let a = [1.0, 2.0, 3.0, 4.0, 5.0];
        let b = [2.0, 1.0, 4.0, 3.0, 5.0];
        assert!((pearson_on(&a, &b, &[0, 1, 2, 3, 4]) - 0.8).abs() < EPS);
        assert!(
            (pearson_on(&a, &b, &[0, 2, 4]) - pearson_on(&a, &b, &[4, 0, 2])).abs() < EPS,
            "row order is irrelevant"
        );
        assert!(
            pearson_on(&a, &[3.0; 5], &[0, 1, 2, 3, 4]).abs() < EPS,
            "a constant has no correlation"
        );
    }

    #[test]
    fn monotone_columns_correlate_perfectly_whatever_their_scale() {
        let x = series(60, |i| i);
        let cubed = series(60, |i| i * i * i + 5.0);
        let reversed = series(60, |i| -i);
        let noisy = series(60, |i| ((i * 7.0) % 13.0) * ((i * 3.0) % 11.0));
        // rho about 0.76 with x: correlated, but under the 0.9 bar
        let wobbly = series(60, |i| i + 20.0 * (i * 1.7).sin());
        let pairs = rank_correlations(&numbers(&[x, cubed, reversed, noisy, wobbly]));
        let rho = |a, b| pairs.iter().find(|p| (p.a, p.b) == (a, b)).map(|p| p.rho);
        assert!((rho(0, 1).unwrap() - 1.0).abs() < EPS);
        assert!(
            (rho(0, 2).unwrap() + 1.0).abs() < EPS,
            "an exact inverse gives -1"
        );
        assert!(
            pairs.iter().all(|p| p.a < 3 && p.b < 3),
            "uncorrelated or weakly correlated columns make no pair: {pairs:?}"
        );
        assert!(pairs.iter().all(|p| p.rows == 60 && p.a < p.b));
    }

    #[test]
    fn a_pair_needs_thirty_rows_where_both_are_numbers() {
        // x on rows 0..50, y on rows 20..70: 30 rows in common
        let x: Vec<Option<f64>> = (0..70).map(|i| (i < 50).then_some(f64::from(i))).collect();
        let y = |from: i32| -> Vec<Option<f64>> {
            (0..70)
                .map(|i| (i >= from).then_some(f64::from(i)))
                .collect()
        };
        assert_eq!(rank_correlations(&numbers(&[x.clone(), y(20)])).len(), 1);
        assert!(
            rank_correlations(&numbers(&[x, y(21)])).is_empty(),
            "29 rows in common"
        );
    }

    #[test]
    fn text_columns_are_left_out_and_the_result_is_deterministic() {
        let mut p = ValueProfile::new(3, 1);
        for i in 0..40 {
            p.observe_row([
                Some(&json!(i)),
                Some(&json!(format!("t{i}"))),
                Some(&json!(i * 2)),
            ]);
        }
        let pairs = rank_correlations(&p);
        assert_eq!(
            pairs.iter().map(|q| (q.a, q.b)).collect::<Vec<_>>(),
            [(0, 2)]
        );
        assert_eq!(rank_correlations(&p), pairs);
    }
}
