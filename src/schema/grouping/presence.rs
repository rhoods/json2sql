//! Co-presence of columns in a sample of rows (#78, T1c): the "taux de correspondance".
//!
//! Columns of one JSON object that are filled together hint that they belong together. Raw co-presence is
//! polluted by the richness of the record (a well-filled product has everything filled): on Open Food Facts
//! 8 % of the pairs of columns with different prefixes reach a phi of 0.5. The rows are therefore cut in five strata by
//! richness (the number of present values over the table's columns) and phi is computed inside each stratum
//! then averaged, weighted by stratum size. Only columns present in 2 % to 98 % of the rows carry a signal.
//! The result stays signed: a strong negative phi between columns of the same domain flags an alias.
//! The counts are exact integers; the float steps follow the #78 spike in the same order.
//!
//! Fonctions :
//! - struct `Presence` — présence (valeur non vide) de chaque colonne dans les lignes échantillonnées.
//! - fn `Presence::new` — présence vide pour un nombre de colonnes donné.
//! - fn `Presence::push_row` — ajoute une ligne : une présence par colonne, les colonnes manquantes sont absentes.
//! - fn `Presence::rows` — nombre de lignes échantillonnées.
//! - fn `Presence::present_count` — nombre de lignes où une colonne est présente.
//! - fn `Presence::informative_columns` — colonnes présentes dans 2 % à 98 % des lignes.
//! - fn `Presence::phi_matrix` — φ ajusté par richesse entre toutes les colonnes informatives.
//! - struct `PhiMatrix` — φ ajusté signé entre colonnes informatives.
//! - fn `PhiMatrix::informative` — colonnes informatives, dans l'ordre.
//! - fn `PhiMatrix::get` — φ ajusté d'une paire, `None` si l'une des colonnes n'est pas informative.
//! - fn `strata` — strate de richesse de chaque ligne (rang de richesse, cinq strates de même taille).
//! - fn `stratum_masks` — un masque de lignes par strate.
//! - fn `counts_per_stratum` — nombre de lignes de chaque strate où une colonne est présente.
//! - fn `phi_adjusted` — φ ajusté de deux colonnes à partir de leurs bitsets.

use std::collections::HashMap;

/// Columns present in fewer rows than this (percent) are too rare to say anything.
const INFORMATIVE_MIN_PERCENT: usize = 2;
/// Columns present in more rows than this (percent) are filled by everyone.
const INFORMATIVE_MAX_PERCENT: usize = 98;
/// Number of richness strata.
const STRATA: usize = 5;

/// Presence of each column in the sampled rows, one bit per row.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Presence {
    columns: usize,
    /// `bits[column]`: bit `row % 64` of word `row / 64` is set when the column is present in `row`.
    #[serde(with = "super::packed::word_lists")]
    bits: Vec<Vec<u64>>,
    /// Number of present columns of each row.
    #[serde(with = "super::packed::half_words")]
    richness: Vec<u32>,
}

impl Presence {
    /// Empty presence for a table of `columns` columns.
    pub fn new(columns: usize) -> Self {
        Self {
            columns,
            bits: vec![Vec::new(); columns],
            richness: Vec::new(),
        }
    }

    /// Adds a row: one flag per column, in column order. Missing flags count as absent, extra ones are ignored.
    pub fn push_row(&mut self, present: impl IntoIterator<Item = bool>) {
        let row = self.richness.len();
        let (word, bit) = (row / 64, row % 64);
        if bit == 0 {
            self.bits.iter_mut().for_each(|column| column.push(0));
        }
        let mut count = 0;
        for (column, flag) in present.into_iter().take(self.columns).enumerate() {
            if flag {
                self.bits[column][word] |= 1 << bit;
                count += 1;
            }
        }
        self.richness.push(count);
    }

    /// Number of sampled rows.
    pub const fn rows(&self) -> usize {
        self.richness.len()
    }

    /// Number of rows where `column` is present.
    pub fn present_count(&self, column: usize) -> usize {
        self.bits[column]
            .iter()
            .map(|word| word.count_ones() as usize)
            .sum()
    }

    /// Columns present in 2 % to 98 % of the rows, in column order; none without rows.
    pub fn informative_columns(&self) -> Vec<usize> {
        let rows = self.rows();
        (0..self.columns)
            .filter(|&column| {
                let count = self.present_count(column);
                rows > 0
                    && count * 100 >= INFORMATIVE_MIN_PERCENT * rows
                    && count * 100 <= INFORMATIVE_MAX_PERCENT * rows
            })
            .collect()
    }

    /// Adjusted phi between every pair of informative columns.
    pub fn phi_matrix(&self) -> PhiMatrix {
        let informative = self.informative_columns();
        let masks = stratum_masks(&strata(&self.richness));
        let sizes: [usize; STRATA] =
            std::array::from_fn(|s| masks[s].iter().map(|w| w.count_ones() as usize).sum());
        let counts: Vec<[usize; STRATA]> = informative
            .iter()
            .map(|&column| counts_per_stratum(&self.bits[column], &masks))
            .collect();
        let k = informative.len();
        let mut values = vec![0.0; k * k];
        for x in 0..k {
            values[x * k + x] = 1.0;
            for y in x + 1..k {
                let (left, right) = (&self.bits[informative[x]], &self.bits[informative[y]]);
                let phi = phi_adjusted(left, right, &masks, &sizes, [&counts[x], &counts[y]]);
                values[x * k + y] = phi;
                values[y * k + x] = phi;
            }
        }
        let position = informative
            .iter()
            .enumerate()
            .map(|(x, &column)| (column, x))
            .collect();
        PhiMatrix {
            informative,
            values,
            position,
        }
    }
}

/// Adjusted phi between the informative columns, signed.
#[derive(Debug, Clone)]
pub struct PhiMatrix {
    informative: Vec<usize>,
    /// Flat `k * k` matrix over `informative`, `k = informative.len()`.
    values: Vec<f64>,
    /// Column to its position in `informative`.
    position: HashMap<usize, usize>,
}

impl PhiMatrix {
    /// The informative columns, in column order.
    pub fn informative(&self) -> &[usize] {
        &self.informative
    }

    /// Adjusted phi of two columns, `None` when either is not informative.
    pub fn get(&self, a: usize, b: usize) -> Option<f64> {
        let (x, y) = (*self.position.get(&a)?, *self.position.get(&b)?);
        Some(self.values[x * self.informative.len() + y])
    }
}

/// Stratum of every row: rows ranked by richness (ties in row order), the ranks cut in five equal parts.
fn strata(richness: &[u32]) -> Vec<usize> {
    let rows = richness.len();
    let mut order: Vec<usize> = (0..rows).collect();
    order.sort_by_key(|&row| richness[row]);
    let mut stratum = vec![0; rows];
    for (rank, &row) in order.iter().enumerate() {
        stratum[row] = (rank * STRATA / rows).min(STRATA - 1);
    }
    stratum
}

/// One bitset of rows per stratum.
fn stratum_masks(strata: &[usize]) -> Vec<Vec<u64>> {
    let mut masks = vec![vec![0_u64; strata.len().div_ceil(64)]; STRATA];
    for (row, &stratum) in strata.iter().enumerate() {
        masks[stratum][row / 64] |= 1 << (row % 64);
    }
    masks
}

/// For each stratum, the number of its rows where the column is present.
fn counts_per_stratum(bits: &[u64], masks: &[Vec<u64>]) -> [usize; STRATA] {
    std::array::from_fn(|s| {
        bits.iter()
            .zip(&masks[s])
            .map(|(word, mask)| (word & mask).count_ones() as usize)
            .sum()
    })
}

/// Phi of two columns inside each stratum, averaged weighted by stratum size. A stratum where either column
/// is constant has no phi and does not count; no stratum left gives 0. `counts` are the per-stratum
/// presence counts of the two columns.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::suboptimal_flops
)] // exact counts; no fused multiply-add, as in the spike
fn phi_adjusted(
    left: &[u64],
    right: &[u64],
    masks: &[Vec<u64>],
    sizes: &[usize; STRATA],
    counts: [&[usize; STRATA]; 2],
) -> f64 {
    let mut both = [0_usize; STRATA];
    for (word, (l, r)) in left.iter().zip(right).enumerate() {
        let joint = l & r;
        for (stratum, mask) in masks.iter().enumerate() {
            both[stratum] += (joint & mask[word]).count_ones() as usize;
        }
    }
    let (mut weighted, mut weight) = (0.0_f64, 0.0_f64);
    for stratum in 0..STRATA {
        let (size, left_n, right_n) = (
            sizes[stratum] as u128,
            counts[0][stratum] as u128,
            counts[1][stratum] as u128,
        );
        let variance = left_n * (size - left_n) * right_n * (size - right_n);
        if variance == 0 {
            continue;
        }
        let covariance = (size * both[stratum] as u128) as i128 - (left_n * right_n) as i128;
        weighted += size as f64 * (covariance as f64 / (variance as f64).sqrt());
        weight += size as f64;
    }
    if weight > 0.0 {
        weighted / weight
    } else {
        0.0
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;

    /// 43 rows x 6 columns (strata of 9, 9, 8, 9 and 8 rows) from a fixed LCG, column 1 mostly copied from column 0; expected values come
    /// from the #78 spike's `phi_adj` run in Python on the same matrix.
    fn lcg_matrix() -> Presence {
        let density = [4_u64, 4, 3, 5, 2, 7];
        let mut seed = 7_u64;
        let mut presence = Presence::new(6);
        for i in 0..43 {
            let mut row = [false; 6];
            for (j, cell) in row.iter_mut().enumerate() {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                *cell = (seed >> 33) % 8 < density[j];
            }
            if i % 4 != 0 {
                row[1] = row[0];
            }
            presence.push_row(row);
        }
        presence
    }

    /// Rows in pairs of equal richness; inside each pair the two columns follow `(a, b)` of the first row
    /// and `(!a, !b)` of the second, the fillers compensate so that both rows of a pair have the same richness.
    fn paired(first: (bool, bool)) -> Presence {
        let mut presence = Presence::new(2 + 10);
        for pair in 0..5 {
            let base = pair + 3; // richness of both rows of the pair
            for (a, b) in [first, (!first.0, !first.1)] {
                let own = usize::from(a) + usize::from(b);
                let mut row = vec![a, b];
                row.extend((0..10).map(|k| k < base - own));
                presence.push_row(row);
            }
        }
        presence
    }

    #[test]
    fn strata_cut_the_rows_by_richness_rank_with_ties_by_row_order() {
        assert_eq!(
            strata(&[3, 1, 2, 5, 4, 0, 9, 8, 7, 6]),
            [1, 0, 1, 2, 2, 0, 4, 4, 3, 3]
        );
        // seven rows: strata of 2, 1, 2, 1, 1 rows
        assert_eq!(strata(&[0, 0, 0, 0, 0, 0, 0]), [0, 0, 1, 2, 2, 3, 4]);
        assert_eq!(strata(&[]).len(), 0);
    }

    #[test]
    fn a_column_is_informative_from_2_to_98_percent_of_the_rows() {
        let counts = [100, 1, 2, 98, 99, 50];
        let mut presence = Presence::new(counts.len());
        for row in 0..100 {
            presence.push_row(counts.iter().map(|&c| row < c));
        }
        assert_eq!(presence.rows(), 100);
        assert_eq!(presence.present_count(3), 98);
        assert_eq!(presence.informative_columns(), [2, 3, 5]);
    }

    #[test]
    fn missing_cells_of_a_short_row_are_absent() {
        let mut presence = Presence::new(3);
        presence.push_row([true]);
        presence.push_row([true, true, true]);
        assert_eq!([0, 1, 2].map(|c| presence.present_count(c)), [2, 1, 1]);
    }

    #[test]
    fn columns_that_follow_each_other_inside_every_stratum_have_phi_one() {
        let phi = paired((true, true)).phi_matrix();
        assert!((phi.get(0, 1).unwrap() - 1.0).abs() < EPS);
    }

    #[test]
    fn columns_that_exclude_each_other_have_a_negative_phi() {
        let phi = paired((true, false)).phi_matrix();
        assert!(
            (phi.get(0, 1).unwrap() + 1.0).abs() < EPS,
            "alias detection needs the sign"
        );
        assert_eq!(phi.get(1, 0), phi.get(0, 1), "symmetric");
    }

    #[test]
    fn strata_where_a_column_is_constant_do_not_dilute_phi() {
        // pairs 0-2 follow each other (phi 1); in pairs 3-4 both columns are filled in both rows: no phi there
        let mut presence = Presence::new(2 + 10);
        for pair in 0..5 {
            let base = pair + 3;
            let rows = if pair < 3 {
                [(true, true), (false, false)]
            } else {
                [(true, true), (true, true)]
            };
            for (a, b) in rows {
                let own = usize::from(a) + usize::from(b);
                let mut row = vec![a, b];
                row.extend((0..10).map(|k| k < base - own));
                presence.push_row(row);
            }
        }
        assert!((presence.phi_matrix().get(0, 1).unwrap() - 1.0).abs() < EPS);
    }

    #[test]
    fn co_presence_explained_by_richness_alone_is_removed() {
        // A and B are filled exactly in the four richest rows (whole strata): a naive phi is 1, the adjusted one
        // has nothing left
        let mut presence = Presence::new(2 + 10);
        for row in 0..10 {
            let rich = row >= 6;
            let mut cells = vec![rich, rich];
            cells.extend((0..10).map(|k| k < row));
            presence.push_row(cells);
        }
        assert!(presence.phi_matrix().get(0, 1).unwrap().abs() < EPS);
    }

    #[test]
    fn phi_matches_the_spike_on_a_fixed_matrix() {
        let phi = lcg_matrix().phi_matrix();
        for (a, b, want) in [
            (0, 1, 0.497_920_388_544_845_27),
            (0, 2, -0.421_398_395_400_350_1),
            (3, 4, -0.141_881_813_063_570_64),
            (2, 3, -0.168_693_050_865_239_78),
            (4, 5, -0.037_724_708_619_703_38),
        ] {
            let got = phi.get(a, b).unwrap();
            assert!(
                (got - want).abs() < EPS,
                "({a}, {b}): {got} instead of {want}"
            );
        }
    }

    /// Stratum of every row of a plain matrix of flags, from the definition (rank by richness, ties by row).
    fn naive_strata(rows: &[Vec<bool>]) -> Vec<usize> {
        let richness: Vec<usize> = rows
            .iter()
            .map(|r| r.iter().filter(|&&p| p).count())
            .collect();
        let mut order: Vec<usize> = (0..rows.len()).collect();
        order.sort_by_key(|&i| richness[i]);
        let mut stratum = vec![0; rows.len()];
        for (rank, &i) in order.iter().enumerate() {
            stratum[i] = (rank * 5 / rows.len()).min(4);
        }
        stratum
    }

    /// Adjusted phi on a plain matrix of flags, straight from the definition: no bitset, no mask.
    #[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)] // exact small counts, plain arithmetic
    fn naive_phi(rows: &[Vec<bool>], left: usize, right: usize) -> f64 {
        let stratum = naive_strata(rows);
        let (mut weighted, mut weight) = (0.0, 0.0);
        for wanted in 0..5 {
            let inside: Vec<&Vec<bool>> = rows
                .iter()
                .zip(&stratum)
                .filter(|(_, &s)| s == wanted)
                .map(|(r, _)| r)
                .collect();
            let size = inside.len() as f64;
            let count = |present: &dyn Fn(&Vec<bool>) -> bool| {
                inside.iter().filter(|r| present(r)).count() as f64
            };
            let (left_n, right_n, both) = (
                count(&|r| r[left]),
                count(&|r| r[right]),
                count(&|r| r[left] && r[right]),
            );
            let spread = (left_n * (size - left_n) * right_n * (size - right_n)).sqrt();
            if spread > 0.0 {
                weighted += size * ((size * both - left_n * right_n) / spread);
                weight += size;
            }
        }
        if weight > 0.0 {
            weighted / weight
        } else {
            0.0
        }
    }

    #[test]
    fn rows_beyond_the_first_64_are_counted_like_the_others() {
        // 200 rows (four 64-bit words), columns partly tied to each other
        let mut seed = 99_u64;
        let mut rows: Vec<Vec<bool>> = Vec::new();
        for _ in 0..200 {
            let mut row = Vec::new();
            for density in [4_u64, 3, 5, 2, 6, 4, 1, 7] {
                seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                row.push((seed >> 33) % 8 < density);
            }
            row[1] = row[0] || row[1];
            rows.push(row);
        }
        let mut presence = Presence::new(8);
        for row in &rows {
            presence.push_row(row.iter().copied());
        }
        let phi = presence.phi_matrix();
        assert!(phi.informative().len() >= 6, "{:?}", phi.informative());
        for &a in phi.informative() {
            for &b in phi.informative() {
                if a < b {
                    let (got, want) = (phi.get(a, b).unwrap(), naive_phi(&rows, a, b));
                    assert!(
                        (got - want).abs() < EPS,
                        "({a}, {b}): {got} instead of {want}"
                    );
                }
            }
        }
    }

    #[test]
    fn only_informative_columns_have_a_phi() {
        let mut presence = Presence::new(3);
        for row in 0..50 {
            presence.push_row([true, row % 2 == 0, row % 5 == 0]); // column 0 is always filled
        }
        let phi = presence.phi_matrix();
        assert_eq!(phi.informative(), [1, 2]);
        assert!(phi.get(0, 1).is_none() && phi.get(1, 0).is_none());
        assert!(phi.get(1, 2).is_some());
    }

    #[test]
    fn no_rows_or_no_variation_give_an_empty_matrix() {
        assert_eq!(Presence::new(4).phi_matrix().informative().len(), 0);
        let mut flat = Presence::new(2);
        for _ in 0..10 {
            flat.push_row([true, false]);
        }
        assert_eq!(flat.phi_matrix().informative().len(), 0);
    }
}
