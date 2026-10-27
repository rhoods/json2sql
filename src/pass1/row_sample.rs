//! Bounded sample of the root rows, kept by the thread that reads the file during Pass 1 (#91, phase 2).
//!
//! The data signals of the column groups need row values, which Pass 1 otherwise forgets. The reading thread keeps
//! the first row that ends past each multiple of `stride` bytes, `stride` being the file size over the row budget:
//! the sample spans the whole file (a sorted file is not read by its first rows only), and as only the reading
//! thread picks rows, the sample is the same whatever the number of Pass 1 workers. When the rows kept exceed the
//! row or the byte budget, one row in two is dropped and the stride doubles, so the sample keeps spanning the file
//! within a fixed memory.
//!
//! Fonctions :
//! - struct `SampledRow` — une ligne racine gardée : clé de l'enveloppe et octets JSON.
//! - struct `RowSampler` — choisit les lignes au fil de la lecture, dans les budgets de lignes et d'octets.
//! - fn `RowSampler::new` — échantillonneur aux budgets par défaut pour un fichier d'une taille donnée.
//! - fn `RowSampler::with_budgets` — échantillonneur aux budgets donnés.
//! - fn `RowSampler::wants` — vrai si la ligne qui se termine à cette position doit être gardée.
//! - fn `RowSampler::keep` — garde une ligne et tient les budgets.
//! - fn `RowSampler::into_rows` — les lignes gardées, dans l'ordre du fichier.
//! - fn `RowSampler::halve` — garde une ligne sur deux et double le pas.

/// Most rows kept: the budget of `--suggest-groups`.
pub const ROW_BUDGET: usize = 20_000;
/// Most bytes of rows kept at once.
pub const BYTE_BUDGET: usize = 256 << 20;

/// One root row kept for the data signals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampledRow {
    /// The wrapper key the row was read under (`None` for a JSON-lines or array file).
    pub key: Option<String>,
    /// The row as JSON.
    pub bytes: Vec<u8>,
}

/// Picks the rows to keep as the file is read.
#[derive(Debug)]
pub struct RowSampler {
    stride: u64,
    /// Rows ending at or past this position are kept.
    next: u64,
    rows: Vec<SampledRow>,
    bytes: usize,
    row_budget: usize,
    byte_budget: usize,
}

impl RowSampler {
    /// A sampler for a file of `total_bytes` with the default budgets.
    #[must_use]
    pub fn new(total_bytes: u64) -> Self {
        Self::with_budgets(total_bytes, ROW_BUDGET, BYTE_BUDGET)
    }

    /// A sampler for a file of `total_bytes` keeping at most `row_budget` rows and `byte_budget` bytes.
    #[must_use]
    pub fn with_budgets(total_bytes: u64, row_budget: usize, byte_budget: usize) -> Self {
        let row_budget = row_budget.max(1);
        let stride = (total_bytes / u64::try_from(row_budget).unwrap_or(u64::MAX)).max(1);
        Self {
            stride,
            next: stride,
            rows: Vec::new(),
            bytes: 0,
            row_budget,
            byte_budget,
        }
    }

    /// True when the row ending at `end` (bytes read so far) is to be kept.
    #[must_use]
    pub const fn wants(&self, end: u64) -> bool {
        end >= self.next
    }

    /// Keeps the row ending at `end`; past a budget, one row in two goes and the stride doubles.
    pub fn keep(&mut self, end: u64, key: Option<String>, bytes: Vec<u8>) {
        self.bytes += bytes.len();
        self.rows.push(SampledRow { key, bytes });
        self.next = (end / self.stride + 1) * self.stride;
        while self.rows.len() > self.row_budget
            || (self.bytes > self.byte_budget && self.rows.len() > 1)
        {
            self.halve();
        }
    }

    /// The rows kept, in the order of the file.
    #[must_use]
    pub fn into_rows(self) -> Vec<SampledRow> {
        self.rows
    }

    /// Keeps the rows of even rank and doubles the stride.
    fn halve(&mut self) {
        let rows = std::mem::take(&mut self.rows);
        self.rows = rows.into_iter().step_by(2).collect();
        self.bytes = self.rows.iter().map(|r| r.bytes.len()).sum();
        self.stride *= 2;
        self.next = self.next.div_ceil(self.stride) * self.stride;
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    /// Feeds `rows` rows of `size` bytes each to `sampler`; returns the numbers of the rows kept.
    fn feed(sampler: &mut RowSampler, rows: u64, size: u64) -> Vec<u64> {
        for n in 0..rows {
            let end = (n + 1) * size;
            if sampler.wants(end) {
                sampler.keep(end, None, n.to_string().into_bytes());
            }
        }
        std::mem::replace(sampler, RowSampler::new(1))
            .into_rows()
            .iter()
            .map(|r| String::from_utf8(r.bytes.clone()).unwrap().parse().unwrap())
            .collect()
    }

    #[test]
    fn a_small_file_is_kept_whole() {
        let mut s = RowSampler::with_budgets(1_000, 100, 1 << 20);
        assert_eq!(feed(&mut s, 10, 100), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn rows_are_taken_at_a_regular_stride_over_the_whole_file() {
        // 1000 rows of 100 bytes, 100 rows wanted: one row in ten, from the start to the end
        let mut s = RowSampler::with_budgets(100_000, 100, 1 << 20);
        let kept = feed(&mut s, 1_000, 100);
        assert_eq!(kept.len(), 100);
        assert!(kept.windows(2).all(|w| w[1] - w[0] == 10), "{kept:?}");
        assert_eq!(
            (kept[0], *kept.last().unwrap()),
            (9, 999),
            "one row per tenth of the file, to its end"
        );
    }

    #[test]
    fn past_the_row_budget_one_row_in_two_goes_and_the_sample_still_spans_the_file() {
        // the size is unknown (too small): far more rows than wanted come
        let mut s = RowSampler::with_budgets(1_000, 100, 1 << 20);
        let kept = feed(&mut s, 10_000, 100);
        assert!(kept.len() <= 100 && kept.len() >= 50, "{}", kept.len());
        assert!(
            *kept.last().unwrap() > 9_000,
            "the end of the file is sampled: {kept:?}"
        );
    }

    #[test]
    fn the_byte_budget_bounds_the_memory() {
        let mut s = RowSampler::with_budgets(100_000, 1_000, 40);
        for n in 0..1_000_u64 {
            let end = (n + 1) * 100;
            if s.wants(end) {
                s.keep(end, None, vec![b'x'; 10]);
            }
        }
        assert!(s.into_rows().len() * 10 <= 40);
    }

    #[test]
    fn the_same_reading_gives_the_same_sample() {
        let (mut a, mut b) = (
            RowSampler::with_budgets(5_000, 37, 1 << 20),
            RowSampler::with_budgets(5_000, 37, 1 << 20),
        );
        assert_eq!(feed(&mut a, 3_000, 70), feed(&mut b, 3_000, 70));
    }
}
