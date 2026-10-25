//! Where the columns left in a table might go (#78, T2): for each of them, the two groups whose centre is nearest to
//! its name in a static embedding.
//!
//! The centre of a group is the normalized sum of the vectors of its members' names (the stems the names signal
//! reads, joined by spaces). The score is the cosine, and only a positive one counts: a column with no known word, or
//! no group in the same direction, gets no suggestion. Only the confirmed groups are candidates; a fusion to confirm
//! is not. It is a hint for the person who reads the report, never a decision.
//!
//! Fonctions :
//! - fn `placements` — les groupes les plus proches de chaque colonne laissée dans la table.
//! - fn `centroid` — centre normalisé de plusieurs vecteurs ; `None` s'il est nul.
//! - fn `nearest` — les groupes dont le centre est le plus proche d'un vecteur, du meilleur au moins bon.

use super::embedding::StaticEmbedder;
use super::proposal::{Placement, Status, TableProposal};

/// How many groups are suggested for a column.
const NEAREST: usize = 2;

/// The nearest groups of every column that stays in the table, in column order; a column gets none when its name
/// has no known word or no confirmed group lies in its direction.
pub fn placements(proposal: &TableProposal, embedder: &StaticEmbedder) -> Vec<Placement> {
    let vectors: Vec<Vec<f64>> = proposal
        .stems
        .iter()
        .map(|stem| embedder.embed(&stem.join(" ")))
        .collect();
    let centres: Vec<(usize, Vec<f64>)> = proposal
        .groups
        .iter()
        .enumerate()
        .filter(|(_, group)| group.status == Status::Confirmed)
        .filter_map(|(index, group)| {
            centroid(group.columns.iter().map(|&column| &vectors[column])).map(|c| (index, c))
        })
        .collect();
    proposal
        .ungrouped
        .iter()
        .filter_map(|&column| {
            let nearest = nearest(&vectors[column], &centres);
            (!nearest.is_empty()).then_some(Placement { column, nearest })
        })
        .collect()
}

/// The normalized sum of `vectors`; `None` when it is zero.
fn centroid<'a>(vectors: impl Iterator<Item = &'a Vec<f64>>) -> Option<Vec<f64>> {
    let mut sum: Vec<f64> = Vec::new();
    for vector in vectors {
        sum.resize(vector.len(), 0.0);
        sum.iter_mut().zip(vector).for_each(|(s, x)| *s += x);
    }
    let norm = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
    (norm > 0.0).then(|| sum.into_iter().map(|x| x / norm).collect())
}

/// The groups (their index and cosine) whose centre is in the direction of `vector`, best first, at most
/// [`NEAREST`]; a tie goes to the group that comes first.
fn nearest(vector: &[f64], centres: &[(usize, Vec<f64>)]) -> Vec<(usize, f64)> {
    let mut scored: Vec<(usize, f64)> = centres
        .iter()
        .map(|(index, centre)| (*index, vector.iter().zip(centre).map(|(x, y)| x * y).sum()))
        .filter(|&(_, score)| score > 0.0)
        .collect();
    scored.sort_by(|x, y| y.1.total_cmp(&x.1).then(x.0.cmp(&y.0)));
    scored.truncate(NEAREST);
    scored
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::super::proposal::{ColumnName, DataStatus, Group};
    use super::*;

    const SQRT_HALF: f64 = std::f64::consts::FRAC_1_SQRT_2;

    fn toy() -> StaticEmbedder {
        StaticEmbedder::from_rows(&[
            ("alpha", &[1.0, 0.0, 0.0]),
            ("beta", &[0.0, 1.0, 0.0]),
            ("gamma", &[0.0, 0.0, 1.0]),
            ("delta", &[1.0, 1.0, 0.0]),
        ])
    }

    fn group(name: &str, status: Status, columns: Vec<usize>) -> Group {
        Group {
            name: name.to_string(),
            columns,
            status,
            evidence: Vec::new(),
            families: 1,
            replaces: Vec::new(),
        }
    }

    /// Twelve grouped columns (`g1` alpha, `g2` beta, `g3` gamma, four each), a fusion to confirm over `g1` and `g2`,
    /// then the columns that stay, each described by its stem.
    fn proposal(left: &[&[&str]]) -> TableProposal {
        let stems: Vec<Vec<String>> = ["alpha", "beta", "gamma"]
            .iter()
            .flat_map(|w| std::iter::repeat_n(vec![(*w).to_string()], 4))
            .chain(
                left.iter()
                    .map(|s| s.iter().map(|w| (*w).to_string()).collect()),
            )
            .collect();
        TableProposal {
            table: "t".to_string(),
            columns: (0..stems.len())
                .map(|i| ColumnName {
                    name: format!("c{i}"),
                    original: format!("c{i}"),
                })
                .collect(),
            groups: vec![
                group("g1", Status::Confirmed, (0..4).collect()),
                group("g2", Status::Confirmed, (4..8).collect()),
                group("g3", Status::Confirmed, (8..12).collect()),
                group("fusion", Status::ToConfirm, (0..8).collect()),
            ],
            ungrouped: (12..stems.len()).collect(),
            stems,
            families: Vec::new(),
            data: DataStatus::Off("test".to_string()),
            placements: Vec::new(),
        }
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn a_column_gets_the_two_nearest_groups_with_their_cosine() {
        let found = placements(&proposal(&[&["alpha", "beta"]]), &toy());
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].column, 12);
        let got: Vec<usize> = found[0].nearest.iter().map(|n| n.0).collect();
        assert_eq!(
            got,
            [0, 1],
            "g1 and g2 tie, the first group comes first; g3 is orthogonal"
        );
        assert!(
            found[0].nearest.iter().all(|n| close(n.1, SQRT_HALF)),
            "{:?}",
            found[0].nearest
        );
    }

    #[test]
    fn the_better_group_comes_first_and_a_zero_cosine_is_not_a_suggestion() {
        let found = placements(
            &proposal(&[&["delta", "gamma", "gamma"], &["gamma", "gamma", "gamma"]]),
            &toy(),
        );
        // delta + 2 gamma = (1,1,2): g3 first (2/sqrt6), then g1 and g2 (1/sqrt6 each); only two are kept
        assert_eq!(
            found[0].nearest.iter().map(|n| n.0).collect::<Vec<_>>(),
            [2, 0]
        );
        assert!(
            close(found[0].nearest[0].1, 2.0 / 6.0_f64.sqrt()),
            "{:?}",
            found[0].nearest
        );
        assert!(
            close(found[0].nearest[1].1, 1.0 / 6.0_f64.sqrt()),
            "{:?}",
            found[0].nearest
        );
        // a pure gamma column is orthogonal to g1 and g2: one suggestion only
        assert_eq!(found[1].nearest, [(2, 1.0)]);
    }

    #[test]
    fn a_column_with_no_known_word_has_no_suggestion() {
        let found = placements(&proposal(&[&["zzz"], &[], &["alpha"]]), &toy());
        assert_eq!(found.iter().map(|p| p.column).collect::<Vec<_>>(), [14]);
    }

    #[test]
    fn a_fusion_to_confirm_is_never_suggested() {
        let found = placements(&proposal(&[&["alpha"], &["beta"]]), &toy());
        assert!(
            found.iter().flat_map(|p| &p.nearest).all(|n| n.0 != 3),
            "{found:?}"
        );
    }

    #[test]
    fn the_centre_of_a_group_is_the_normalized_sum_of_its_members() {
        let (a, b) = (vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]);
        let centre = centroid([&a, &b, &b].into_iter()).unwrap();
        let norm = 5.0_f64.sqrt();
        assert!(
            close(centre[0], 1.0 / norm) && close(centre[1], 2.0 / norm) && close(centre[2], 0.0),
            "{centre:?}"
        );
        assert!(centroid([&vec![0.0; 3], &vec![0.0; 3]].into_iter()).is_none());
        assert!(centroid(std::iter::empty()).is_none());
    }

    #[test]
    fn a_group_with_a_stronger_pull_wins_over_a_looser_one() {
        // g1 = alpha x4 (centre alpha); a column "alpha beta" is halfway between alpha and beta: a group of two
        // alphas and two betas has the same direction as the column, so it scores 1 against 0.707
        let mut p = proposal(&[&["alpha", "beta"]]);
        p.stems[0] = vec!["alpha".to_string()];
        p.stems[1] = vec!["alpha".to_string()];
        p.stems[2] = vec!["beta".to_string()];
        p.stems[3] = vec!["beta".to_string()];
        let found = placements(&p, &toy());
        assert_eq!(found[0].nearest[0].0, 0);
        assert!(close(found[0].nearest[0].1, 1.0), "{:?}", found[0].nearest);
    }
}
