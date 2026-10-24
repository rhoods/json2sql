//! Lexical grouping of the columns of a wide table from their stems (#78, T1b).
//!
//! Each stem becomes a vector of weighted character trigrams (TF-IDF); columns are clustered by average
//! linkage (UPGMA) and the dendrogram is cut at a similarity threshold. Columns left out of every group are
//! then clustered among themselves, weights recomputed on them alone, with a smaller minimum size: this
//! recovers small themes (three `POP_*` columns) that the first minimum size dropped. The arithmetic follows
//! the #78 spike operation by operation (no fused multiply-add), so that groups can be compared with it.
//!
//! Fonctions :
//! - struct `LexicalParams` — seuils du regroupement (coupe de similarité, tailles minimales).
//! - fn `LexicalParams::default` — valeurs de départ issues des spikes de #78.
//! - struct `Sparse` — vecteur creux de trigrammes pondérés, normalisé.
//! - struct `Merge` — une fusion du dendrogramme (similarité, groupe conservé, groupe absorbé).
//! - fn `lexical_groups` — groupes de colonnes (indices) à partir des radicaux : passe 1, puis colonnes seules.
//! - fn `cluster` — regroupe une liste de radicaux : vecteurs, similarités, liaison moyenne, coupe.
//! - fn `trigrams` — trigrammes de caractères d'un mot entouré de `#`.
//! - fn `term_counts` — trigrammes d'un radical avec leur nombre d'occurrences, dans l'ordre d'apparition.
//! - fn `tfidf_vectors` — vecteur TF-IDF normalisé de chaque radical.
//! - fn `weigh` — pondère par l'idf puis normalise les comptes d'un radical.
//! - fn `dot` — produit scalaire de deux vecteurs creux (cosinus, car normalisés).
//! - fn `similarity_matrix` — matrice n×n des similarités, à plat.
//! - fn `best_pair` — paire de groupes la plus proche (la première en cas d'égalité).
//! - fn `average_linkage` — fusions successives par liaison moyenne, jusqu'à passer sous un seuil.
//! - fn `find` — racine d'un élément dans la forêt des fusions.
//! - fn `groups_from_merges` — composantes des fusions retenues, d'au moins la taille minimale.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::error::{J2sError, Result};

/// Memory bound: the similarity matrix holds `n * n` floats (`PostgreSQL` stops at 1600 columns anyway).
pub const MAX_COLUMNS: usize = 2_000;

/// Thresholds of the lexical grouping.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LexicalParams {
    /// Two clusters merge while their average similarity is at least this.
    pub cut: f64,
    /// Smallest group kept by the first pass.
    pub min_group: usize,
    /// Smallest group kept when the left-out columns are clustered among themselves.
    pub min_loner_group: usize,
}

impl Default for LexicalParams {
    fn default() -> Self {
        Self {
            cut: 0.20,
            min_group: 4,
            min_loner_group: 3,
        }
    }
}

/// Normalized sparse vector of weighted trigrams.
struct Sparse {
    /// `(feature, weight)` in order of first occurrence in the stem.
    terms: Vec<(usize, f64)>,
    by_feature: HashMap<usize, f64>,
}

/// One merge of the dendrogram: the cluster `absorbed` joins `kept` (the smaller index) at `similarity`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Merge {
    similarity: f64,
    kept: usize,
    absorbed: usize,
}

/// Groups of column indices (into `stems`), each sorted, the groups sorted by their first member.
#[allow(dead_code)] // shortcut: the binary re-includes `schema` and nothing calls this until the proposal command (T1e); drop then.
pub fn lexical_groups(stems: &[Vec<String>], params: &LexicalParams) -> Result<Vec<Vec<usize>>> {
    if stems.len() > MAX_COLUMNS {
        return Err(J2sError::InvalidInput(format!(
            "lexical grouping handles at most {MAX_COLUMNS} columns, got {}",
            stems.len()
        )));
    }
    let all: Vec<&[String]> = stems.iter().map(Vec::as_slice).collect();
    let mut groups = cluster(&all, params.cut, params.min_group);
    let grouped: BTreeSet<usize> = groups.iter().flatten().copied().collect();
    let loners: Vec<usize> = (0..stems.len()).filter(|i| !grouped.contains(i)).collect();
    let loner_stems: Vec<&[String]> = loners.iter().map(|&i| all[i]).collect();
    for group in cluster(&loner_stems, params.cut, params.min_loner_group) {
        groups.push(group.into_iter().map(|k| loners[k]).collect());
    }
    groups.sort_by_key(|g| g[0]);
    Ok(groups)
}

/// Clusters `stems` and keeps the groups of at least `min_size` columns (never fewer than two).
fn cluster(stems: &[&[String]], cut: f64, min_size: usize) -> Vec<Vec<usize>> {
    let n = stems.len();
    if n < 2 {
        return Vec::new();
    }
    let sim = similarity_matrix(&tfidf_vectors(stems));
    groups_from_merges(&average_linkage(sim, n, cut), n, min_size)
}

/// Character trigrams of `token` surrounded by `#` (`pop` gives `#po`, `pop`, `op#`).
fn trigrams(token: &str) -> Vec<String> {
    let padded: Vec<char> = std::iter::once('#')
        .chain(token.chars())
        .chain(std::iter::once('#'))
        .collect();
    padded.windows(3).map(|w| w.iter().collect()).collect()
}

/// Trigrams of a stem with their number of occurrences, in order of first occurrence; `ids` numbers the
/// trigrams across the whole table.
fn term_counts(stem: &[String], ids: &mut HashMap<String, usize>) -> Vec<(usize, f64)> {
    let mut terms: Vec<(usize, f64)> = Vec::new();
    for gram in stem.iter().flat_map(|token| trigrams(token)) {
        let next = ids.len();
        let id = *ids.entry(gram).or_insert(next);
        match terms.iter_mut().find(|t| t.0 == id) {
            Some(term) => term.1 += 1.0,
            None => terms.push((id, 1.0)),
        }
    }
    terms
}

/// TF-IDF vector of every stem, `idf = ln((1 + n) / (1 + df)) + 1`, normalized to unit length.
#[allow(clippy::cast_precision_loss)] // column counts are far below 2^52
fn tfidf_vectors(stems: &[&[String]]) -> Vec<Sparse> {
    let mut ids: HashMap<String, usize> = HashMap::new();
    let counts: Vec<Vec<(usize, f64)>> = stems
        .iter()
        .map(|stem| term_counts(stem, &mut ids))
        .collect();
    let mut df = vec![0_usize; ids.len()];
    for &(feature, _) in counts.iter().flatten() {
        df[feature] += 1;
    }
    let n = stems.len() as f64;
    counts
        .into_iter()
        .map(|terms| weigh(terms, &df, n))
        .collect()
}

/// Weights `terms` (feature, count) by the idf, then scales them to unit length (an empty vector stays empty).
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)] // no fused multiply-add: same rounding as the spike
fn weigh(terms: Vec<(usize, f64)>, df: &[usize], n: f64) -> Sparse {
    let weighted: Vec<(usize, f64)> = terms
        .into_iter()
        .map(|(feature, count)| {
            (
                feature,
                count * (((1.0 + n) / (1.0 + df[feature] as f64)).ln() + 1.0),
            )
        })
        .collect();
    let norm = weighted.iter().fold(0.0, |acc, &(_, x)| acc + x * x).sqrt();
    let norm = if norm > 0.0 { norm } else { 1.0 };
    let terms: Vec<(usize, f64)> = weighted
        .into_iter()
        .map(|(feature, x)| (feature, x / norm))
        .collect();
    let by_feature = terms.iter().copied().collect();
    Sparse { terms, by_feature }
}

/// Dot product of two sparse vectors, summed over the shorter one (cosine, since both have unit length).
#[allow(clippy::suboptimal_flops)] // no fused multiply-add: same rounding as the spike
fn dot(a: &Sparse, b: &Sparse) -> f64 {
    let (short, long) = if a.terms.len() > b.terms.len() {
        (b, a)
    } else {
        (a, b)
    };
    short.terms.iter().fold(0.0, |acc, &(feature, x)| {
        acc + x * long.by_feature.get(&feature).copied().unwrap_or(0.0)
    })
}

/// Flat `n * n` matrix of the cosine similarity between every pair of vectors (diagonal unused).
fn similarity_matrix(vectors: &[Sparse]) -> Vec<f64> {
    let n = vectors.len();
    let mut sim = vec![0.0; n * n];
    for i in 0..n {
        for j in i + 1..n {
            let s = dot(&vectors[i], &vectors[j]);
            sim[i * n + j] = s;
            sim[j * n + i] = s;
        }
    }
    sim
}

/// The closest pair of live clusters: the highest similarity, the first pair in column order on a tie.
fn best_pair(sim: &[f64], n: usize, live: &[usize]) -> (f64, usize, usize) {
    let (mut best, mut kept, mut absorbed) = (-1.0, live[0], live[1]);
    for (pos, &i) in live.iter().enumerate() {
        for &j in &live[pos + 1..] {
            let s = sim[i * n + j];
            if s > best {
                (best, kept, absorbed) = (s, i, j);
            }
        }
    }
    (best, kept, absorbed)
}

/// Average-linkage merges of `n` clusters from their flat similarity matrix, in decreasing similarity,
/// until the best one falls below `stop_below`.
// shortcut: O(n³) scan of every live pair at each merge, fine up to `MAX_COLUMNS`; switch to a nearest-neighbour chain beyond.
#[allow(clippy::cast_precision_loss, clippy::suboptimal_flops)] // sizes far below 2^52; no fused multiply-add: same rounding as the spike
fn average_linkage(mut sim: Vec<f64>, n: usize, stop_below: f64) -> Vec<Merge> {
    let mut live: Vec<usize> = (0..n).collect();
    let mut size = vec![1_usize; n];
    let mut merges = Vec::new();
    while live.len() > 1 {
        let (similarity, kept, absorbed) = best_pair(&sim, n, &live);
        if similarity < stop_below {
            break;
        }
        let (kept_size, absorbed_size) = (size[kept] as f64, size[absorbed] as f64);
        for &k in live.iter().filter(|&&k| k != kept && k != absorbed) {
            let average = (sim[kept * n + k] * kept_size + sim[absorbed * n + k] * absorbed_size)
                / (kept_size + absorbed_size);
            sim[kept * n + k] = average;
            sim[k * n + kept] = average;
        }
        live.retain(|&k| k != absorbed);
        size[kept] += size[absorbed];
        merges.push(Merge {
            similarity,
            kept,
            absorbed,
        });
    }
    merges
}

/// Root of `x` in the merge forest, flattening the path on the way.
fn find(parent: &mut [usize], x: usize) -> usize {
    let mut root = x;
    while parent[root] != root {
        root = parent[root];
    }
    let mut cur = x;
    while parent[cur] != root {
        cur = std::mem::replace(&mut parent[cur], root);
    }
    root
}

/// Connected components of the merges, kept when they hold at least `min_size` columns (never fewer than two).
fn groups_from_merges(merges: &[Merge], n: usize, min_size: usize) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..n).collect();
    for m in merges {
        let (kept_root, absorbed_root) = (find(&mut parent, m.kept), find(&mut parent, m.absorbed));
        parent[absorbed_root] = kept_root;
    }
    let mut by_root: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for x in 0..n {
        by_root.entry(find(&mut parent, x)).or_default().push(x);
    }
    by_root
        .into_values()
        .filter(|g| g.len() >= min_size.max(2))
        .collect()
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    const EPS: f64 = 1e-12;

    /// Stems from readable names: `kiwi_red` gives `["kiwi", "red"]`.
    fn stems(names: &[&str]) -> Vec<Vec<String>> {
        names
            .iter()
            .map(|n| n.split('_').map(str::to_string).collect())
            .collect()
    }

    fn refs(stems: &[Vec<String>]) -> Vec<&[String]> {
        stems.iter().map(Vec::as_slice).collect()
    }

    /// Flat symmetric `n * n` matrix from `(i, j, similarity)` triples; every other pair is 0.
    fn matrix(n: usize, pairs: &[(usize, usize, f64)]) -> Vec<f64> {
        let mut m = vec![0.0; n * n];
        for &(i, j, s) in pairs {
            m[i * n + j] = s;
            m[j * n + i] = s;
        }
        m
    }

    /// Two themes of four columns (`kiwi_*`, `zulu_*`) sharing no trigram, plus two unrelated names.
    fn two_themes() -> Vec<Vec<String>> {
        stems(&[
            "kiwi_red",
            "kiwi_blue",
            "kiwi_green",
            "kiwi_gold",
            "zulu_aaa",
            "zulu_bbb",
            "zulu_ccc",
            "zulu_ddd",
            "qqq",
            "www",
        ])
    }

    #[test]
    fn trigrams_pad_the_token_with_hashes() {
        assert_eq!(trigrams("pop"), ["#po", "pop", "op#"]);
        assert_eq!(trigrams("a"), ["#a#"]);
        assert!(trigrams("").is_empty());
    }

    #[test]
    fn a_lone_token_gets_equal_unit_weights() {
        // "ab" and "cd" in a two-column table: every trigram is in one column, weights equal, norm 1
        let s = stems(&["ab", "cd"]);
        let v = tfidf_vectors(&refs(&s));
        assert_eq!(v[0].terms.len(), 2);
        assert!(v[0]
            .terms
            .iter()
            .all(|&(_, w)| (w - std::f64::consts::FRAC_1_SQRT_2).abs() < EPS));
        assert!((dot(&v[0], &v[0]) - 1.0).abs() < EPS);
        assert!(dot(&v[0], &v[1]).abs() < EPS, "no trigram in common");
    }

    #[test]
    fn identical_stems_are_fully_similar() {
        let s = stems(&["kiwi_red", "kiwi_red", "zulu_aaa"]);
        let v = tfidf_vectors(&refs(&s));
        assert!((dot(&v[0], &v[1]) - 1.0).abs() < EPS);
        assert!(dot(&v[0], &v[2]).abs() < EPS);
    }

    #[test]
    fn an_empty_stem_has_an_empty_vector() {
        let s = vec![vec![], vec!["kiwi".to_string()]];
        let v = tfidf_vectors(&refs(&s));
        assert!(v[0].terms.is_empty());
        assert!(dot(&v[0], &v[1]).abs() < EPS);
    }

    #[test]
    fn average_linkage_merges_the_closest_pair_then_averages() {
        let m = matrix(
            4,
            &[
                (0, 1, 0.9),
                (2, 3, 0.8),
                (0, 2, 0.2),
                (0, 3, 0.4),
                (1, 2, 0.3),
                (1, 3, 0.1),
            ],
        );
        let merges = average_linkage(m, 4, 0.0);
        let got: Vec<(usize, usize)> = merges.iter().map(|m| (m.kept, m.absorbed)).collect();
        assert_eq!(got, [(0, 1), (2, 3), (0, 2)]);
        // last merge: mean of the four cross similarities
        assert!((merges[2].similarity - 0.25).abs() < EPS);
    }

    #[test]
    fn average_linkage_weights_clusters_by_their_size() {
        // {0,1} (two columns) is 0.2 from 3, and 2 is 0.3 from 3: joined to 2 first, the cluster of three
        // is (0.2 * 2 + 0.3) / 3 from 3, not the plain mean of 0.2 and 0.3
        let m = matrix(
            4,
            &[
                (0, 1, 0.9),
                (0, 2, 0.5),
                (1, 2, 0.5),
                (0, 3, 0.2),
                (1, 3, 0.2),
                (2, 3, 0.3),
            ],
        );
        let merges = average_linkage(m, 4, 0.0);
        assert_eq!((merges[1].kept, merges[1].absorbed), (0, 2));
        assert!(
            (merges[2].similarity - 0.7 / 3.0).abs() < EPS,
            "{}",
            merges[2].similarity
        );
    }

    #[test]
    fn a_group_always_has_at_least_two_columns() {
        assert!(groups_from_merges(&[], 3, 0).is_empty());
        assert!(groups_from_merges(&[], 3, 1).is_empty());
    }

    #[test]
    fn a_tie_is_broken_by_column_order() {
        let m = matrix(3, &[(0, 1, 0.5), (0, 2, 0.5), (1, 2, 0.5)]);
        let merges = average_linkage(m, 3, 0.0);
        assert_eq!((merges[0].kept, merges[0].absorbed), (0, 1));
    }

    #[test]
    fn linkage_stops_below_the_threshold() {
        let m = matrix(
            4,
            &[
                (0, 1, 0.9),
                (2, 3, 0.8),
                (0, 2, 0.2),
                (0, 3, 0.4),
                (1, 2, 0.3),
                (1, 3, 0.1),
            ],
        );
        assert_eq!(average_linkage(m.clone(), 4, 0.5).len(), 2);
        assert_eq!(
            average_linkage(m, 4, 0.25).len(),
            3,
            "a merge exactly at the threshold is made"
        );
    }

    #[test]
    fn groups_follow_the_merges_and_the_minimum_size() {
        let m = matrix(
            4,
            &[
                (0, 1, 0.9),
                (2, 3, 0.8),
                (0, 2, 0.2),
                (0, 3, 0.4),
                (1, 2, 0.3),
                (1, 3, 0.1),
            ],
        );
        let merges = average_linkage(m, 4, 0.3);
        assert_eq!(groups_from_merges(&merges, 4, 2), [vec![0, 1], vec![2, 3]]);
        assert!(groups_from_merges(&merges, 4, 3).is_empty());
    }

    #[test]
    fn two_themes_give_two_groups_and_leave_the_rest_out() {
        let got = lexical_groups(&two_themes(), &LexicalParams::default()).unwrap();
        assert_eq!(got, [vec![0, 1, 2, 3], vec![4, 5, 6, 7]]);
    }

    #[test]
    fn left_out_columns_are_grouped_among_themselves_with_a_smaller_minimum() {
        // a theme of four, and a theme of three that the first minimum size drops
        let mut s = two_themes();
        s.truncate(4);
        s.extend(stems(&["moon_one", "moon_two", "moon_six", "qqq", "www"]));
        let got = lexical_groups(&s, &LexicalParams::default()).unwrap();
        assert_eq!(got, [vec![0, 1, 2, 3], vec![4, 5, 6]]);
        let strict = LexicalParams {
            min_loner_group: 4,
            ..LexicalParams::default()
        };
        assert_eq!(lexical_groups(&s, &strict).unwrap(), [vec![0, 1, 2, 3]]);
    }

    #[test]
    fn groups_are_sorted_by_first_member_across_both_passes() {
        // the theme of three comes first in the table but is only found by the second pass
        let mut s = stems(&["moon_one", "moon_two", "moon_six"]);
        s.extend(two_themes().into_iter().take(4));
        let got = lexical_groups(&s, &LexicalParams::default()).unwrap();
        assert_eq!(got, [vec![0, 1, 2], vec![3, 4, 5, 6]]);
    }

    #[test]
    fn groups_are_disjoint_sorted_and_the_result_is_deterministic() {
        let s = two_themes();
        let got = lexical_groups(&s, &LexicalParams::default()).unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for g in &got {
            assert!(g.windows(2).all(|w| w[0] < w[1]), "members sorted");
            assert!(g.iter().all(|&i| seen.insert(i)), "no column in two groups");
        }
        assert!(
            got.windows(2).all(|w| w[0][0] < w[1][0]),
            "groups sorted by first member"
        );
        assert_eq!(lexical_groups(&s, &LexicalParams::default()).unwrap(), got);
    }

    #[test]
    fn empty_stems_are_never_grouped() {
        let mut s = two_themes();
        s.extend(vec![vec![], vec![], vec![], vec![]]);
        let got = lexical_groups(&s, &LexicalParams::default()).unwrap();
        assert!(got.iter().flatten().all(|&i| i < 8));
    }

    #[test]
    fn fewer_than_two_columns_give_no_group() {
        assert!(lexical_groups(&[], &LexicalParams::default())
            .unwrap()
            .is_empty());
        assert!(
            lexical_groups(&stems(&["kiwi_red"]), &LexicalParams::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn too_many_columns_are_refused_before_any_allocation() {
        let many = vec![Vec::new(); MAX_COLUMNS + 1];
        let err = lexical_groups(&many, &LexicalParams::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains(&MAX_COLUMNS.to_string()), "{err}");
    }
}
