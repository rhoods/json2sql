//! #78 T0 — test bench for the thematic column grouping: labeled datasets, scoring, "first word" baseline.
//!
//! Each `tests/fixtures/column_groups/*.toml` holds one dataset's columns and a PARTIAL ground truth:
//! `[[relation]]` = columns that must end up in the same group, `[[separate]]` = columns that must never
//! share one. A method is scored on groups of column names with `column_groups::score`; the reference to
//! beat is `column_groups::prefix_baseline`. A dataset with `role = "held_out"` is never used to tune
//! thresholds: `only_tuning` is the only way the tuning bench reads the fixtures.
//!
//! Print the table: `cargo test --test bench_column_groups report -- --nocapture`
//!
//! The presence signal needs row data, which is not in the repository: put `<dataset>.jsonl` files (the rows of
//! each tuning dataset) in a directory and run `J2S_BENCH_DATA=<dir> cargo test --test bench_column_groups
//! data_signals_report -- --nocapture`. Without the variable that report does nothing.
//!
//! The static embeddings (T2) need a Model2Vec folder with a WordPiece tokenizer, which is not in the repository
//! either (`potion-base-8M`, 30 MB): `J2S_BENCH_MODEL=<dir> cargo test --test bench_column_groups embeddings`.
#![allow(clippy::disallowed_methods)]

mod column_groups;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use column_groups::{first_word, fixtures, only_tuning, prefix_baseline, score, Dataset, Role};
use json2sql::io::reader::JsonReader;
use json2sql::schema::grouping::embedding::StaticEmbedder;
use json2sql::schema::grouping::lexical::{lexical_groups, LexicalParams};
use json2sql::schema::grouping::names::normalize;
use json2sql::schema::grouping::numeric::{rank_correlations, shape, Shape};
use json2sql::schema::grouping::placement::placements;
use json2sql::schema::grouping::presence::PhiMatrix;
use json2sql::schema::grouping::proposal::{
    propose, ColumnName, DataSignals, Status, TableProposal,
};
use json2sql::schema::grouping::sample::{
    sample_tables, SampleSource, SampleTarget, TableSample, DEFAULT_BUDGET,
};
use json2sql::schema::grouping::shared::shared_value_pairs;
use json2sql::schema::grouping::values::ValueProfile;

fn cols(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| (*n).to_string()).collect()
}

fn groups(spec: &[&[&str]]) -> Vec<Vec<String>> {
    spec.iter().map(|g| cols(g)).collect()
}

/// 10 columns and one clique relation `{a1, b1, c1}`.
const TINY: &str = r#"
name = "tiny"
role = "tuning"
source = "unit test"
columns = ["a1", "a2", "a3", "a4", "b1", "b2", "c1", "d1", "d2", "e1"]

[[relation]]
name = "abc"
left = ["a1", "b1", "c1"]
"#;

fn tiny(extra: &str) -> Result<Dataset, String> {
    Dataset::from_toml(&format!("{TINY}{extra}"))
}

// ---------------------------------------------------------------- baseline

#[test]
fn first_word_splits_snake_camel_and_digits() {
    for (name, want) in [
        ("nutrition_score", "nutrition"),
        ("strIngredient12", "str"),
        ("ADM0_A3_US", "adm"),
        ("POP1950", "pop"),
        ("idMeal", "id"),
        ("_keywords", "keywords"),
        ("FCLASS_TLC", "fclass"),
        ("NAMEPAR", "namepar"),
        ("100g_energy", "g"),
        ("2024", "2024"),
        ("___", "___"),
    ] {
        assert_eq!(first_word(name), want, "{name}");
    }
}

#[test]
fn prefix_baseline_keeps_groups_of_four_or_more() {
    let names = cols(&["a_1", "a_2", "a_3", "a_4", "b_1", "b_2", "c_1"]);
    assert_eq!(
        prefix_baseline(&names),
        groups(&[&["a_1", "a_2", "a_3", "a_4"]])
    );
}

// ---------------------------------------------------------------- scoring

#[test]
fn score_reports_groups_coverage_and_largest() {
    let s = score(
        &tiny("").unwrap(),
        &groups(&[&["a1", "a2", "a3", "a4"], &["b1", "b2", "d1"]]),
    )
    .unwrap();
    assert_eq!(
        (s.columns, s.groups, s.coverage, s.largest, s.capped),
        (10, 2, 7, 4, 0)
    );
}

#[test]
fn score_counts_linked_pairs_found_together() {
    // pairs of {a1, b1, c1}: only (a1, b1) share a group
    let s = score(&tiny("").unwrap(), &groups(&[&["a1", "b1", "d1", "d2"]])).unwrap();
    assert_eq!((s.linked[0].hit, s.linked[0].total), (1, 3));
}

#[test]
fn score_ignores_pairs_with_a_column_outside_every_group() {
    let s = score(&tiny("").unwrap(), &groups(&[&["a1", "a2", "a3", "a4"]])).unwrap();
    assert_eq!(s.linked[0].hit, 0);
}

#[test]
fn score_drops_groups_over_half_of_the_columns() {
    let ds = tiny("").unwrap();
    // 6 of 10 columns: no split at all, so it covers nothing and its inner pairs do not count
    let s = score(&ds, &groups(&[&["a1", "a2", "a3", "a4", "b1", "b2"]])).unwrap();
    assert_eq!((s.groups, s.coverage, s.largest, s.capped), (0, 0, 6, 1));
    assert_eq!(s.linked[0].hit, 0);
    // exactly half is still a group
    let s = score(&ds, &groups(&[&["a1", "a2", "a3", "a4", "b1"]])).unwrap();
    assert_eq!(
        (s.groups, s.coverage, s.capped, s.linked[0].hit),
        (1, 5, 0, 1)
    );
}

#[test]
fn score_counts_separated_pairs_that_share_a_group() {
    let ds = tiny("[[separate]]\nname = \"a vs de\"\nleft = [\"a1\"]\nright = [\"d1\", \"e1\"]\n")
        .unwrap();
    // (a1, d1) share a group, (a1, e1) do not
    let s = score(&ds, &groups(&[&["a1", "d1", "b2", "b1"]])).unwrap();
    assert_eq!((s.separated[0].hit, s.separated[0].total), (1, 2));
}

#[test]
fn score_rejects_unknown_columns_and_columns_in_two_groups() {
    let ds = tiny("").unwrap();
    let err = score(&ds, &groups(&[&["a1", "zz"]])).unwrap_err();
    assert!(err.contains("zz"), "{err}");
    let err = score(&ds, &groups(&[&["a1", "a2"], &["a2", "a3"]])).unwrap_err();
    assert!(err.contains("a2"), "{err}");
}

// ---------------------------------------------------------------- ground truth loader

#[test]
fn relations_expand_to_unique_ordered_pairs() {
    let ds = tiny("[[relation]]\nname = \"bipartite\"\nleft = [\"a1\", \"a2\"]\nright = [\"d1\", \"d2\", \"e1\"]\n").unwrap();
    assert_eq!(ds.linked[0].pairs.len(), 3, "clique of 3 columns");
    assert_eq!(ds.linked[1].pairs.len(), 6, "2 x 3 columns");
    assert!(ds.linked.iter().flat_map(|r| &r.pairs).all(|(i, j)| i < j));
}

#[test]
fn loader_rejects_a_truth_that_cannot_be_trusted() {
    for (extra, want) in [
        (
            "[[relation]]\nname = \"typo\"\nleft = [\"a1\", \"a9\"]\n",
            "a9",
        ),
        (
            "[[relation]]\nname = \"overlap\"\nleft = [\"a1\", \"b2\"]\nright = [\"b2\", \"c1\"]\n",
            "both sides",
        ),
        (
            "[[relation]]\nname = \"alone\"\nleft = [\"a1\"]\n",
            "at least two",
        ),
        (
            "[[relation]]\nname = \"empty\"\nleft = [\"a1\"]\nright = []\n",
            "empty",
        ),
        (
            "[[relation]]\nname = \"twice\"\nleft = [\"a1\", \"a1\"]\n",
            "twice",
        ),
        (
            "[[relation]]\nname = \"key typo\"\nlefft = [\"a1\", \"a2\"]\n",
            "lefft",
        ),
        // (a1, b1) is also in the clique {a1, b1, c1}: a truth cannot link and separate the same pair
        (
            "[[separate]]\nname = \"a vs bc\"\nleft = [\"a1\"]\nright = [\"b1\", \"c1\"]\n",
            "both linked and separated",
        ),
    ] {
        let err = tiny(extra).unwrap_err();
        assert!(err.contains(want), "{extra:?} -> {err}");
    }
}

#[test]
fn loader_rejects_a_theme_no_group_could_ever_hold() {
    // 6 of 10 columns: over the cap the scorer applies to groups, so no valid grouping could find it
    let six =
        "[[relation]]\nname = \"big\"\nleft = [\"a1\", \"a2\", \"a3\", \"a4\", \"b1\", \"b2\"]\n";
    assert!(tiny(six).unwrap_err().contains("finer theme"));
    // both sides of a bipartite relation end up in the same group, so they count together
    let two_sides = "[[relation]]\nname = \"big\"\nleft = [\"a1\", \"a2\", \"a3\"]\nright = [\"b1\", \"b2\", \"c1\"]\n";
    assert!(tiny(two_sides).unwrap_err().contains("finer theme"));
    // exactly half is fine, and a `separate` relation may span more: it is not a group
    let half = "[[relation]]\nname = \"half\"\nleft = [\"a1\", \"a2\", \"a3\", \"a4\", \"b1\"]\n";
    assert!(tiny(half).is_ok());
    let wide = "[[separate]]\nname = \"wide\"\nleft = [\"a2\", \"a3\", \"a4\", \"b2\"]\nright = [\"d1\", \"d2\", \"e1\"]\n";
    assert!(tiny(wide).is_ok());
}

#[test]
fn loader_rejects_bad_dataset_headers() {
    let dup = TINY.replace("\"e1\"]", "\"e1\", \"a1\"]");
    assert!(Dataset::from_toml(&dup).unwrap_err().contains("duplicate"));
    let role = TINY.replace("tuning", "validation");
    assert!(Dataset::from_toml(&role)
        .unwrap_err()
        .contains("validation"));
    let unlabeled = TINY.split("[[relation]]").next().unwrap();
    assert!(Dataset::from_toml(unlabeled)
        .unwrap_err()
        .contains("[[relation]]"));
}

#[test]
fn rows_path_says_where_the_columns_are_below_a_root_row() {
    assert!(
        tiny("").unwrap().rows_path.is_empty(),
        "default: the columns are the root rows' keys"
    );
    let nested = TINY.replace(
        "source = \"unit test\"",
        "source = \"unit test\"\nrows_path = [\"properties\"]",
    );
    assert_eq!(
        Dataset::from_toml(&nested).unwrap().rows_path,
        ["properties"]
    );
    for ds in fixtures() {
        let want: &[&str] = if ds.name.starts_with("ne_") {
            &["properties"]
        } else {
            &[]
        };
        assert_eq!(ds.rows_path, want, "{}", ds.name);
    }
}

#[test]
fn held_out_sets_never_reach_the_tuning_bench() {
    let held = Dataset::from_toml(&TINY.replace("tuning", "held_out")).unwrap();
    assert_eq!(held.role, Role::HeldOut);
    let kept = only_tuning(vec![held, tiny("").unwrap()]);
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].role, Role::Tuning);
}

// ---------------------------------------------------------------- committed fixtures

struct Reference {
    name: &'static str,
    columns: usize,
    groups: usize,
    coverage: usize,
    largest: usize,
    capped: usize,
    /// per `[[relation]]`: (pairs the method puts together, pairs in the relation)
    linked: &'static [(usize, usize)],
    /// per `[[separate]]`: (pairs the method wrongly puts together, pairs in the relation)
    separated: &'static [(usize, usize)],
}

/// "First word, groups of 4+, no group over half the columns" on the committed fixtures. Computed on
/// 2026-10-24 by an independent Python oracle (regex tokenizer); the totals also pin each fixture's shape.
const PREFIX_REFERENCE: &[Reference] = &[
    Reference {
        name: "ne_countries",
        columns: 168,
        groups: 6,
        coverage: 117,
        largest: 38,
        capped: 0,
        linked: &[(0, 21), (0, 6), (0, 15), (3, 21)],
        separated: &[(0, 858)],
    },
    Reference {
        name: "ne_places",
        columns: 137,
        groups: 6,
        coverage: 107,
        largest: 33,
        capped: 0,
        linked: &[
            (190, 231),
            (56, 153),
            (3, 15),
            (0, 10),
            (0, 6),
            (0, 15),
            (0, 1),
        ],
        separated: &[(0, 858)],
    },
    Reference {
        name: "openfoodfacts",
        columns: 238,
        groups: 17,
        coverage: 145,
        largest: 32,
        capped: 0,
        linked: &[(0, 75), (0, 105), (0, 6), (0, 6), (0, 1)],
        separated: &[(0, 704), (0, 198)],
    },
    Reference {
        // the "str" prefix is shared by 52 of 54 columns: one giant group, scored as no split
        name: "themealdb",
        columns: 54,
        groups: 0,
        coverage: 0,
        largest: 52,
        capped: 1,
        linked: &[(0, 190), (0, 190), (0, 10), (0, 6), (0, 3)],
        separated: &[(0, 200)],
    },
];

fn assert_reference(want: &Reference, got: &column_groups::Score) {
    let shape = (
        got.columns,
        got.groups,
        got.coverage,
        got.largest,
        got.capped,
    );
    let expected = (
        want.columns,
        want.groups,
        want.coverage,
        want.largest,
        want.capped,
    );
    assert_eq!(shape, expected, "{}", want.name);
    let pairs = |rs: &[column_groups::RelationScore]| {
        rs.iter().map(|r| (r.hit, r.total)).collect::<Vec<_>>()
    };
    assert_eq!(pairs(&got.linked), want.linked, "{} linked", want.name);
    assert_eq!(
        pairs(&got.separated),
        want.separated,
        "{} separated",
        want.name
    );
}

fn dataset<'a>(all: &'a [Dataset], name: &str) -> &'a Dataset {
    all.iter()
        .find(|d| d.name == name)
        .unwrap_or_else(|| panic!("fixture {name} missing"))
}

#[test]
fn prefix_baseline_matches_the_reference_on_every_fixture() {
    let all = fixtures();
    assert_eq!(
        all.len(),
        PREFIX_REFERENCE.len(),
        "a fixture was added or removed: update PREFIX_REFERENCE"
    );
    for want in PREFIX_REFERENCE {
        let ds = dataset(&all, want.name);
        assert_reference(want, &score(ds, &prefix_baseline(&ds.columns)).unwrap());
    }
}

#[test]
fn every_fixture_is_labeled_and_names_its_source() {
    for ds in fixtures() {
        assert!(!ds.linked.is_empty(), "{} has no relation", ds.name);
        assert!(
            ds.source.len() > 10,
            "{} does not say where it comes from",
            ds.name
        );
    }
}

// ---------------------------------------------------------------- T1a: name normalization

struct StemReference {
    name: &'static str,
    format_tokens: &'static [&'static str],
    noise_words: &'static [&'static str],
    /// columns whose family key was removed
    keyed: usize,
    samples: &'static [(&'static str, &'static [&'static str])],
}

/// `schema::grouping::names::normalize` on the committed fixtures (original names). Computed on 2026-10-24
/// by replaying the spike code (`generic_v2.build_v2`) on the same columns; the Rust output matched it on all
/// 597 columns, stem by stem.
const STEM_REFERENCE: &[StemReference] = &[
    StemReference {
        // `A2`/`A3` codes lose their digit and leave a lone `a` behind many prefixes: a format token
        name: "ne_countries",
        format_tokens: &["a"],
        noise_words: &[],
        keyed: 94,
        samples: &[
            ("FCLASS_IT", &["fclass"]),
            ("NAME_FR", &["name"]),
            ("ADM0_A3_US", &["adm"]),
            ("NAME_LONG", &["name", "long"]),
            ("POP_EST", &["pop", "est"]),
            ("ISO_A3", &["iso"]),
        ],
    },
    StemReference {
        name: "ne_places",
        format_tokens: &[],
        noise_words: &[],
        keyed: 59,
        samples: &[
            ("FCLASS_US", &["fclass"]),
            ("NAME_EN", &["name"]),
            ("POP1950", &["pop"]),
            ("MAX_AREAKM", &["max", "areakm"]),
            ("NAMEPAR", &["namepar"]),
        ],
    },
    StemReference {
        name: "openfoodfacts",
        format_tokens: &["debug", "hierarchy", "lc", "n", "prev", "tags"],
        noise_words: &[],
        keyed: 0,
        samples: &[
            ("ingredients_text_fr", &["ingredients", "text", "fr"]),
            ("additives_tags", &["additives"]),
            ("_keywords", &["keywords"]),
            ("last_modified_t", &["last", "modified", "t"]),
        ],
    },
    StemReference {
        // `str` is in 52 of 54 names; the numbering of strIngredient1..20 is dropped
        name: "themealdb",
        format_tokens: &[],
        noise_words: &["str"],
        keyed: 0,
        samples: &[
            ("strIngredient7", &["ingredient"]),
            ("strMeasure12", &["measure"]),
            ("strMealThumb", &["meal", "thumb"]),
            ("idMeal", &["id", "meal"]),
            ("strInstructions", &["instructions"]),
        ],
    },
];

fn strs(set: &std::collections::BTreeSet<String>) -> Vec<&str> {
    set.iter().map(String::as_str).collect()
}

#[test]
fn name_stems_match_the_reference_on_every_tuning_fixture() {
    // method output is read on tuning sets only: a held-out set must not be looked at while tuning
    let all = only_tuning(fixtures());
    assert_eq!(
        all.len(),
        STEM_REFERENCE.len(),
        "a tuning fixture was added or removed: update STEM_REFERENCE"
    );
    for want in STEM_REFERENCE {
        let ds = all
            .iter()
            .find(|d| d.name == want.name)
            .unwrap_or_else(|| panic!("fixture {} missing", want.name));
        let got = normalize(&ds.columns);
        assert_eq!(got.stems.len(), ds.columns.len(), "{}", want.name);
        assert_eq!(
            strs(&got.format_tokens),
            want.format_tokens,
            "{} format tokens",
            want.name
        );
        assert_eq!(
            strs(&got.noise_words),
            want.noise_words,
            "{} noise words",
            want.name
        );
        assert_eq!(
            got.keyed.iter().filter(|&&k| k).count(),
            want.keyed,
            "{} keyed columns",
            want.name
        );
        for (column, stem) in want.samples {
            let i = ds
                .columns
                .iter()
                .position(|c| c == column)
                .unwrap_or_else(|| panic!("{}: no column {column}", want.name));
            assert_eq!(got.stems[i], *stem, "{}: {column}", want.name);
        }
    }
}

// ---------------------------------------------------------------- T1b: lexical grouping

/// The lexical method end to end on one dataset: names, stems, trigram clusters, then the left-out columns.
fn lexical_method(ds: &Dataset) -> Vec<Vec<String>> {
    let stems = normalize(&ds.columns).stems;
    let groups = lexical_groups(&stems, &LexicalParams::default()).unwrap();
    groups
        .iter()
        .map(|g| g.iter().map(|&i| ds.columns[i].clone()).collect())
        .collect()
}

/// `lexical_method` on the tuning sets. Computed on 2026-10-24 in Python: pass 1 is the spike code replayed on
/// the verified stems; pass 2 (the left-out columns) is a lexical reimplementation of the spike's loner step,
/// which used embeddings. The Rust groups matched it member by member on all four datasets. A regression
/// guard, not an acceptance threshold: the group cap and the criterion against the prefix are still to be agreed.
const LEXICAL_REFERENCE: &[Reference] = &[
    Reference {
        name: "ne_countries",
        columns: 168,
        groups: 11,
        coverage: 136,
        largest: 38,
        capped: 0,
        linked: &[(3, 21), (3, 6), (10, 15), (3, 21)],
        separated: &[(0, 858)],
    },
    Reference {
        name: "ne_places",
        columns: 137,
        groups: 7,
        coverage: 113,
        largest: 36,
        capped: 0,
        linked: &[
            (191, 231),
            (40, 153),
            (10, 15),
            (0, 10),
            (0, 6),
            (0, 15),
            (0, 1),
        ],
        separated: &[(0, 858)],
    },
    Reference {
        name: "openfoodfacts",
        columns: 238,
        groups: 24,
        coverage: 196,
        largest: 33,
        capped: 0,
        linked: &[(15, 75), (105, 105), (0, 6), (0, 6), (0, 1)],
        separated: &[(0, 704), (0, 198)],
    },
    Reference {
        name: "themealdb",
        columns: 54,
        groups: 3,
        coverage: 44,
        largest: 20,
        capped: 0,
        linked: &[(190, 190), (190, 190), (0, 10), (0, 6), (3, 3)],
        separated: &[(0, 200)],
    },
];

#[test]
fn lexical_groups_match_the_reference_on_every_tuning_fixture() {
    // method output is read on tuning sets only: a held-out set must not be looked at while tuning
    let all = only_tuning(fixtures());
    assert_eq!(
        all.len(),
        LEXICAL_REFERENCE.len(),
        "a tuning fixture was added or removed: update LEXICAL_REFERENCE"
    );
    for want in LEXICAL_REFERENCE {
        let ds = dataset(&all, want.name);
        assert_reference(want, &score(ds, &lexical_method(ds)).unwrap());
    }
}

// ---------------------------------------------------------------- T1c: presence signal (local data only)

/// Directory holding `<dataset>.jsonl`, the rows of each tuning dataset (Natural Earth: one GeoJSON Feature per
/// line). Unset in CI, where the row data is not available.
const DATA_ENV: &str = "J2S_BENCH_DATA";

/// Rows of the root table in `file`: one pass over the raw bytes, nothing parsed.
fn count_rows(file: &Path) -> u64 {
    let (mut reader, _) = JsonReader::open(file).unwrap();
    std::iter::from_fn(|| reader.next_raw()).count() as u64
}

/// Presence and values of the dataset's columns in one regular-stride sample of the rows in `file`.
fn sample_of(ds: &Dataset, file: &Path) -> TableSample {
    let columns: Vec<Vec<String>> = ds.columns.iter().map(|c| vec![c.clone()]).collect();
    let source = SampleSource {
        input: file,
        root: &ds.name,
        root_rows: count_rows(file),
        limit: None,
        budget: DEFAULT_BUDGET,
    };
    let target = SampleTarget {
        path: &ds.rows_path,
        columns: &columns,
    };
    sample_tables(&source, &[target]).unwrap().remove(0)
}

/// Of a relation's pairs: how many have both columns informative, their mean phi, how many reach 0.3.
fn phi_stats(relation: &column_groups::Relation, phi: &PhiMatrix) -> (usize, f64, usize) {
    let values: Vec<f64> = relation
        .pairs
        .iter()
        .filter_map(|&(i, j)| phi.get(i, j))
        .collect();
    let mean = values.iter().fold(0.0, |sum, v| sum + v) / values.len().max(1) as f64;
    (
        values.len(),
        mean,
        values.iter().filter(|&&v| v >= 0.3).count(),
    )
}

/// Shared-value similarity from which a pair counts as backed (the spike's cut for grouping by values).
const VALUES_CUT: f64 = 0.5;

/// The column pairs `(a, b)`, `a < b`, that each data signal backs.
struct Backing {
    values: HashSet<(usize, usize)>,
    rank: HashSet<(usize, usize)>,
    shape: HashSet<(usize, usize)>,
}

fn backing(values: &ValueProfile) -> Backing {
    let shapes: Vec<Option<Shape>> = (0..values.columns()).map(|c| shape(values, c)).collect();
    Backing {
        values: shared_value_pairs(values)
            .into_iter()
            .filter(|p| p.score >= VALUES_CUT)
            .map(|p| (p.a, p.b))
            .collect(),
        rank: rank_correlations(values)
            .into_iter()
            .map(|p| (p.a, p.b))
            .collect(),
        shape: (0..shapes.len())
            .flat_map(|a| (a + 1..shapes.len()).map(move |b| (a, b)))
            .filter(|&(a, b)| shapes[a].is_some() && shapes[a] == shapes[b])
            .collect(),
    }
}

/// How many of a relation's pairs a set of backed pairs contains.
fn backed(relation: &column_groups::Relation, pairs: &HashSet<(usize, usize)>) -> usize {
    relation.pairs.iter().filter(|p| pairs.contains(p)).count()
}

/// Every pair `(a, b)`, `a < b`, of columns the lexical method puts in one group.
fn lexical_pairs(ds: &Dataset) -> HashSet<(usize, usize)> {
    let stems = normalize(&ds.columns).stems;
    let groups = lexical_groups(&stems, &LexicalParams::default()).unwrap();
    groups
        .iter()
        .flat_map(|g| {
            g.iter()
                .enumerate()
                .flat_map(|(i, &a)| g[i + 1..].iter().map(move |&b| (a, b)))
        })
        .collect()
}

/// Columns that appear in any relation of the truth, linked or separated.
fn labeled_columns(ds: &Dataset) -> HashSet<usize> {
    ds.linked
        .iter()
        .chain(&ds.separated)
        .flat_map(|r| r.pairs.iter().flat_map(|&(a, b)| [a, b]))
        .collect()
}

/// Up to `n` best-scored pairs between columns of different first words, none of them in the truth, as text.
fn top_unlabeled(ds: &Dataset, scored: Vec<(usize, usize, f64)>, n: usize) -> String {
    let labeled = labeled_columns(ds);
    let mut free: Vec<_> = scored
        .into_iter()
        .filter(|&(a, b, _)| !labeled.contains(&a) && !labeled.contains(&b))
        .filter(|&(a, b, _)| first_word(&ds.columns[a]) != first_word(&ds.columns[b]))
        .collect();
    free.sort_by(|x, y| {
        y.2.abs()
            .total_cmp(&x.2.abs())
            .then((x.0, x.1).cmp(&(y.0, y.1)))
    });
    free.iter()
        .take(n)
        .map(|&(a, b, v)| format!("{} ~ {} ({v:.2})", ds.columns[a], ds.columns[b]))
        .collect::<Vec<_>>()
        .join(" ; ")
}

/// Not an assertion: how well the data signals tell linked pairs from separated ones, on local data only.
#[test]
fn data_signals_report() {
    let Some(dir) = std::env::var_os(DATA_ENV).map(PathBuf::from) else {
        eprintln!("[SKIP] {DATA_ENV} is not set");
        return;
    };
    for ds in only_tuning(fixtures()) {
        let file = dir.join(format!("{}.jsonl", ds.name));
        if !file.exists() {
            eprintln!("[SKIP] {} not found", file.display());
            continue;
        }
        let TableSample { presence, values } = sample_of(&ds, &file);
        let support = backing(&values);
        let phi = presence.phi_matrix();
        let inf = phi.informative();
        let all: Vec<f64> = inf
            .iter()
            .enumerate()
            .flat_map(|(x, &a)| inf[x + 1..].iter().map(move |&b| (a, b)))
            .filter_map(|(a, b)| phi.get(a, b))
            .collect();
        let background =
            100.0 * all.iter().filter(|&&v| v >= 0.3).count() as f64 / all.len().max(1) as f64;
        println!(
            "== {} : {} rows sampled, {}/{} informative columns, {background:.1}% of their pairs reach phi 0.3",
            ds.name,
            presence.rows(),
            inf.len(),
            ds.columns.len()
        );
        let lexical = lexical_pairs(&ds);
        for (kind, relations) in [("linked   ", &ds.linked), ("separated", &ds.separated)] {
            let line = |text: &dyn Fn(&column_groups::Relation) -> String| {
                relations.iter().map(text).collect::<Vec<_>>().join(" | ")
            };
            println!(
                "   {kind} presence: {}",
                line(&|r| {
                    let (n, mean, strong) = phi_stats(r, &phi);
                    format!(
                        "{} {n}/{} pairs, mean {mean:+.2}, >=0.3: {strong}",
                        r.name,
                        r.pairs.len()
                    )
                })
            );
            println!(
                "   {kind} data    : {}",
                line(&|r| {
                    // for a linked relation only the pairs lexical leaves apart can be won by a data signal
                    let rest = column_groups::Relation {
                        name: r.name.clone(),
                        pairs: r
                            .pairs
                            .iter()
                            .copied()
                            .filter(|p| kind.starts_with("sep") || !lexical.contains(p))
                            .collect(),
                    };
                    format!(
                        "{}: {} pairs, {} to win; backed among those: values {}, rank {}, shape {}",
                        r.name,
                        r.pairs.len(),
                        rest.pairs.len(),
                        backed(&rest, &support.values),
                        backed(&rest, &support.rank),
                        backed(&rest, &support.shape)
                    )
                })
            );
        }
        let by_values = shared_value_pairs(&values)
            .into_iter()
            .filter(|p| p.score >= VALUES_CUT)
            .map(|p| (p.a, p.b, p.score))
            .collect();
        let by_rank = rank_correlations(&values)
            .into_iter()
            .map(|p| (p.a, p.b, p.rho))
            .collect();
        println!(
            "   unlabeled cross-prefix, values: {}",
            top_unlabeled(&ds, by_values, 5)
        );
        println!(
            "   unlabeled cross-prefix, rank  : {}",
            top_unlabeled(&ds, by_rank, 5)
        );
        let shapes: Vec<String> = [Shape::Year, Shape::EpochSeconds, Shape::Degrees]
            .iter()
            .map(|&wanted| {
                let names: Vec<&str> = (0..ds.columns.len())
                    .filter(|&c| shape(&values, c) == Some(wanted))
                    .map(|c| ds.columns[c].as_str())
                    .collect();
                format!("{wanted:?} [{}]", names.join(", "))
            })
            .collect();
        println!("   shapes: {}", shapes.join(" ; "));
    }
}

// ---------------------------------------------------------------- T1e: proposal (rule of two signals)

/// The proposal for a dataset, from the names alone (no row data: every fusion stays to confirm).
fn proposal_of(ds: &Dataset) -> TableProposal {
    proposal_with(ds, Err("bench: names only"))
}

/// The proposal for a dataset with the given data signals, or the reason they are off.
fn proposal_with(ds: &Dataset, data: Result<&DataSignals, &str>) -> TableProposal {
    let columns = ds
        .columns
        .iter()
        .map(|c| ColumnName {
            name: c.clone(),
            original: c.clone(),
        })
        .collect();
    propose(&ds.name, columns, data, &LexicalParams::default()).unwrap()
}

/// The groups a proposal applies, as column names: the confirmed ones, or, with `accept_fusions`, the fusions to
/// confirm in place of the families they replace.
fn applied(ds: &Dataset, proposal: &TableProposal, accept_fusions: bool) -> Vec<Vec<String>> {
    let replaced: HashSet<&str> = if accept_fusions {
        proposal
            .groups
            .iter()
            .flat_map(|g| g.replaces.iter().map(String::as_str))
            .collect()
    } else {
        HashSet::new()
    };
    proposal
        .groups
        .iter()
        .filter(|g| {
            if g.status == Status::Confirmed {
                !replaced.contains(g.name.as_str())
            } else {
                accept_fusions
            }
        })
        .map(|g| g.columns.iter().map(|&c| ds.columns[c].clone()).collect())
        .collect()
}

/// The proposal from names alone, fusions left to confirm, on the tuning sets: what a first run applies. Computed
/// 2026-10-24 by an independent Python rule (lexical groups split by the first word of the stem, families of
/// three or more columns kept), then the same scorer as the other references.
const PROPOSAL_REFERENCE: &[Reference] = &[
    Reference {
        name: "ne_countries",
        columns: 168,
        groups: 6,
        coverage: 114,
        largest: 36,
        capped: 0,
        linked: &[(0, 21), (0, 6), (0, 15), (3, 21)],
        separated: &[(0, 858)],
    },
    Reference {
        name: "ne_places",
        columns: 137,
        groups: 7,
        coverage: 97,
        largest: 33,
        capped: 0,
        linked: &[
            (190, 231),
            (12, 153),
            (3, 15),
            (0, 10),
            (0, 6),
            (0, 15),
            (0, 1),
        ],
        separated: &[(0, 858)],
    },
    Reference {
        name: "openfoodfacts",
        columns: 238,
        groups: 21,
        coverage: 145,
        largest: 29,
        capped: 0,
        linked: &[(0, 75), (0, 105), (0, 6), (0, 6), (0, 1)],
        separated: &[(0, 704), (0, 198)],
    },
    Reference {
        name: "themealdb",
        columns: 54,
        groups: 3,
        coverage: 43,
        largest: 20,
        capped: 0,
        linked: &[(190, 190), (190, 190), (0, 10), (0, 6), (1, 3)],
        separated: &[(0, 200)],
    },
];

#[test]
fn applying_every_fusion_gives_back_the_lexical_groups() {
    for ds in only_tuning(fixtures()) {
        let mut want = lexical_method(&ds);
        let mut got = applied(&ds, &proposal_of(&ds), true);
        want.sort();
        got.sort();
        assert_eq!(got, want, "{}", ds.name);
    }
}

#[test]
fn the_confirmed_proposal_never_joins_a_separated_pair_on_the_tuning_sets() {
    for ds in only_tuning(fixtures()) {
        let score = score(&ds, &applied(&ds, &proposal_of(&ds), false)).unwrap();
        assert!(
            score.separated.iter().all(|r| r.hit == 0),
            "{}: {:?}",
            ds.name,
            score.separated
        );
    }
}

#[test]
fn the_confirmed_proposal_matches_the_reference_on_every_tuning_fixture() {
    let all = only_tuning(fixtures());
    assert_eq!(
        all.len(),
        PROPOSAL_REFERENCE.len(),
        "a tuning fixture was added or removed: update PROPOSAL_REFERENCE"
    );
    for want in PROPOSAL_REFERENCE {
        let ds = dataset(&all, want.name);
        assert_reference(
            want,
            &score(ds, &applied(ds, &proposal_of(ds), false)).unwrap(),
        );
    }
}

/// Not an assertion: prints the bench table (`-- --nocapture`); each method adds a row per dataset.
#[test]
fn report() {
    for ds in only_tuning(fixtures()) {
        let prefix = score(&ds, &prefix_baseline(&ds.columns)).unwrap();
        let lexical = score(&ds, &lexical_method(&ds)).unwrap();
        let proposal = proposal_of(&ds);
        let confirmed = score(&ds, &applied(&ds, &proposal, false)).unwrap();
        let fusions = proposal
            .groups
            .iter()
            .filter(|g| g.status == Status::ToConfirm)
            .count();
        println!(
            "== {} ({:?})\n   prefix  : {prefix}\n   lexical : {lexical}\n   proposal: {confirmed}\n   ({fusions} fusions to confirm)",
            ds.name, ds.role
        );
    }
}

/// Not an assertion: the proposal with the data signals on, on local data only. For each tuning set: what a first
/// run applies without and with the data, and the fusions the data confirmed.
#[test]
fn data_proposal_report() {
    let Some(dir) = std::env::var_os(DATA_ENV).map(PathBuf::from) else {
        eprintln!("[SKIP] {DATA_ENV} is not set");
        return;
    };
    for ds in only_tuning(fixtures()) {
        let file = dir.join(format!("{}.jsonl", ds.name));
        if !file.exists() {
            eprintln!("[SKIP] {} not found", file.display());
            continue;
        }
        let signals = DataSignals::new(&sample_of(&ds, &file));
        let names_only = proposal_of(&ds);
        let with_data = proposal_with(&ds, Ok(&signals));
        let count = |p: &TableProposal, status: Status| {
            p.groups.iter().filter(|g| g.status == status).count()
        };
        println!(
            "== {}\n   names only: {}\n   with data : {}\n   {:?}; fusions to confirm {} -> {}",
            ds.name,
            score(&ds, &applied(&ds, &names_only, false)).unwrap(),
            score(&ds, &applied(&ds, &with_data, false)).unwrap(),
            with_data.data,
            count(&names_only, Status::ToConfirm),
            count(&with_data, Status::ToConfirm),
        );
        for g in with_data
            .groups
            .iter()
            .filter(|g| g.families > 1 && g.status == Status::Confirmed)
        {
            println!(
                "   confirmed fusion {} ({} columns, {} families): {:?}",
                g.name,
                g.columns.len(),
                g.families,
                g.evidence
            );
        }
    }
}

// ---------------------------------------------------------------- T2: static embeddings (local model only)

/// Folder of a Model2Vec model with a WordPiece tokenizer. Unset in CI, where the model is not available.
const MODEL_ENV: &str = "J2S_BENCH_MODEL";

fn local_model() -> Option<StaticEmbedder> {
    let Some(dir) = std::env::var_os(MODEL_ENV).map(PathBuf::from) else {
        eprintln!("[SKIP] {MODEL_ENV} is not set");
        return None;
    };
    Some(StaticEmbedder::load(&dir).unwrap())
}

/// The first components of the vectors the independent Python implementation gives for a list of texts
/// (accents, punctuation, long and unknown words), written with `oracle_embed.py --cases`.
#[test]
fn embeddings_match_the_python_oracle_on_the_real_model() {
    let Some(embedder) = local_model() else {
        return;
    };
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/column_groups/embedding_cases.json")).unwrap();
    assert!(cases.len() >= 30, "{} cases", cases.len());
    for case in &cases {
        let text = case["text"].as_str().unwrap();
        let head: Vec<f64> = case["head"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect();
        let got = embedder.embed(text);
        for (i, want) in head.iter().enumerate() {
            assert!(
                (got[i] - want).abs() < 1e-8,
                "{text:?}[{i}]: {} against {want}",
                got[i]
            );
        }
    }
}

/// Where the independent Python implementation (`oracle_embed.py --placements`) puts the nine columns the issue
/// cites, from the confirmed groups of the names-only proposal of the OpenFoodFacts fixture: for each column, its two
/// nearest groups as (a member of the group, cosine). `created_t` has the right group (history, `last_*`) second;
/// `photographers` and `creator` have no group to go to, and their scores say so.
const SPIKE_PLACEMENTS: &[(&str, [(&str, f64); 2])] = &[
    (
        "vitamins_tags",
        [
            ("nutrition_data", 0.492_101),
            ("ingredients_text", 0.293_544),
        ],
    ),
    (
        "minerals_tags",
        [
            ("nutrition_data", 0.349_369),
            ("ingredients_text", 0.323_576),
        ],
    ),
    (
        "serving_size",
        [
            ("nutrition_data", 0.251_839),
            ("ingredients_text", 0.160_234),
        ],
    ),
    (
        "nutrient_levels_tags",
        [
            ("nutrition_data", 0.644_355),
            ("unknown_ingredients_n", 0.443_447),
        ],
    ),
    (
        "created_t",
        [("origins", 0.319_125), ("last_image_t", 0.224_729)],
    ),
    (
        "photographers",
        [("countries", 0.150_392), ("brands", 0.146_558)],
    ),
    (
        "creator",
        [("origins", 0.316_548), ("allergens", 0.145_393)],
    ),
    (
        "ecoscore_grade",
        [
            ("environmental_score_grade", 0.405_939),
            ("nutrition_data", 0.281_539),
        ],
    ),
    (
        "ecoscore_tags",
        [
            ("environmental_score_grade", 0.241_066),
            ("product_name", 0.208_904),
        ],
    ),
];

#[test]
fn placements_match_the_python_oracle_on_the_columns_of_the_spike() {
    let Some(embedder) = local_model() else {
        return;
    };
    let all = only_tuning(fixtures());
    let ds = dataset(&all, "openfoodfacts");
    let mut proposal = proposal_of(ds);
    proposal.placements = placements(&proposal, &embedder);
    let index = |name: &str| {
        ds.columns
            .iter()
            .position(|c| c == name)
            .unwrap_or_else(|| panic!("no column {name}"))
    };
    let group_of = |member: &str| {
        let member = index(member);
        proposal
            .groups
            .iter()
            .position(|g| g.status == Status::Confirmed && g.columns.contains(&member))
            .unwrap_or_else(|| panic!("no confirmed group holds {member}"))
    };
    for (column, want) in SPIKE_PLACEMENTS {
        let got = proposal
            .placements
            .iter()
            .find(|p| p.column == index(column))
            .unwrap_or_else(|| panic!("{column}"));
        assert_eq!(got.nearest.len(), 2, "{column}: {:?}", got.nearest);
        for (&(group, score), &(member, cosine)) in got.nearest.iter().zip(want) {
            assert_eq!(group, group_of(member), "{column}: {:?}", got.nearest);
            assert!(
                (score - cosine).abs() < 1e-5,
                "{column}: {score} against {cosine}"
            );
        }
    }
}
