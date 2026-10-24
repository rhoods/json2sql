//! #78 T0 — test bench for the thematic column grouping: labeled datasets, scoring, "first word" baseline.
//!
//! Each `tests/fixtures/column_groups/*.toml` holds one dataset's columns and a PARTIAL ground truth:
//! `[[relation]]` = columns that must end up in the same group, `[[separate]]` = columns that must never
//! share one. A method is scored on groups of column names with `column_groups::score`; the reference to
//! beat is `column_groups::prefix_baseline`. A dataset with `role = "held_out"` is never used to tune
//! thresholds: `only_tuning` is the only way the tuning bench reads the fixtures.
//!
//! Print the table: `cargo test --test bench_column_groups report -- --nocapture`
#![allow(clippy::disallowed_methods)]

mod column_groups;

use column_groups::{first_word, fixtures, only_tuning, prefix_baseline, score, Dataset, Role};
use json2sql::schema::grouping::names::normalize;

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
    /// per `[[relation]]`: (pairs the prefix puts together, pairs in the relation)
    linked: &'static [(usize, usize)],
    /// per `[[separate]]`: (pairs the prefix wrongly puts together, pairs in the relation)
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

#[test]
fn prefix_baseline_matches_the_reference_on_every_fixture() {
    let all = fixtures();
    assert_eq!(
        all.len(),
        PREFIX_REFERENCE.len(),
        "a fixture was added or removed: update PREFIX_REFERENCE"
    );
    for want in PREFIX_REFERENCE {
        let ds = all
            .iter()
            .find(|d| d.name == want.name)
            .unwrap_or_else(|| panic!("fixture {} missing", want.name));
        let got = score(ds, &prefix_baseline(&ds.columns)).unwrap();
        let shape = (
            got.columns,
            got.groups,
            got.coverage,
            got.largest,
            got.capped,
        );
        assert_eq!(
            shape,
            (
                want.columns,
                want.groups,
                want.coverage,
                want.largest,
                want.capped
            ),
            "{}",
            want.name
        );
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

/// Not an assertion: prints the bench table (`-- --nocapture`); each method adds a row per dataset.
#[test]
fn report() {
    for ds in only_tuning(fixtures()) {
        let s = score(&ds, &prefix_baseline(&ds.columns)).unwrap();
        println!("== {} ({:?})\n   prefix: {s}", ds.name, ds.role);
    }
}
