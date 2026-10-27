//! Compact snapshot form of the number tables of a Pass 1 sample (#91, phase 2).
//!
//! The snapshot is pretty-printed JSON, which puts every number of an array on its own indented line: the presence
//! bits, string hashes and rank numbers of a sample would weigh ten times their size. Each such table is written as
//! one hexadecimal string instead (16 digits per `u64` or `f64`, 8 per `u32`); a float keeps its exact bits, NaN
//! included.
//!
//! Fonctions :
//! - fn `to_hex` — chiffres hexadécimaux de mots de `width` chiffres.
//! - fn `from_hex` — mots de `width` chiffres relus depuis une chaîne hexadécimale.

/// The hexadecimal digits of `words`, `width` digits each.
fn to_hex(words: impl Iterator<Item = u64>, width: usize) -> String {
    use std::fmt::Write as _;
    words.fold(String::new(), |mut text, w| {
        let _ = write!(text, "{w:0width$x}"); // writing to a String cannot fail
        text
    })
}

/// The words of `width` digits of `text`.
fn from_hex<E: serde::de::Error>(text: &str, width: usize) -> Result<Vec<u64>, E> {
    if !text.len().is_multiple_of(width) || !text.is_ascii() {
        return Err(E::custom(format!(
            "packed table: {} characters is not a multiple of {width}",
            text.len()
        )));
    }
    (0..text.len())
        .step_by(width)
        .map(|at| u64::from_str_radix(&text[at..at + width], 16).map_err(E::custom))
        .collect()
}

/// `Vec<Vec<u64>>` as one hexadecimal string per inner list.
pub mod word_lists {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(lists: &[Vec<u64>], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            lists
                .iter()
                .map(|list| super::to_hex(list.iter().copied(), 16)),
        )
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u64>>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|text| super::from_hex(text, 16))
            .collect()
    }
}

/// `BTreeSet<u64>` as one hexadecimal string.
pub mod word_set {
    use std::collections::BTreeSet;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(set: &BTreeSet<u64>, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::to_hex(set.iter().copied(), 16))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeSet<u64>, D::Error> {
        Ok(
            super::from_hex::<D::Error>(&String::deserialize(deserializer)?, 16)?
                .into_iter()
                .collect(),
        )
    }
}

/// `Vec<u32>` as one hexadecimal string.
pub mod half_words {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(words: &[u32], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::to_hex(words.iter().map(|&w| u64::from(w)), 8))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u32>, D::Error> {
        super::from_hex::<D::Error>(&String::deserialize(deserializer)?, 8)?
            .into_iter()
            .map(|w| u32::try_from(w).map_err(serde::de::Error::custom))
            .collect()
    }
}

/// `Vec<f64>` as one hexadecimal string of their bits (NaN included).
pub mod floats {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(values: &[f64], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::to_hex(values.iter().map(|x| x.to_bits()), 16))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f64>, D::Error> {
        Ok(
            super::from_hex::<D::Error>(&String::deserialize(deserializer)?, 16)?
                .into_iter()
                .map(f64::from_bits)
                .collect(),
        )
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use std::collections::BTreeSet;

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct Tables {
        #[serde(with = "super::word_lists")]
        lists: Vec<Vec<u64>>,
        #[serde(with = "super::word_set")]
        set: BTreeSet<u64>,
        #[serde(with = "super::half_words")]
        halves: Vec<u32>,
        #[serde(with = "super::floats")]
        floats: Vec<f64>,
    }

    #[test]
    fn every_table_round_trips_as_one_string_each() {
        let t = Tables {
            lists: vec![vec![7], vec![], vec![u64::MAX, 3]],
            set: BTreeSet::from([5, 9, u64::MAX]),
            halves: vec![0, 42, u32::MAX],
            floats: vec![1.5, f64::NAN, -0.0, f64::INFINITY],
        };
        let json = serde_json::to_string_pretty(&t).unwrap();
        assert_eq!(
            json.lines().count(),
            10,
            "one line per table (and per list): {json}"
        );
        let back: Tables = serde_json::from_str(&json).unwrap();
        assert_eq!(
            (back.lists, back.set, back.halves),
            (t.lists, t.set, t.halves)
        );
        let bits = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        assert_eq!(
            bits(&back.floats),
            bits(&t.floats),
            "exact bits, NaN included"
        );
    }

    #[test]
    fn a_damaged_table_is_an_error_not_a_panic() {
        for bad in [r#""abc""#, r#""zzzzzzzzzzzzzzzz""#, r#""éééééééé""#] {
            assert!(
                serde_json::from_str::<SetOnly>(&format!(r#"{{"set": {bad}}}"#)).is_err(),
                "{bad}"
            );
        }
    }

    #[derive(serde::Deserialize)]
    struct SetOnly {
        #[serde(with = "super::word_set")]
        #[allow(dead_code)]
        set: BTreeSet<u64>,
    }
}
