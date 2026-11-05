//! Strategy name enum used to selectively disable auto-detection heuristics.
//!
//! Pass 1 accepts a `HashSet<StrategyName>` of strategies to skip; the finalizer
//! bypasses the corresponding detection step for each disabled strategy. Useful for
//! debugging an unexpected layout or forcing a specific materialization.
//!
//! Fonctions :
//! - enum `StrategyName` — stratégies optionnelles désactivables (Sibling seulement depuis #200).
//! - fn `StrategyName::as_str` — nom TOML/CLI de la stratégie.
//! - fn `StrategyName::try_from` — implémente `TryFrom<&str>` ; parse une chaîne, distingue stratégie
//!   inconnue, obligatoire (non désactivable) ou seulement proposée (`structured_pivot`).
//! - enum `StrategyError` — erreur de parsing d'un nom de stratégie (Mandatory/Proposed/Unknown).
//! - fn `parse_disabled_strategies` — valide une liste de flags `--disable-strategy` (CLI).

use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyName {
    Sibling,
}

impl StrategyName {
    #[allow(dead_code)] // used in tests + available as public API
    pub const OPTIONAL: &'static [Self] = &[Self::Sibling];

    #[allow(dead_code)] // used by json2sql-ui::config for TOML serialization
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sibling => "sibling",
        }
    }
}

impl TryFrom<&str> for StrategyName {
    type Error = StrategyError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "sibling" => Ok(Self::Sibling),
            // The suffix grid is only proposed since #200: there is nothing automatic left to disable.
            "structured_pivot" => Err(StrategyError::Proposed(s.to_string())),
            // The pivot is part of the split since #200: the split chooses it when the values are of one type.
            "split" | "pivot" => Err(StrategyError::Mandatory(s.to_string())),
            _ => Err(StrategyError::Unknown(s.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StrategyError {
    #[error("strategy '{0}' is mandatory and cannot be disabled\n  → mandatory strategies: split, pivot (part of the split)\n  → to store one table as JSONB: strategy = \"jsonb\" in its --schema-config section\n  → optional strategies: sibling")]
    Mandatory(String),
    #[error("strategy '{0}' is no longer automatic, there is nothing to disable\n  → --suggest-groups proposes the suffix grids; suffix_columns in a --schema-config section applies one\n  → optional strategies: sibling")]
    Proposed(String),
    #[error("unknown strategy '{0}'\n  → optional strategies: sibling")]
    Unknown(String),
}

/// Validates CLI flag values. Returns the set of disabled strategies or the
/// first error encountered. Called in main.rs before any file I/O.
pub fn parse_disabled_strategies(raw: &[String]) -> Result<HashSet<StrategyName>, StrategyError> {
    raw.iter()
        .map(|s| StrategyName::try_from(s.as_str()))
        .collect()
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_name_round_trip() {
        for &s in StrategyName::OPTIONAL {
            assert_eq!(StrategyName::try_from(s.as_str()).unwrap(), s);
        }
    }

    #[test]
    fn test_mandatory_strategy_rejected() {
        assert!(matches!(
            StrategyName::try_from("split"),
            Err(StrategyError::Mandatory(_))
        ));
    }

    #[test]
    fn test_unknown_strategy_rejected() {
        assert!(matches!(
            StrategyName::try_from("foobar"),
            Err(StrategyError::Unknown(_))
        ));
    }

    #[test]
    fn test_parse_valid_set() {
        let result = parse_disabled_strategies(&["sibling".to_string()]).unwrap();
        assert!(result.contains(&StrategyName::Sibling));
    }

    #[test]
    fn test_structured_pivot_is_only_proposed_now() {
        // #200: the suffix grid is never applied automatically; the error says where it went.
        let err = StrategyName::try_from("structured_pivot").unwrap_err();
        assert!(matches!(err, StrategyError::Proposed(_)));
        assert!(err.to_string().contains("suffix_columns"), "{err}");
    }

    #[test]
    fn test_pivot_is_part_of_the_mandatory_split() {
        // #200: the split of a wide table decides by itself between pivot and frequency; JSONB is asked in the TOML.
        let err = StrategyName::try_from("pivot").unwrap_err();
        assert!(matches!(err, StrategyError::Mandatory(_)));
        assert!(err.to_string().contains("strategy = \"jsonb\""), "{err}");
    }

    #[test]
    fn test_parse_empty_is_ok() {
        assert!(parse_disabled_strategies(&[]).unwrap().is_empty());
    }

    #[test]
    fn test_parse_mandatory_error() {
        assert!(matches!(
            parse_disabled_strategies(&["split".to_string()]),
            Err(StrategyError::Mandatory(_))
        ));
    }

    #[test]
    fn test_parse_unknown_error() {
        assert!(matches!(
            parse_disabled_strategies(&["foobar".to_string()]),
            Err(StrategyError::Unknown(_))
        ));
    }

    #[test]
    fn test_error_message_mandatory_hint() {
        let msg = StrategyName::try_from("split").unwrap_err().to_string();
        assert!(msg.contains("mandatory") && msg.contains("optional"));
    }

    #[test]
    fn test_error_message_unknown_hint() {
        let msg = StrategyName::try_from("foobar").unwrap_err().to_string();
        assert!(msg.contains("optional strategies"));
    }
}
