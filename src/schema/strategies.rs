use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyName {
    Sibling,
    Pivot,
    StructuredPivot,
}

impl StrategyName {
    pub const OPTIONAL: &'static [StrategyName] = &[
        StrategyName::Sibling,
        StrategyName::Pivot,
        StrategyName::StructuredPivot,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            StrategyName::Sibling => "sibling",
            StrategyName::Pivot => "pivot",
            StrategyName::StructuredPivot => "structured_pivot",
        }
    }
}

impl TryFrom<&str> for StrategyName {
    type Error = StrategyError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "sibling" => Ok(StrategyName::Sibling),
            "pivot" => Ok(StrategyName::Pivot),
            "structured_pivot" => Ok(StrategyName::StructuredPivot),
            "split" => Err(StrategyError::Mandatory(s.to_string())),
            _ => Err(StrategyError::Unknown(s.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StrategyError {
    #[error("strategy '{0}' is mandatory and cannot be disabled\n  → mandatory strategies: split\n  → optional strategies: sibling, pivot, structured_pivot")]
    Mandatory(String),
    #[error("unknown strategy '{0}'\n  → optional strategies: sibling, pivot, structured_pivot")]
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
        assert!(matches!(StrategyName::try_from("split"), Err(StrategyError::Mandatory(_))));
    }

    #[test]
    fn test_unknown_strategy_rejected() {
        assert!(matches!(StrategyName::try_from("foobar"), Err(StrategyError::Unknown(_))));
    }

    #[test]
    fn test_parse_valid_set() {
        let raw = vec!["sibling".to_string(), "pivot".to_string()];
        let result = parse_disabled_strategies(&raw).unwrap();
        assert!(result.contains(&StrategyName::Sibling));
        assert!(result.contains(&StrategyName::Pivot));
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
