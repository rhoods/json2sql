//! Labels et badges de stratégie — représentation lisible d'une `InferredStrategy`/`UserOverride`
//! pour l'affichage dans la liste de tables et l'écran Strategy.
//!
//! Fonctions :
//! - fn `strategy_label` — libellé lisible d'une `InferredStrategy`
//! - fn `strategy_badge` — classe CSS + libellé court d'une `InferredStrategy`
//! - fn `user_override_badge` — classe CSS + libellé court d'un `UserOverride`

use json2sql::schema::table_schema::{InferredStrategy, UserOverride};

pub const fn strategy_label(s: &InferredStrategy) -> &'static str {
    match s {
        InferredStrategy::Columns                     => "DEFAULT",
        InferredStrategy::Pivot                       => "PIVOT",
        InferredStrategy::Jsonb                       => "JSONB SÉP.",
        InferredStrategy::JsonbFlatten                => "JSONB INLINE",
        InferredStrategy::StructuredPivot(_)          => "STRUCT PIVOT",
        InferredStrategy::SiblingCollapse(_)               => "SIBLING COLLAPSE",
        InferredStrategy::SiblingCollapseMulti(_)          => "SIBLING COLLAPSE MULTI",
        InferredStrategy::AutoSplit { .. }            => "AUTO SPLIT",
        InferredStrategy::Ignore                      => "SKIP",
        InferredStrategy::NormalizeDynamicKeys { .. } => "NORMALIZE",
        InferredStrategy::Flatten { .. }              => "FLATTEN",
    }
}

/// Returns (`css_badge_class_suffix`, `short_label`) for the new design-system `.badge` classes.
pub const fn strategy_badge(s: &InferredStrategy) -> (&'static str, &'static str) {
    match s {
        InferredStrategy::Columns                     => ("default",   "default"),
        InferredStrategy::Jsonb                       => ("jsonb",     "jsonb"),
        InferredStrategy::JsonbFlatten                => ("jsonbi",    "flatten"),
        InferredStrategy::Pivot                       => ("pivot",     "pivot"),
        InferredStrategy::NormalizeDynamicKeys { .. } => ("normalize", "normalize"),
        InferredStrategy::Ignore                      => ("skip",      "skip"),
        InferredStrategy::Flatten { .. }              => ("flatten",   "flatten"),
        InferredStrategy::StructuredPivot(_)          => ("pivot",     "struct pivot"),
        InferredStrategy::SiblingCollapse(_)               => ("pivot",     "sibling collapse"),
        InferredStrategy::SiblingCollapseMulti(_)          => ("pivot",     "sibling collapse multi"),
        InferredStrategy::AutoSplit { .. }            => ("normalize", "auto split"),
    }
}

pub const fn user_override_badge(o: &UserOverride) -> (&'static str, &'static str) {
    match o {
        UserOverride::Columns                      => ("columns",   "columns"),
        UserOverride::Pivot                        => ("pivot",     "pivot"),
        UserOverride::Jsonb                        => ("jsonb",     "jsonb"),
        UserOverride::Skip                         => ("skip",      "skip"),
        UserOverride::JsonbFlatten                 => ("jsonb",     "jsonb flatten"),
        UserOverride::Flatten { .. }               => ("flatten",   "flatten"),
        UserOverride::NormalizeDynamicKeys { .. }  => ("normalize", "normalize"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use json2sql::schema::table_schema::TableSchema;

    fn make_table(name: &str, parent: Option<&str>) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.parent_table = parent.map(str::to_string);
        t
    }

    #[test]
    fn strategy_badge_uses_effective_strategy_not_inferred() {
        // Table with ui_override=Pivot but inferred as Columns
        let mut t = make_table("tags", None);
        t.set_ui_override(Some(UserOverride::Pivot));
        // strategy_badge on effective_strategy() should give Pivot badge, not Columns
        let (cls_eff, lbl_eff) = strategy_badge(&*t.effective_strategy());
        let (cls_inf, lbl_inf) = strategy_badge(&t.inferred_strategy);
        assert_ne!(lbl_eff, lbl_inf, "effective badge must differ from inferred badge");
        assert_eq!(lbl_eff, "pivot", "effective badge must be pivot");
        assert_eq!(lbl_inf, "default", "inferred badge must still be default (Columns)");
        assert_eq!(cls_eff, "pivot");
        assert_eq!(cls_inf, "default");
    }
}
