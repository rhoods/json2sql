pub mod setup;
pub mod analysis;
pub mod strategy;
pub mod preview;
pub mod import;

use json2sql::schema::table_schema::WideStrategy;
use crate::theme;

pub fn strategy_label(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns                     => "DEFAULT",
        WideStrategy::Pivot                       => "PIVOT",
        WideStrategy::Jsonb                       => "JSONB",
        WideStrategy::StructuredPivot(_)          => "STRUCT PIVOT",
        WideStrategy::KeyedPivot(_)               => "KEYED PIVOT",
        WideStrategy::AutoSplit { .. }            => "AUTO SPLIT",
        WideStrategy::Ignore                      => "SKIP",
        WideStrategy::NormalizeDynamicKeys { .. } => "NORMALIZE",
        WideStrategy::Flatten { .. }              => "FLATTEN",
    }
}

pub fn strategy_color(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns                     => theme::BADGE_DEFAULT,
        WideStrategy::Pivot                       => theme::BADGE_NORMALIZE,
        WideStrategy::Jsonb                       => theme::BADGE_JSONB,
        WideStrategy::StructuredPivot(_)          => theme::BADGE_FLATTEN,
        WideStrategy::KeyedPivot(_)               => theme::BADGE_FLATTEN,
        WideStrategy::AutoSplit { .. }            => theme::BADGE_NORMALIZE,
        WideStrategy::Ignore                      => theme::BADGE_SKIP,
        WideStrategy::NormalizeDynamicKeys { .. } => theme::BADGE_NORMALIZE,
        WideStrategy::Flatten { .. }              => theme::BADGE_FLATTEN,
    }
}
