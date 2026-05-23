/// Right-panel component for the Strategy Editor.
///
/// Renders either single-select (strategy buttons + NormalizeDynamicKeys)
/// or multi-select (bulk-apply buttons) depending on `multi_select`.
use std::collections::HashMap;

use dioxus::prelude::*;

use json2sql::schema::table_schema::WideStrategy;

use crate::screens::strategy_label;
use crate::state::AppState;
use crate::theme;

// ---------------------------------------------------------------------------
// Pure helper — extracted for testability and shared by single + multi paths
// ---------------------------------------------------------------------------

/// Apply one strategy choice to the overrides map.
/// `None`     → remove override (revert to auto-detected Default).
/// `Some(s)`  → insert / replace override with `s`.
fn apply_strategy_override(
    overrides: &mut HashMap<String, WideStrategy>,
    name: &str,
    strategy: Option<WideStrategy>,
) {
    match strategy {
        None    => { overrides.remove(name); }
        Some(s) => { overrides.insert(name.to_string(), s); }
    }
}

// Display data for the five user-selectable strategies.
// None = "Default / remove override". Tuple: (strategy, label, badge_color).
type StrategyOption = (Option<WideStrategy>, &'static str, &'static str);

fn strategy_options() -> [StrategyOption; 5] {
    [
        (None,                           "Default (columns)",           theme::BADGE_DEFAULT),
        (Some(WideStrategy::Jsonb),      "JSONB séparé (table propre)", theme::BADGE_JSONB),
        (Some(WideStrategy::JsonbFlatten),"JSONB inline (colonne parent)",theme::BADGE_JSONB_INLINE),
        (Some(WideStrategy::Pivot),      "Pivot (EAV)",                 theme::BADGE_NORMALIZE),
        (Some(WideStrategy::Ignore),     "Skip (exclude)",              theme::BADGE_SKIP),
    ]
}

#[component]
pub fn StrategyConfigurator(
    mut state: Signal<AppState>,
    /// Index into schemas of the last-selected table.
    idx: usize,
    current_strategy: WideStrategy,
    multi_select: bool,
    selection_count: usize,
    common_parent: Option<String>,
) -> Element {
    let mut normalize_id_col: Signal<String> = use_signal(|| "id".to_string());

    let current_label = strategy_label(&current_strategy);
    let normalize_id_col_value = normalize_id_col.read().trim().to_string();
    let normalize_id_col_invalid = normalize_id_col_value.is_empty()
        || state.read().schema.schemas.get(idx)
            .map(|s| s.columns.iter().any(|col| col.name == normalize_id_col_value))
            .unwrap_or(false);

    rsx! {
        div {
            style: "flex:0 1 30%;min-width:0;min-height:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};padding:16px;display:flex;flex-direction:column;overflow-y:auto;",

            if multi_select {
                // ── Multi-select mode ─────────────────────────────────────
                h3 {
                    style: "color:{theme::ON_SURFACE};font-size:0.875rem;font-weight:600;margin:0 0 4px 0;",
                    "{selection_count} tables sélectionnées"
                }
                if let Some(ref parent) = common_parent {
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 12px 0;",
                        "Parent commun : "
                        span { style: "color:{theme::ON_SURFACE};font-family:{theme::FONT_CODE};", "{parent}" }
                    }
                } else {
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 12px 0;",
                        "Parents variés"
                    }
                }
                p {
                    style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.75rem;margin:0 0 8px 0;font-weight:600;",
                    "Appliquer à toutes :"
                }
                div {
                    style: "display:flex;flex-direction:column;gap:6px;margin-bottom:16px;",
                    for (strategy_opt, label, color) in strategy_options() {
                        StrategyButton {
                            label,
                            active: false,
                            color,
                            onclick: move |_| {
                                let names: Vec<String> = {
                                    let s = state.read();
                                    s.schema.selected_table_indices.iter()
                                        .filter_map(|&i| s.schema.schemas.get(i).map(|t| t.name.clone()))
                                        .collect()
                                };
                                let mut s = state.write();
                                for name in names {
                                    apply_strategy_override(&mut s.schema.strategy_overrides, &name, strategy_opt.clone());
                                }
                            }
                        }
                    }
                }
                div { style: "flex:1;" }
                p {
                    style: "color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;font-style:italic;margin:0;",
                    "La fusion (Keyed Pivot) nécessite une re-analyse avec un seuil Jaccard ajusté — non disponible via l'IHM."
                }
            } else {
                // ── Single-select mode ────────────────────────────────────
                h3 {
                    style: "color:{theme::ON_SURFACE};font-size:0.875rem;font-weight:600;margin:0 0 4px 0;",
                    "Strategy"
                }
                p {
                    style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 16px 0;",
                    "Current: "
                    span { style: "color:{theme::ON_SURFACE};font-family:{theme::FONT_CODE};", "{current_label}" }
                }
                div {
                    style: "display:flex;flex-direction:column;gap:6px;margin-bottom:16px;",
                    for (strategy_opt, label, color) in strategy_options() {
                        {
                            let is_active = match &strategy_opt {
                                None    => matches!(current_strategy, WideStrategy::Columns),
                                Some(s) => s == &current_strategy,
                            };
                            rsx! {
                                StrategyButton {
                                    label,
                                    active: is_active,
                                    color,
                                    onclick: move |_| {
                                        let name = state.read().schema.schemas[idx].name.clone();
                                        apply_strategy_override(
                                            &mut state.write().schema.strategy_overrides,
                                            &name,
                                            strategy_opt.clone(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                div {
                    style: "border-top:1px solid {theme::OUTLINE_VARIANT};padding-top:12px;margin-bottom:6px;",
                    p {
                        style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.75rem;margin:0 0 8px 0;font-weight:600;",
                        "Normalize dynamic keys"
                    }
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 8px 0;",
                        "Each JSON key becomes a row. The key itself is stored in the column named below."
                    }
                    input {
                        class: "input-field",
                        style: "margin-bottom:8px;",
                        r#type: "text",
                        placeholder: "id_column (e.g. image_id)",
                        value: "{normalize_id_col.read()}",
                        oninput: move |e| { *normalize_id_col.write() = e.value(); },
                    }
                    button {
                        class: "btn-ghost",
                        style: "width:100%;",
                        disabled: normalize_id_col_invalid,
                        onclick: move |_| {
                            let col = normalize_id_col.read().trim().to_string();
                            let id_col = if col.is_empty() { "id".to_string() } else { col };
                            let name = state.read().schema.schemas[idx].name.clone();
                            state.write().schema.strategy_overrides.insert(
                                name,
                                WideStrategy::NormalizeDynamicKeys { id_column: id_col },
                            );
                        },
                        "Apply Normalize"
                    }
                    if normalize_id_col_invalid {
                        p {
                            style: "color:{theme::ERROR};font-size:0.75rem;margin:8px 0 0 0;",
                            "Enter a non-empty, unique key column name."
                        }
                    }
                }
                div { style: "flex:1;" }
                if matches!(current_strategy, WideStrategy::StructuredPivot(_) | WideStrategy::KeyedPivot(_) | WideStrategy::AutoSplit { .. }) {
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin:0 0 12px 0;font-style:italic;",
                        "Auto-detected strategy. Override above if needed."
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_none_removes_override() {
        let mut overrides = HashMap::new();
        overrides.insert("products".to_string(), WideStrategy::Jsonb);
        apply_strategy_override(&mut overrides, "products", None);
        assert!(!overrides.contains_key("products"));
    }

    #[test]
    fn apply_some_inserts_override() {
        let mut overrides = HashMap::new();
        apply_strategy_override(&mut overrides, "products", Some(WideStrategy::Pivot));
        assert_eq!(overrides.get("products"), Some(&WideStrategy::Pivot));
    }

    #[test]
    fn apply_some_replaces_existing() {
        let mut overrides = HashMap::new();
        overrides.insert("products".to_string(), WideStrategy::Jsonb);
        apply_strategy_override(&mut overrides, "products", Some(WideStrategy::Pivot));
        assert_eq!(overrides.get("products"), Some(&WideStrategy::Pivot));
    }

    #[test]
    fn apply_none_on_missing_is_noop() {
        let mut overrides = HashMap::new();
        apply_strategy_override(&mut overrides, "products", None);
        assert!(overrides.is_empty());
    }
}

// ---------------------------------------------------------------------------
// Sub-component: strategy toggle button
// ---------------------------------------------------------------------------

#[component]
fn StrategyButton(
    label: &'static str,
    active: bool,
    color: &'static str,
    onclick: EventHandler<MouseEvent>,
) -> Element {
    let style = if active {
        format!(
            "background:{c};color:{t};-webkit-text-fill-color:{t};\
             border:none;border-radius:2px;padding:7px 12px;\
             font-size:0.8125rem;font-weight:600;cursor:pointer;text-align:left;width:100%;",
            c = color, t = theme::ON_PRIMARY
        )
    } else {
        format!(
            "background:transparent;color:{c};-webkit-text-fill-color:{c};\
             border:1px solid {c}66;border-radius:2px;padding:7px 12px;\
             font-size:0.8125rem;cursor:pointer;text-align:left;width:100%;",
            c = color
        )
    };

    rsx! {
        button {
            style: "{style}",
            aria_label: "{label}",
            onclick: move |e| onclick.call(e),
            "{label}"
        }
    }
}
