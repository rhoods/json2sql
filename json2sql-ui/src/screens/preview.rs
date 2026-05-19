/// Screen 4 — SQL Schema Preview (read-only)
///
/// Three-panel layout:
///   left 25%  — table list with strategy badges
///   center 45% — generated DDL for selected table
///   right 30%  — table summary (strategy, columns, FK)

// UI Constants
const SELECTED_ROW_BG: &str = "#00A57233";
const SELECTED_ACCENT_COLOR: &str = "#4EDEA3";
const BORDER_COLOR: &str = "#40475233";
const BADGE_TEXT_COLOR: &str = "#0D0D0D";

use dioxus::prelude::*;

use json2sql::db::ddl::generate_ddl_preview;
use json2sql::schema::table_schema::WideStrategy;

use crate::screens::{strategy_color, strategy_label};
use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn PreviewScreen(mut state: Signal<AppState>) -> Element {
    let schemas = state.read().build_effective_schemas();

    if schemas.is_empty() {
        return rsx! {
            div {
                style: "display:flex;align-items:center;justify-content:center;height:100vh;background:{theme::BG_ROOT};",
                p { style: "color:{theme::ON_SURFACE_DIM};", "No schema loaded." }
            }
        };
    }

    let pg_schema = state.read().pg_schema.clone();
    let idx = state.read().last_selected_idx.min(schemas.len().saturating_sub(1));
    let selected = &schemas[idx];

    // Generate DDL for the selected table with overrides applied.
    let ddl = generate_ddl_preview(selected, &pg_schema);

    // Pre-calculate column counts for better performance
    let (data_cols, gen_cols) = selected.columns.iter()
        .fold((0, 0), |(data, gen), col| {
            if col.is_generated { (data, gen + 1) } else { (data + 1, gen) }
        });

    let strategy_lbl = strategy_label(&selected.wide_strategy);
    let strategy_col = strategy_color(&selected.wide_strategy);

    // Pre-compute last-child status for tree connectors (└─ vs ├─).
    let is_last_child: Vec<bool> = {
        let mut result = vec![true; schemas.len()];
        for i in 0..schemas.len() {
            if let Some(ref parent) = schemas[i].parent_table {
                for j in (i + 1)..schemas.len() {
                    if schemas[j].parent_table.as_deref() == Some(parent.as_str()) {
                        result[i] = false;
                        break;
                    }
                }
            }
        }
        result
    };

    // Pre-compute routing container names so children inherit the dimmed style.
    // build_effective_schemas() already applies overrides, so checking wide_strategy directly is correct.
    let routing_container_names: std::collections::HashSet<&str> = schemas.iter()
        .filter(|t| {
            matches!(t.wide_strategy, WideStrategy::MultiKeyedPivot(_))
                && t.columns.iter().all(|c| c.is_generated)
        })
        .map(|t| t.name.as_str())
        .collect();

    // Exclude Skip (Ignore) tables — they have no DDL and won't exist in the DB.
    let visible_schemas: Vec<_> = schemas.iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.wide_strategy, WideStrategy::Ignore))
        .collect();

    rsx! {
        div {
            style: "display:flex;flex-direction:column;height:100vh;background:{theme::BG_ROOT};",

            // ── Top bar ──────────────────────────────────────────────────
            div {
                style: "padding:10px 24px;background:{theme::BG_WORKSPACE};display:flex;align-items:center;gap:16px;",
                span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "Setup > Analysis > Strategy Editor > " }
                span { style: "color:{theme::ON_SURFACE};font-size:0.8125rem;font-weight:600;", "SQL Preview" }
                div { style: "flex:1;" }
                span {
                    style: "background:{theme::BG_SIDEBAR};color:{theme::ON_SURFACE_VARIANT};font-size:0.6875rem;padding:3px 8px;border-radius:2px;font-family:{theme::FONT_CODE};",
                    "{schemas.len()} tables"
                }
            }

            // ── Three-panel workspace ─────────────────────────────────────
            div {
                style: "display:flex;flex:1;overflow:hidden;min-height:0;min-width:0;",

                // ── Left — table tree with columns (25%) ─────────────────
                div {
                    style: "flex:0 1 25%;min-width:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};overflow-y:auto;padding:4px 0;",
                    for &(orig_i, table) in &visible_schemas {
                        {
                            let is_selected = orig_i == idx;
                            let is_routing_container = (matches!(table.wide_strategy, WideStrategy::MultiKeyedPivot(_))
                                && table.columns.iter().all(|c| c.is_generated))
                                || table.parent_table.as_deref()
                                    .map(|p| routing_container_names.contains(p))
                                    .unwrap_or(false);
                            let (label, badge_color) = if is_routing_container {
                                ("ROUTE", theme::BADGE_ROUTE)
                            } else {
                                (strategy_label(&table.wide_strategy), strategy_color(&table.wide_strategy))
                            };
                            let row_bg = if is_selected { format!("background:{};", SELECTED_ROW_BG) } else { "background:transparent;".to_string() };
                            let accent = if is_selected { format!("border-left:2px solid {};", SELECTED_ACCENT_COLOR) } else { "border-left:2px solid transparent;".to_string() };
                            let name_color = if is_routing_container { theme::ON_SURFACE_DIM } else { theme::ON_SURFACE };
                            let parent_indent = table.depth.saturating_sub(1) * 14 + 8;
                            let connector = if table.depth == 0 { "" } else if is_last_child[orig_i] { "└─" } else { "├─" };
                            let connector_color = if is_routing_container { theme::ON_SURFACE_DIM } else { "#717680" };
                            let wrapper_opacity = if is_routing_container { "opacity:0.6;" } else { "" };
                            let col_indent = table.depth * 14 + 22;
                            rsx! {
                                div {
                                    key: "{orig_i}",
                                    style: "{wrapper_opacity}",
                                    // Table header row
                                    div {
                                        style: "display:flex;align-items:center;gap:4px;padding:5px 8px 5px {parent_indent}px;cursor:pointer;{row_bg}{accent}",
                                        onclick: move |_| {
                                            let mut s = state.write();
                                            s.selected_table_indices = std::collections::HashSet::from([orig_i]);
                                            s.last_selected_idx = orig_i;
                                        },
                                        if table.depth > 0 {
                                            span {
                                                style: "font-size:0.6875rem;color:{connector_color};flex-shrink:0;line-height:1;",
                                                "{connector}"
                                            }
                                        }
                                        span {
                                            style: "font-family:{theme::FONT_CODE};font-size:0.75rem;color:{name_color};flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                                            "{table.name}"
                                        }
                                        span {
                                            style: "font-size:0.5625rem;font-weight:700;letter-spacing:0.04em;color:{BADGE_TEXT_COLOR};background:{badge_color};padding:1px 4px;border-radius:2px;flex-shrink:0;",
                                            "{label}"
                                        }
                                    }
                                    // Column rows
                                    for (j, col) in table.columns.iter().enumerate() {
                                        {
                                            let col_color = if col.is_generated {
                                                theme::ON_SURFACE_DIM
                                            } else {
                                                theme::ON_SURFACE_VARIANT
                                            };
                                            let type_str = col.pg_type.as_sql().to_lowercase();
                                            rsx! {
                                                div {
                                                    key: "{j}",
                                                    style: "display:flex;align-items:baseline;gap:6px;padding:1px 8px 1px {col_indent}px;",
                                                    span {
                                                        style: "font-family:{theme::FONT_CODE};font-size:0.6875rem;color:{col_color};flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                                                        "{col.name}"
                                                    }
                                                    span {
                                                        style: "font-size:0.5625rem;color:{theme::ON_SURFACE_DIM};white-space:nowrap;flex-shrink:0;font-family:{theme::FONT_CODE};",
                                                        "{type_str}"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // ── Center — DDL preview (45%) ────────────────────────────
                div {
                    style: "flex:0 1 45%;min-width:0;box-sizing:border-box;background:{theme::BG_EDITOR};overflow-y:auto;padding:16px;",
                    pre {
                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::ON_SURFACE};margin:0;white-space:pre-wrap;word-break:break-all;",
                        "{ddl}"
                    }
                }

                // ── Right — table summary (30%) ───────────────────────────
                div {
                    style: "flex:0 1 30%;min-width:0;min-height:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};padding:16px;overflow-y:auto;",
                    h3 {
                        style: "color:{theme::ON_SURFACE};font-size:0.875rem;font-weight:600;margin:0 0 16px 0;",
                        "Table summary"
                    }

                    SummaryRow { label: "Name", value: selected.name.clone(), mono: true }

                    // Strategy badge
                    div {
                        style: "display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #40475233;",
                        span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "Strategy" }
                        span {
                            style: "font-size:0.6875rem;font-weight:700;color:{BADGE_TEXT_COLOR};background:{strategy_col};padding:2px 6px;border-radius:2px;",
                            "{strategy_lbl}"
                        }
                    }

                    SummaryRow { label: "Data columns", value: data_cols.to_string(), mono: false }
                    SummaryRow { label: "Generated columns", value: gen_cols.to_string(), mono: false }

                    if let Some(ref parent) = selected.parent_table {
                        SummaryRow { label: "Parent table", value: parent.clone(), mono: true }
                    }

                    SummaryRow {
                        label: "Depth",
                        value: selected.depth.to_string(),
                        mono: false
                    }

                    if selected.is_junction() {
                        div {
                            style: "margin-top:12px;padding:8px;background:{theme::BG_INPUT};border-radius:2px;",
                            p {
                                style: "color:{theme::TERTIARY};font-size:0.75rem;margin:0;",
                                "Junction table — scalar array values"
                            }
                        }
                    }

                    // NormalizeDynamicKeys — show the id_column name
                    if let WideStrategy::NormalizeDynamicKeys { ref id_column } = selected.wide_strategy {
                        div {
                            style: "margin-top:12px;padding:8px;background:{theme::BG_INPUT};border-radius:2px;",
                            p { style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 4px 0;", "Key column" }
                            p { style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::ON_SURFACE};margin:0;", "{id_column}" }
                        }
                    }
                }
            }

            // ── Bottom bar ───────────────────────────────────────────────
            div {
                style: "padding:12px 24px;background:{theme::BG_WORKSPACE};display:flex;justify-content:space-between;",
                button {
                    class: "btn-ghost",
                    onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                    "aria-label": "Return to strategy editor",
                    "← Back to Strategies"
                }
                button {
                    class: "btn-primary",
                    onclick: move |_| { state.write().screen = AppScreen::Import; },
                    "aria-label": "Proceed to data import",
                    "Start Import →"
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sub-component: summary key/value row
// ---------------------------------------------------------------------------

#[component]
fn SummaryRow(label: &'static str, value: String, mono: bool) -> Element {
    let value_style = if mono {
        format!("font-family:{};font-size:0.8125rem;color:{};", theme::FONT_CODE, theme::ON_SURFACE)
    } else {
        format!("font-size:0.8125rem;color:{};", theme::ON_SURFACE)
    };

    rsx! {
        div {
            style: "display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid {BORDER_COLOR};",
            span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "{label}" }
            span { style: "{value_style}", "{value}" }
        }
    }
}
