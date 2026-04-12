/// Screen 3 — Strategy Editor (main workspace)
///
/// Three-panel layout:
///   left 25%  — table list with strategy badges
///   center 45% — column list for selected table
///   right 30%  — strategy configurator
use dioxus::prelude::*;

use json2sql::schema::table_schema::{TableSchema, WideStrategy};

use crate::screens::{strategy_color, strategy_label};
use crate::state::{AppScreen, AppState};
use crate::theme;

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[component]
pub fn StrategyScreen(mut state: Signal<AppState>) -> Element {
    // Buffer for the id_column name when setting NormalizeDynamicKeys.
    let mut normalize_id_col: Signal<String> = use_signal(|| "id".to_string());

    let schemas = state.read().schemas.clone();
    let tables_count = schemas.len();
    let columns_count: usize = schemas.iter().map(|s| s.columns.len()).sum();

    // Guard: if schemas are empty (shouldn't happen, but just in case)
    if schemas.is_empty() {
        return rsx! {
            div {
                style: "display:flex;align-items:center;justify-content:center;height:100vh;background:{theme::BG_ROOT};",
                p { style: "color:{theme::ON_SURFACE_DIM};", "No schema loaded." }
            }
        };
    }

    let idx = state.read().selected_table_idx.min(schemas.len().saturating_sub(1));
    let selected: &TableSchema = &schemas[idx];
    let current_strategy = selected.wide_strategy.clone();
    let current_label = strategy_label(&current_strategy);

    rsx! {
        div {
            style: "display:flex;flex-direction:column;height:100vh;background:{theme::BG_ROOT};",

            // ── Top bar ──────────────────────────────────────────────────
            div {
                style: "padding:10px 24px;background:{theme::BG_WORKSPACE};display:flex;align-items:center;gap:16px;",
                span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "Setup > Analysis > " }
                span { style: "color:{theme::ON_SURFACE};font-size:0.8125rem;font-weight:600;", "Strategy Editor" }
                div { style: "flex:1;" }
                // Stats badges
                span {
                    style: "background:{theme::BG_SIDEBAR};color:{theme::ON_SURFACE_VARIANT};font-size:0.6875rem;padding:3px 8px;border-radius:2px;font-family:{theme::FONT_CODE};",
                    "{tables_count} tables"
                }
                span {
                    style: "background:{theme::BG_SIDEBAR};color:{theme::ON_SURFACE_VARIANT};font-size:0.6875rem;padding:3px 8px;border-radius:2px;font-family:{theme::FONT_CODE};",
                    "{columns_count} columns"
                }
            }

            // ── Three-panel workspace ─────────────────────────────────────
            div {
                style: "display:flex;flex:1;overflow:hidden;",

                // ── Left — table list (25%) ───────────────────────────────
                div {
                    style: "flex:0 0 25%;background:{theme::BG_SIDEBAR};overflow-y:auto;padding:4px 0;",
                    for (i, table) in schemas.iter().enumerate() {
                        {
                            let is_selected = i == idx;
                            let indent = table.depth * 12;
                            let label = strategy_label(&table.wide_strategy);
                            let badge_color = strategy_color(&table.wide_strategy);
                            let row_bg = if is_selected {
                                "background:#00A57233;"
                            } else {
                                "background:transparent;"
                            };
                            let accent = if is_selected {
                                "border-left:2px solid #4EDEA3;"
                            } else {
                                "border-left:2px solid transparent;"
                            };
                            rsx! {
                                div {
                                    key: "{i}",
                                    style: "display:flex;align-items:center;gap:6px;padding:5px 8px 5px {indent}px;cursor:pointer;{row_bg}{accent}",
                                    onclick: move |_| { state.write().selected_table_idx = i; },
                                    span {
                                        style: "font-family:{theme::FONT_CODE};font-size:0.75rem;color:{theme::ON_SURFACE};flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                                        "{table.name}"
                                    }
                                    span {
                                        style: "font-size:0.5625rem;font-weight:700;letter-spacing:0.04em;color:#0D0D0D;background:{badge_color};padding:1px 4px;border-radius:2px;flex-shrink:0;",
                                        "{label}"
                                    }
                                }
                            }
                        }
                    }
                }

                // ── Center — column list (45%) ────────────────────────────
                div {
                    style: "flex:0 0 45%;background:{theme::BG_WORKSPACE};overflow-y:auto;padding:16px;",
                    // Table header
                    div {
                        style: "margin-bottom:16px;",
                        h2 {
                            style: "font-family:{theme::FONT_CODE};font-size:1rem;color:{theme::ON_SURFACE};margin:0 0 4px 0;letter-spacing:-0.02em;",
                            "{selected.name}"
                        }
                        if let Some(ref parent) = selected.parent_table {
                            span { style: "font-size:0.75rem;color:{theme::ON_SURFACE_DIM};", "↳ {parent}" }
                        }
                    }
                    // Column rows
                    div {
                        style: "display:grid;grid-template-columns:1fr auto;gap:0;",
                        // Header row
                        span { style: "font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;color:{theme::ON_SURFACE_DIM};padding:4px 0;border-bottom:1px solid #40475266;", "Column" }
                        span { style: "font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;color:{theme::ON_SURFACE_DIM};padding:4px 0;border-bottom:1px solid #40475266;text-align:right;", "Type" }
                        // Data rows
                        for col in selected.columns.iter() {
                            {
                                let name_color = if col.is_generated {
                                    theme::ON_SURFACE_DIM
                                } else {
                                    theme::ON_SURFACE
                                };
                                let type_str = col.pg_type.as_sql();
                                rsx! {
                                    span {
                                        key: "{col.name}",
                                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{name_color};padding:4px 0;border-bottom:1px solid #40475233;",
                                        "{col.name}"
                                    }
                                    span {
                                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::TERTIARY};padding:4px 0;border-bottom:1px solid #40475233;text-align:right;",
                                        "{type_str}"
                                    }
                                }
                            }
                        }
                    }
                }

                // ── Right — strategy configurator (30%) ──────────────────
                div {
                    style: "flex:0 0 30%;background:{theme::BG_SIDEBAR};padding:16px;display:flex;flex-direction:column;overflow-y:auto;",
                    h3 {
                        style: "color:{theme::ON_SURFACE};font-size:0.875rem;font-weight:600;margin:0 0 4px 0;",
                        "Strategy"
                    }
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 16px 0;",
                        "Current: "
                        span { style: "color:{theme::ON_SURFACE};font-family:{theme::FONT_CODE};", "{current_label}" }
                    }

                    // Strategy buttons
                    div {
                        style: "display:flex;flex-direction:column;gap:6px;margin-bottom:16px;",

                        StrategyButton {
                            label: "Default (columns)",
                            active: matches!(current_strategy, WideStrategy::Columns),
                            color: theme::BADGE_DEFAULT,
                            onclick: move |_| {
                                state.write().schemas[idx].wide_strategy = WideStrategy::Columns;
                            }
                        }
                        StrategyButton {
                            label: "JSONB (store as-is)",
                            active: matches!(current_strategy, WideStrategy::Jsonb),
                            color: theme::BADGE_JSONB,
                            onclick: move |_| {
                                state.write().schemas[idx].wide_strategy = WideStrategy::Jsonb;
                            }
                        }
                        StrategyButton {
                            label: "Pivot (EAV)",
                            active: matches!(current_strategy, WideStrategy::Pivot),
                            color: theme::BADGE_NORMALIZE,
                            onclick: move |_| {
                                state.write().schemas[idx].wide_strategy = WideStrategy::Pivot;
                            }
                        }
                        StrategyButton {
                            label: "Skip (exclude)",
                            active: matches!(current_strategy, WideStrategy::Ignore),
                            color: theme::BADGE_SKIP,
                            onclick: move |_| {
                                state.write().schemas[idx].wide_strategy = WideStrategy::Ignore;
                            }
                        }
                    }

                    // NormalizeDynamicKeys — needs an id_column name
                    div {
                        style: "border-top:1px solid #40475266;padding-top:12px;margin-bottom:6px;",
                        p {
                            style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.75rem;margin:0 0 8px 0;font-weight:600;",
                            "Normalize dynamic keys"
                        }
                        p {
                            style: "color:{theme::ON_SURFACE_DIM};font-size:0.75rem;margin:0 0 8px 0;",
                            "Each JSON key becomes a row. The key itself is stored in the column named below."
                        }
                        input {
                            style: "{theme::STYLE_INPUT}margin-bottom:8px;",
                            r#type: "text",
                            placeholder: "id_column (e.g. image_id)",
                            value: "{normalize_id_col.read()}",
                            oninput: move |e| { *normalize_id_col.write() = e.value(); },
                        }
                        button {
                            style: "{theme::STYLE_BTN_GHOST}width:100%;",
                            onclick: move |_| {
                                let col = normalize_id_col.read().trim().to_string();
                                let id_col = if col.is_empty() { "id".to_string() } else { col };
                                state.write().schemas[idx].wide_strategy =
                                    WideStrategy::NormalizeDynamicKeys { id_column: id_col };
                            },
                            "Apply Normalize"
                        }
                    }

                    div { style: "flex:1;" }

                    // Bottom note for auto-detected strategies
                    if matches!(current_strategy, WideStrategy::StructuredPivot(_) | WideStrategy::KeyedPivot(_) | WideStrategy::AutoSplit { .. }) {
                        p {
                            style: "color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin:0 0 12px 0;font-style:italic;",
                            "Auto-detected strategy. Override above if needed."
                        }
                    }
                }
            }

            // ── Bottom bar ───────────────────────────────────────────────
            div {
                style: "padding:12px 24px;background:{theme::BG_WORKSPACE};display:flex;justify-content:flex-end;align-items:center;",
                button {
                    style: "{theme::STYLE_BTN_PRIMARY}",
                    onclick: move |_| {
                        state.write().screen = AppScreen::Preview;
                    },
                    "Preview SQL Schema →"
                }
            }
        }
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
            "background:{};color:#0D0D0D;border:none;border-radius:2px;padding:7px 12px;font-size:0.8125rem;font-weight:600;cursor:pointer;text-align:left;width:100%;",
            color
        )
    } else {
        format!(
            "background:transparent;color:{};border:1px solid {}66;border-radius:2px;padding:7px 12px;font-size:0.8125rem;cursor:pointer;text-align:left;width:100%;",
            color, color
        )
    };

    rsx! {
        button {
            style: "{style}",
            onclick: move |e| onclick.call(e),
            "{label}"
        }
    }
}
