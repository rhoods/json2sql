/// Screen 4 — SQL Schema Preview (read-only)
///
/// Three-panel layout:
///   left 25%  — table list with strategy badges
///   center 45% — generated DDL for selected table
///   right 30%  — table summary (strategy, columns, FK)
use dioxus::prelude::*;

use json2sql::db::ddl::generate_create_table;
use json2sql::schema::table_schema::WideStrategy;

use crate::screens::{strategy_color, strategy_label};
use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn PreviewScreen(mut state: Signal<AppState>) -> Element {
    let schemas = state.read().schemas.clone();

    if schemas.is_empty() {
        return rsx! {
            div {
                style: "display:flex;align-items:center;justify-content:center;height:100vh;background:{theme::BG_ROOT};",
                p { style: "color:{theme::ON_SURFACE_DIM};", "No schema loaded." }
            }
        };
    }

    let idx = state.read().selected_table_idx.min(schemas.len().saturating_sub(1));
    let selected = &schemas[idx];

    // Generate DDL for the selected table (preview mode: no DROP, IF NOT EXISTS).
    let ddl = generate_create_table(selected, "public", false);

    let data_cols = selected.columns.iter().filter(|c| !c.is_generated).count();
    let gen_cols  = selected.columns.iter().filter(|c| c.is_generated).count();
    let strategy_lbl = strategy_label(&selected.wide_strategy);
    let strategy_col = strategy_color(&selected.wide_strategy);

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
                            let row_bg = if is_selected { "background:#00A57233;" } else { "background:transparent;" };
                            let accent = if is_selected { "border-left:2px solid #4EDEA3;" } else { "border-left:2px solid transparent;" };
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

                // ── Center — DDL preview (45%) ────────────────────────────
                div {
                    style: "flex:0 0 45%;background:{theme::BG_EDITOR};overflow-y:auto;padding:16px;",
                    pre {
                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::ON_SURFACE};margin:0;white-space:pre-wrap;word-break:break-all;",
                        "{ddl}"
                    }
                }

                // ── Right — table summary (30%) ───────────────────────────
                div {
                    style: "flex:0 0 30%;background:{theme::BG_SIDEBAR};padding:16px;overflow-y:auto;",
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
                            style: "font-size:0.6875rem;font-weight:700;color:#0D0D0D;background:{strategy_col};padding:2px 6px;border-radius:2px;",
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
                    style: "{theme::STYLE_BTN_GHOST}",
                    onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                    "← Back to Strategies"
                }
                button {
                    style: "{theme::STYLE_BTN_PRIMARY}",
                    onclick: move |_| { state.write().screen = AppScreen::Import; },
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
            style: "display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid #40475233;",
            span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "{label}" }
            span { style: "{value_style}", "{value}" }
        }
    }
}
