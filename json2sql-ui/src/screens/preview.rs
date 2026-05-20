/// Screen 4 — SQL Schema Preview (read-only)
///
/// Three-panel layout:
///   left 25%  — table list with strategy badges
///   center 45% — generated DDL for selected table
///   right 30%  — table summary (strategy, columns, FK)

use dioxus::prelude::*;

use json2sql::db::ddl::generate_ddl_preview;
use json2sql::schema::table_schema::WideStrategy;

use crate::screens::{strategy_color, strategy_label, TableListPanel, TableRowEntry};
use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn PreviewScreen(mut state: Signal<AppState>) -> Element {
    let schemas = {
        let s = state.read();
        crate::screens::build_effective_schemas(&s.schemas, &s.strategy_overrides)
    };

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

    let is_last_child = crate::screens::compute_last_child(&schemas);

    // Exclude tables that produce no DDL: Skip (Ignore) and pure routing containers.
    let visible_schemas: Vec<_> = schemas.iter()
        .enumerate()
        .filter(|(_, t)| {
            !matches!(t.wide_strategy, WideStrategy::Ignore)
            && !(matches!(t.wide_strategy, WideStrategy::MultiKeyedPivot(_))
                 && t.columns.iter().all(|c| c.is_generated))
        })
        .collect();

    // Pre-compute display entries for TableListPanel.
    let table_entries: Vec<TableRowEntry> = visible_schemas.iter().map(|&(orig_i, table)| {
        TableRowEntry {
            index: orig_i,
            name: table.name.clone(),
            depth: table.depth,
            is_last_child: is_last_child[orig_i],
            is_selected: orig_i == idx,
            badge_label: strategy_label(&table.wide_strategy),
            badge_color: strategy_color(&table.wide_strategy),
            dim: false,
        }
    }).collect();

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

                // ── Left — table tree (25%) ──────────────────────────────
                TableListPanel {
                    entries: table_entries,
                    on_select: move |orig_i: usize| {
                        let mut s = state.write();
                        s.selected_table_indices = std::collections::HashSet::from([orig_i]);
                        s.last_selected_idx = orig_i;
                    },
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
                        style: "display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid {theme::OUTLINE_FAINT};",
                        span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "Strategy" }
                        span {
                            style: "font-size:0.6875rem;font-weight:700;color:{theme::ON_PRIMARY};background:{strategy_col};padding:2px 6px;border-radius:2px;",
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
            style: "display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid {theme::OUTLINE_FAINT};",
            span { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "{label}" }
            span { style: "{value_style}", "{value}" }
        }
    }
}
