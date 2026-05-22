/// Screen 3 — Strategy Editor (main workspace)
///
/// Three-panel layout:
///   left 25%  — table list with strategy badges
///   center 45% — column list for selected table
///   right 30%  — strategy configurator

use dioxus::prelude::*;

use json2sql::schema::finalizer::OverflowWarning;
use json2sql::schema::table_schema::{TableSchema, WideStrategy};

use crate::screens::{pick_save_file, strategy_color, strategy_label, PickResult, TableListPanel, TableRowEntry};
use crate::screens::strategy_configurator::StrategyConfigurator;
use crate::state::{AppScreen, AppState};
use crate::theme;

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[component]
pub fn StrategyScreen(mut state: Signal<AppState>) -> Element {
    let mut banner_dismissed = use_signal(|| false);
    let mut multi_select_mode: Signal<bool> = use_signal(|| false);
    // Feedback after save: Some(Ok(path)) or Some(Err(msg))
    let mut save_feedback: Signal<Option<Result<String, String>>> = use_signal(|| None);

    let schemas = state.read().schemas.clone();
    let overflow_warnings = state.read().overflow_warnings.clone();
    // Set of table names auto-converted to JSONB by the column limit guard.
    let overflow_table_names: std::collections::HashSet<String> =
        overflow_warnings.iter().map(|w| w.table_name.clone()).collect();
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

    let idx = state.read().last_selected_idx.min(schemas.len().saturating_sub(1));
    let selected: &TableSchema = &schemas[idx];
    let current_strategy = state.read().strategy_overrides
        .get(&selected.name)
        .cloned()
        .unwrap_or_else(|| selected.wide_strategy.clone());

    let selected_indices = state.read().selected_table_indices.clone();
    let multi_select = selected_indices.len() > 1;
    let common_parent: Option<String> = if multi_select {
        let parents: std::collections::HashSet<Option<&str>> = selected_indices.iter()
            .filter_map(|&i| schemas.get(i))
            .map(|t| t.parent_table.as_deref())
            .collect();
        if parents.len() == 1 { parents.into_iter().next().flatten().map(|s| s.to_string()) } else { None }
    } else {
        None
    };
    let selection_count = selected_indices.len();

    let is_last_child = crate::screens::compute_last_child(&schemas);
    let strategy_overrides_snap = state.read().strategy_overrides.clone();

    // Pre-compute display entries for TableListPanel.
    let table_entries: Vec<TableRowEntry> = schemas.iter().enumerate().map(|(i, table)| {
        let user_overrode = strategy_overrides_snap.contains_key(&table.name);
        let effective = strategy_overrides_snap.get(&table.name).cloned()
            .unwrap_or_else(|| table.wide_strategy.clone());
        let is_routing_container = !user_overrode
            && matches!(effective, WideStrategy::MultiKeyedPivot(_))
            && table.columns.iter().all(|c| c.is_generated);
        let is_overflow_jsonb = !user_overrode
            && matches!(effective, WideStrategy::Jsonb)
            && overflow_table_names.contains(&table.name);
        let (badge_label, badge_color) = if is_routing_container {
            ("ROUTE", theme::BADGE_ROUTE)
        } else if is_overflow_jsonb {
            ("JSONB ⚠", theme::BADGE_JSONB_OVERFLOW)
        } else {
            (strategy_label(&effective), strategy_color(&effective))
        };
        TableRowEntry {
            index: i,
            name: table.name.clone(),
            depth: table.depth,
            is_last_child: is_last_child[i],
            is_selected: selected_indices.contains(&i),
            badge_label,
            badge_color,
            dim: is_routing_container,
        }
    }).collect();

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
                // Save feedback inline
                if let Some(ref result) = *save_feedback.read() {
                    match result {
                        Ok(path) => rsx! {
                            span {
                                style: "color:{theme::PRIMARY};font-size:0.6875rem;",
                                "✓ Saved to {path}"
                            }
                        },
                        Err(msg) => rsx! {
                            span {
                                style: "color:{theme::ERROR};font-size:0.6875rem;",
                                "✗ {msg}"
                            }
                        },
                    }
                }
                // Multi-select toggle
                {
                    let is_multi = *multi_select_mode.read();
                    let btn_style = if is_multi {
                        format!("font-size:0.8125rem;padding:4px 10px;background:{};color:{};-webkit-text-fill-color:{};border:none;", theme::SECONDARY, theme::ON_PRIMARY, theme::ON_PRIMARY)
                    } else {
                        "font-size:0.8125rem;padding:4px 10px;".to_string()
                    };
                    let label = if is_multi { "✓ Multi-select ON" } else { "⊕ Multi-select" };
                    rsx! {
                        button {
                            class: if is_multi { "btn-ghost" } else { "btn-ghost" },
                            style: "{btn_style}",
                            onclick: move |_| {
                                let new_val = !*multi_select_mode.read();
                                multi_select_mode.set(new_val);
                                if !new_val {
                                    // Turning off: collapse to last_selected_idx only
                                    let mut s = state.write();
                                    let last = s.last_selected_idx;
                                    s.selected_table_indices = std::collections::HashSet::from([last]);
                                }
                            },
                            "{label}"
                        }
                    }
                }
                button {
                    class: "btn-ghost",
                    style: "font-size:0.8125rem;padding:4px 10px;",
                    onclick: move |_| {
                        let s = state.read();
                        let schemas = s.schemas.clone();
                        let total_rows = s.pass1_progress.rows_scanned;
                        let truncated = s.truncated_names.clone();
                        let collisions = s.column_collisions.clone();
                        let stats = s.pass1_stats.clone();
                        let overrides = s.strategy_overrides.clone();
                        drop(s);
                        let mut fb = save_feedback.clone();
                        spawn(async move {
                            match pick_save_file("schema.json").await {
                                PickResult::Selected(path) => {
                                    let result = json2sql::schema::persistence::save_with_overrides(
                                        &schemas, total_rows, &truncated, &collisions, &stats, &overrides, &path,
                                    );
                                    match result {
                                        Ok(()) => fb.set(Some(Ok(
                                            path.file_name()
                                                .and_then(|n| n.to_str())
                                                .unwrap_or("schema.json")
                                                .to_string()
                                        ))),
                                        Err(e) => fb.set(Some(Err(e.to_string()))),
                                    }
                                }
                                PickResult::Cancelled => {}
                                PickResult::NotAvailable => {
                                    fb.set(Some(Err("File picker unavailable".to_string())));
                                }
                            }
                        });
                    },
                    "💾 Save schema"
                }
            }

            // ── Overflow warning banner ───────────────────────────────────
            if !overflow_warnings.is_empty() && !*banner_dismissed.read() {
                div {
                    style: "padding:8px 24px;background:#3A2500;border-bottom:1px solid {theme::TERTIARY}44;display:flex;align-items:center;gap:8px;flex-wrap:wrap;",
                    span {
                        style: "color:{theme::TERTIARY};font-size:0.75rem;font-weight:700;flex-shrink:0;",
                        "⚠ {overflow_warnings.len()} table(s) auto-converted to JSONB (exceeded PostgreSQL 1600-column limit):"
                    }
                    for w in overflow_warnings.iter() {
                        span {
                            key: "{w.table_name}",
                            style: "font-family:{theme::FONT_CODE};font-size:0.6875rem;color:{theme::TERTIARY};background:#FFFFFF18;padding:1px 6px;border-radius:2px;",
                            "{w.table_name} ({w.original_column_count} cols)"
                        }
                    }
                    div { style: "flex:1;" }
                    button {
                        style: "background:transparent;border:none;color:{theme::TERTIARY};font-size:1rem;cursor:pointer;padding:0 4px;line-height:1;flex-shrink:0;",
                        aria_label: "Dismiss",
                        onclick: move |_| { banner_dismissed.set(true); },
                        "×"
                    }
                }
            }

            // ── Three-panel workspace ─────────────────────────────────────
            div {
                style: "display:flex;flex:1;overflow:hidden;min-height:0;min-width:0;",

                // ── Left — table list (25%) ───────────────────────────────
                TableListPanel {
                    entries: table_entries,
                    on_select: move |i: usize| {
                        let is_multi = *multi_select_mode.read();
                        let mut s = state.write();
                        if is_multi {
                            if s.selected_table_indices.contains(&i) {
                                if s.selected_table_indices.len() > 1 {
                                    s.selected_table_indices.remove(&i);
                                }
                            } else {
                                s.selected_table_indices.insert(i);
                                s.last_selected_idx = i;
                            }
                        } else {
                            s.selected_table_indices = std::collections::HashSet::from([i]);
                            s.last_selected_idx = i;
                        }
                    },
                }

                // ── Center — column list (45%) ────────────────────────────
                div {
                    style: "flex:0 1 45%;min-width:0;box-sizing:border-box;background:{theme::BG_WORKSPACE};overflow-y:auto;padding:16px;",
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
                        span { style: "font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;color:{theme::ON_SURFACE_DIM};padding:4px 0;border-bottom:1px solid {theme::OUTLINE_VARIANT};", "Column" }
                        span { style: "font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;color:{theme::ON_SURFACE_DIM};padding:4px 0;border-bottom:1px solid {theme::OUTLINE_VARIANT};text-align:right;", "Type" }
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
                                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{name_color};padding:4px 0;border-bottom:1px solid {theme::OUTLINE_FAINT};",
                                        "{col.name}"
                                    }
                                    span {
                                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::TERTIARY};padding:4px 0;border-bottom:1px solid {theme::OUTLINE_FAINT};text-align:right;",
                                        "{type_str}"
                                    }
                                }
                            }
                        }
                    }
                }

                // ── Right — strategy configurator (30%) ──────────────────
                StrategyConfigurator {
                    state,
                    idx,
                    current_strategy,
                    multi_select,
                    selection_count,
                    common_parent,
                }
            }

            // ── Bottom bar ───────────────────────────────────────────────
            div {
                style: "padding:12px 24px;background:{theme::BG_WORKSPACE};display:flex;justify-content:flex-end;align-items:center;",
                button {
                    class: "btn-primary",
                    onclick: move |_| {
                        state.write().screen = AppScreen::Preview;
                    },
                    "Preview SQL Schema →"
                }
            }
        }
    }
}

