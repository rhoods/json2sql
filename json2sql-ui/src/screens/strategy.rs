//! Screen 3 — Strategy Editor
//!
//! Three-pane split layout (left 320px · center fluid · right 340px):
//!   left   — flat sortable table list with badges + filter
//!   center — column detail (single) or selection summary (multi)
//!   right  — strategy configurator (single) or bulk apply (multi)
#![allow(clippy::disallowed_methods, clippy::derive_partial_eq_without_eq)]

use dioxus::prelude::*;

use dioxus::prelude::Modifiers;
use json2sql::schema::table_schema::{InferredStrategy, UserOverride};

use crate::screens::{build_table_rows, pick_save_file, strategy_badge, strategy_label, user_override_badge, PickResult, TableRowsCtx};
use crate::state::{compute_jaccard_display, select_children_visible};
use crate::screens::table_list::TableListPanel;
use crate::state::{AppScreen, AppState};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[allow(clippy::derive_partial_eq_without_eq)]
#[component]
pub fn StrategyScreen(mut state: Signal<AppState>) -> Element {
    let mut left_collapsed:  Signal<bool>            = use_signal(|| false);
    let mut right_collapsed: Signal<bool>            = use_signal(|| false);
    let mut filter_text:     Signal<String>          = use_signal(String::new);
    let mut warn_only:       Signal<bool>            = use_signal(|| false);
    let mut save_feedback:   Signal<Option<Result<String, String>>> = use_signal(|| None);
    let mut picking_save:    Signal<bool>            = use_signal(|| false);
    let mut banner_dismissed:         Signal<bool> = use_signal(|| false);
    let mut cfg_banner_dismissed:     Signal<bool> = use_signal(|| false);
    let mut anchor_idx:      Signal<usize>           = use_signal(|| 0);

    // ── Derived snapshot ──────────────────────────────────────────────────
    let schemas           = state.read().schema.schemas.clone();
    let overrides_snap    = state.read().schema.strategy_overrides.clone();
    let overflow_warnings  = state.read().schema.overflow_warnings.clone();
    let config_warnings    = state.read().schema.config_warnings.clone();
    let selected_indices  = state.read().schema.selected_table_indices.clone();
    let absorbed_names    = state.read().schema.absorbed_names.clone();

    let overflow_names: std::collections::HashSet<String> =
        overflow_warnings.iter().map(|w| w.table_name.clone()).collect();

    let tables_count  = schemas.len();
    let columns_count: usize = schemas.iter().map(|s| s.columns.len()).sum();
    let overflow_count = overflow_warnings.len();

    if schemas.is_empty() {
        return rsx! {
            div { style: "display:flex;align-items:center;justify-content:center;height:100vh;background:var(--bg);",
                p { style: "color:var(--fg-3);", "No schema loaded." }
            }
        };
    }

    let idx = state.read().schema.last_selected_idx.min(schemas.len().saturating_sub(1));
    let selected_table = &schemas[idx];
    let inferred_strategy = &selected_table.inferred_strategy;
    let current_override: Option<&UserOverride> = overrides_snap.get(&selected_table.name);

    let is_multi = selected_indices.len() > 1;
    let selection_count = selected_indices.len();

    // Jaccard info + derived display strings — computed only when multi-select is active.
    let jaccard = if is_multi {
        let idx_vec: Vec<usize> = selected_indices.iter().copied().collect();
        compute_jaccard_display(&schemas, &idx_vec)
    } else {
        crate::state::JaccardDisplay { score: 1.0, common: 0, union_count: 0 }
    };
    let jaccard_score_pct   = format!("{:.0}%", jaccard.score * 100.0);
    let jaccard_ratio_txt   = if jaccard.union_count == 0 {
        "no data cols".to_string()
    } else {
        format!("{} / {} cols", jaccard.common, jaccard.union_count)
    };
    let (jaccard_color, jaccard_label) = if jaccard.score >= 0.7 {
        ("var(--success)", "High similarity")
    } else if jaccard.score >= 0.5 {
        ("var(--warning)", "Medium similarity")
    } else {
        ("var(--danger)", "Low similarity")
    };

    // Table list — filtered
    let filter   = filter_text.read().to_lowercase();
    let show_warn = *warn_only.read();

    // Pre-compute rows and visible index list so the Shift+click handler can capture it.
    let anomaly_counts = state.read().import.pass2_progress.anomaly_counts_per_table.clone();
    let table_rows = build_table_rows(&schemas, &TableRowsCtx {
        overrides: &overrides_snap, overflow_names: &overflow_names,
        selected_indices: &selected_indices, absorbed_names: &absorbed_names,
        filter: &filter, show_warn_only: show_warn, anomaly_counts: &anomaly_counts,
    });
    let visible_indices: Vec<usize> = table_rows.iter()
        .filter(|r| r.visible)
        .map(|r| r.index)
        .collect();
    let vi_all      = visible_indices.clone();
    let vi_children = visible_indices.clone();

    // ── Render ────────────────────────────────────────────────────────────
    rsx! {
        div { style: "display:flex;flex-direction:column;height:100vh;background:var(--bg);",

            // ── Subbar ────────────────────────────────────────────────────
            div { class: "subbar",
                div { class: "crumb",
                    button {
                        class: "step",
                        onclick: move |_| { state.write().screen = AppScreen::Setup; },
                        "Setup"
                    }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Analysis" }
                    span { class: "sep", "›" }
                    button { class: "step active", "Strategy editor" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "SQL Preview" }
                }
                span { class: "grow" }
                div { class: "row gap-md",
                    span { class: "badge muted sq", "{tables_count} tables" }
                    span { class: "badge muted sq", "{columns_count} cols" }
                    if overflow_count > 0 {
                        span { class: "badge warn sq", "⚠ {overflow_count} overflow" }
                    }
                    // Save schema
                    button {
                        class: "btn ghost sm",
                        disabled: picking_save(),
                        onclick: move |_| {
                            if picking_save() { return; }
                            let s = state.read();
                            let schemas = s.schema.schemas.clone();
                            let total_rows = s.schema.pass1_progress.rows_scanned;
                            let truncated = s.schema.truncated_names.clone();
                            let collisions = s.schema.column_collisions.clone();
                            let stats = s.schema.pass1_stats.clone();
                            let overrides = s.schema.strategy_overrides.clone();
                            let overflow_warnings = s.schema.overflow_warnings.clone();
                            drop(s);
                            spawn(async move {
                                picking_save.set(true);
                                save_feedback.set(None);
                                if let PickResult::Selected(path) = pick_save_file("schema.json").await {
                                    let filename = path.file_name()
                                        .and_then(|n| n.to_str())
                                        .unwrap_or("schema.json")
                                        .to_string();
                                    let result = tokio::task::spawn_blocking(move || {
                                        json2sql::schema::persistence::save_with_overrides(
                                            &schemas, total_rows, &truncated, &collisions,
                                            &stats, &overflow_warnings, &overrides, &path,
                                        )
                                    }).await;
                                    match result {
                                        Ok(Ok(())) => save_feedback.set(Some(Ok(filename))),
                                        Ok(Err(e)) => save_feedback.set(Some(Err(e.to_string()))),
                                        Err(e)     => save_feedback.set(Some(Err(format!("Save failed: {e}")))),
                                    }
                                }
                                picking_save.set(false);
                            });
                        },
                        if picking_save() { "Saving…" } else { "💾 Save" }
                    }
                    if let Some(ref fb) = *save_feedback.read() {
                        match fb {
                            Ok(name) => rsx! { span { style: "color:var(--success);font-size:var(--fs-xs);", "✓ {name}" } },
                            Err(msg) => rsx! { span { style: "color:var(--danger);font-size:var(--fs-xs);", "✗ {msg}" } },
                        }
                    }
                }
            }

            // ── Overflow banner ───────────────────────────────────────────
            if !overflow_warnings.is_empty() && !*banner_dismissed.read() {
                div { style: "padding:6px 16px;background:#3A2500;border-bottom:1px solid rgba(240,176,114,.3);display:flex;align-items:center;gap:8px;flex-wrap:wrap;",
                    span { style: "color:var(--warning);font-size:var(--fs-xs);font-weight:700;flex-shrink:0;",
                        "⚠ {overflow_count} table(s) auto-converted to JSONB (exceeded PostgreSQL 1600-column limit):"
                    }
                    for w in overflow_warnings.iter() {
                        span {
                            key: "{w.table_name}",
                            style: "font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);color:var(--warning);background:rgba(255,255,255,.1);padding:1px 6px;border-radius:var(--r-sm);",
                            "{w.table_name} ({w.original_column_count} cols)"
                        }
                    }
                    span { class: "grow" }
                    button {
                        style: "background:transparent;border:none;color:var(--warning);cursor:pointer;font-size:1rem;padding:0 4px;",
                        onclick: move |_| { banner_dismissed.set(true); },
                        "×"
                    }
                }
            }

            // ── Config-warnings banner ────────────────────────────────────
            if !config_warnings.is_empty() && !*cfg_banner_dismissed.read() {
                div { style: "padding:6px 16px;background:#2A1A00;border-bottom:1px solid rgba(240,176,114,.2);display:flex;align-items:flex-start;gap:8px;flex-wrap:wrap;",
                    span { style: "color:var(--warning);font-size:var(--fs-xs);font-weight:700;flex-shrink:0;",
                        "⚠ Schema config warnings:"
                    }
                    div { style: "display:flex;flex-direction:column;gap:2px;flex:1;",
                        for w in config_warnings.iter() {
                            span {
                                key: "{w.to_message()}",
                                style: "font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);color:var(--warning);",
                                "{w.to_message()}"
                            }
                        }
                    }
                    button {
                        style: "background:transparent;border:none;color:var(--warning);cursor:pointer;font-size:1rem;padding:0 4px;flex-shrink:0;",
                        onclick: move |_| { cfg_banner_dismissed.set(true); },
                        "×"
                    }
                }
            }

            // ── split-3 ───────────────────────────────────────────────────
            div { class: "split-3", style: "flex:1;min-height:0;",

                // ── LEFT — table list ─────────────────────────────────────
                {
                    let lc = *left_collapsed.read();
                    rsx! {
                        div {
                            class: if lc { "pane collapsed" } else { "pane" },
                            style: "flex:0 0 320px;",

                            // collapsed strip (shown when collapsed)
                            div {
                                class: "collapsed-strip",
                                onclick: move |_| { left_collapsed.set(false); },
                                span { class: "lbl", "Tables" }
                                span { style: "font-size:18px;color:var(--fg-3);", "⊞" }
                            }

                            // pane head
                            div { class: "pane-head",
                                span { class: "ttl", "⬡ Tables" }
                                span { class: "count", "{tables_count}" }
                                span { class: "grow" }
                                button {
                                    class: "collapse-btn",
                                    title: "Collapse",
                                    onclick: move |_| { left_collapsed.set(!lc); },
                                    if lc { "▶" } else { "◀" }
                                }
                            }

                            // filter bar
                            div { style: "padding:6px 10px;border-bottom:1px solid var(--bd);display:flex;gap:8px;align-items:center;",
                                input {
                                    class: "input sm grow",
                                    placeholder: "filter…",
                                    value: "{filter_text.read()}",
                                    oninput: move |e| { *filter_text.write() = e.value(); },
                                }
                                span { class: "seg",
                                    button {
                                        class: if !show_warn { "on" } else { "" },
                                        onclick: move |_| { warn_only.set(false); },
                                        "all"
                                    }
                                    button {
                                        class: if show_warn { "on" } else { "" },
                                        onclick: move |_| { warn_only.set(true); },
                                        "⚠"
                                    }
                                }
                                button {
                                    class: "btn ghost sq",
                                    style: "font-size:10px;padding:1px 6px;height:22px;",
                                    title: "Select all visible",
                                    onclick: move |_| {
                                        let mut s = state.write();
                                        s.schema.selected_table_indices =
                                            vi_all.iter().copied().collect();
                                        if let Some(&last) = vi_all.last() {
                                            s.schema.last_selected_idx = last;
                                        }
                                    },
                                    "⊕ all"
                                }
                            }

                            // table rows
                            div { class: "pane-body no-pad",
                                TableListPanel {
                                    rows: table_rows,
                                    show_checkboxes: true,
                                    on_select_children: move |i| {
                                        let s = state.read();
                                        let children = select_children_visible(
                                            &s.schema.schemas, i, &vi_children,
                                        );
                                        drop(s);
                                        if !children.is_empty() {
                                            let mut s = state.write();
                                            s.schema.selected_table_indices = children;
                                            s.schema.last_selected_idx = i;
                                        }
                                    },
                                    on_select: move |(i, modifiers): (usize, Modifiers)| {
                                        let shift = modifiers.contains(Modifiers::SHIFT);
                                        let ctrl  = modifiers.contains(Modifiers::CONTROL)
                                            || modifiers.contains(Modifiers::META);
                                        if shift {
                                            let anchor = *anchor_idx.read();
                                            state.write().schema.apply_shift_click(i, anchor, &visible_indices);
                                        } else {
                                            state.write().schema.apply_click(i, ctrl);
                                            anchor_idx.set(i);
                                        }
                                    },
                                }

                                // multi-select sticky footer
                                if is_multi {
                                    div { style: "position:sticky;bottom:0;padding:8px 12px;background:var(--bg-1);border-top:1px solid var(--bd);display:flex;justify-content:space-between;align-items:center;",
                                        span { style: "font-size:var(--fs-sm);",
                                            b { style: "color:var(--acc);", "{selection_count} tables" }
                                            " selected"
                                        }
                                        button {
                                            class: "btn ghost sm",
                                            onclick: move |_| {
                                                let last = state.read().schema.last_selected_idx;
                                                state.write().schema.selected_table_indices =
                                                    std::collections::HashSet::from([last]);
                                            },
                                            "Clear"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                div { class: "splitter" }

                // ── CENTER — column detail / selection summary ─────────────
                div { class: "pane fluid",

                    if is_multi {
                        // ── Multi-select summary ──────────────────────────
                        {
                            let total_cols: usize = selected_indices.iter()
                                .filter_map(|&i| schemas.get(i))
                                .map(|t| t.columns.len()).sum();
                            rsx! {
                                div { class: "pane-head",
                                    span { class: "ttl", "⚡ Selection summary" }
                                    span { class: "meta", "{selection_count} tables · {total_cols} cols total" }
                                }
                                div { class: "pane-body",
                                    div { class: "alert info compact mb-sm",
                                        span { "⚡" }
                                        div {
                                            "You have "
                                            b { "{selection_count} tables" }
                                            " selected. Choose a bulk strategy in the right panel."
                                        }
                                    }
                                    h4 { style: "margin:14px 0 8px;font-size:var(--fs-md);color:var(--fg);", "Selected tables" }
                                    div { class: "card", style: "overflow:hidden;",
                                        table { class: "t",
                                            thead {
                                                tr {
                                                    th { "name" }
                                                    th { class: "ta-r", "cols" }
                                                    th { "current strategy" }
                                                }
                                            }
                                            tbody {
                                                for &i in selected_indices.iter() {
                                                    if let Some(t) = schemas.get(i) {
                                                        {
                                                            let (bc, bl) = if let Some(ov) = overrides_snap.get(&t.name) {
                                                                user_override_badge(ov)
                                                            } else {
                                                                strategy_badge(&t.inferred_strategy)
                                                            };
                                                            let is_indented = t.depth > 0;
                                                            rsx! {
                                                                tr { key: "{t.name}",
                                                                    td { class: "mono",
                                                                        if is_indented { span { class: "fg-4", "└ " } }
                                                                        "{t.name}"
                                                                    }
                                                                    td { class: "ta-r mono fg-2", "{t.columns.len()}" }
                                                                    td { span { class: "badge {bc}", "{bl}" } }
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
                        }
                    } else {
                        // ── Single-table column detail ────────────────────
                        {
                            let is_overflow = overflow_names.contains(&selected_table.name)
                                && !overrides_snap.contains_key(&selected_table.name);
                            rsx! {
                                div { class: "pane-head",
                                    span { class: "ttl",
                                        b { style: "font-family:'JetBrains Mono',monospace;color:var(--fg);",
                                            "{selected_table.name}"
                                        }
                                    }
                                    span { class: "meta", "{selected_table.columns.len()} cols" }
                                    span { class: "grow" }
                                }
                                div { class: "pane-body",
                                    if is_overflow {
                                        div { class: "alert warn mb-sm",
                                            span { "⚠" }
                                            div { b { "Wide table" } " — exceeded PostgreSQL 1600-column limit. Auto-converted to JSONB." }
                                        }
                                    }
                                    div { class: "card", style: "overflow:hidden;",
                                        table { class: "t",
                                            thead {
                                                tr {
                                                    th { "Column" }
                                                    th { "Type" }
                                                }
                                            }
                                            tbody {
                                                for col in selected_table.columns.iter() {
                                                    {
                                                        let type_str = col.pg_type.as_sql();
                                                        let name_style = if col.is_generated {
                                                            "color:var(--fg-3);"
                                                        } else {
                                                            ""
                                                        };
                                                        rsx! {
                                                            tr { key: "{col.name}",
                                                                td { class: "mono", style: "{name_style}", "{col.name}" }
                                                                td { class: "mono fg-2", "{type_str}" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    // strategy footer
                                    div { class: "row mt-md fs-xs fg-3",
                                        span {
                                            b { class: "fg-2", "Strategy:" }
                                            " {strategy_label(inferred_strategy)}"
                                        }
                                        if let Some(ref parent) = selected_table.parent_table {
                                            span { "·" }
                                            span {
                                                b { class: "fg-2", "Parent:" }
                                                span { class: "mono", " {parent}" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                div { class: "splitter" }

                // ── RIGHT — strategy configurator ─────────────────────────
                {
                    let rc = *right_collapsed.read();
                    rsx! {
                        div {
                            class: if rc { "pane collapsed" } else { "pane" },
                            style: "flex:0 0 340px;",

                            // collapsed strip
                            div {
                                class: "collapsed-strip",
                                onclick: move |_| { right_collapsed.set(false); },
                                span { class: "lbl", if is_multi { "Bulk Strategy" } else { "Strategy" } }
                                span { style: "font-size:18px;color:var(--fg-3);", "⊞" }
                            }

                            // pane head
                            div { class: "pane-head",
                                span { class: "ttl",
                                    if is_multi {
                                        "⚡ {selection_count} tables selected"
                                    } else {
                                        "Strategy"
                                    }
                                }
                                span { class: "grow" }
                                button {
                                    class: "btn ghost sm",
                                    title: if is_multi { "Reset all selected to auto-detected" } else { "Reset to auto-detected" },
                                    onclick: move |_| {
                                        let mut s = state.write();
                                        if is_multi {
                                            let names: Vec<String> = s.schema.selected_table_indices
                                                .iter()
                                                .filter_map(|&i| s.schema.schemas.get(i).map(|t| t.name.clone()))
                                                .collect();
                                            for name in names {
                                                s.schema.strategy_overrides.remove(&name);
                                            }
                                        } else {
                                            let name = s.schema.schemas[idx].name.clone();
                                            s.schema.strategy_overrides.remove(&name);
                                        }
                                    },
                                    "↩"
                                }
                                button {
                                    class: "collapse-btn",
                                    title: "Collapse",
                                    onclick: move |_| { right_collapsed.set(!rc); },
                                    if rc { "◀" } else { "▶" }
                                }
                            }

                            // pane body
                            div { class: "pane-body",
                                if is_multi {
                                    // ── Jaccard similarity ────────────────
                                    div { style: "padding:12px 14px;border-bottom:1px solid var(--bd);",
                                        div { style: "display:flex;align-items:center;gap:8px;margin-bottom:6px;",
                                            span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-weight:600;text-transform:uppercase;letter-spacing:.05em;",
                                                "Jaccard similarity"
                                            }
                                            span { style: "font-size:var(--fs-sm);font-weight:700;color:{jaccard_color};margin-left:auto;",
                                                "{jaccard_score_pct}"
                                            }
                                        }
                                        div { style: "height:6px;border-radius:3px;background:var(--bg-2);overflow:hidden;",
                                            div { style: "height:100%;border-radius:3px;background:{jaccard_color};width:{jaccard_score_pct};transition:width .2s;" }
                                        }
                                        div { style: "display:flex;justify-content:space-between;margin-top:5px;",
                                            span { style: "font-size:var(--fs-xs);color:var(--fg-4);", "{jaccard_label}" }
                                            span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;",
                                                "{jaccard_ratio_txt}"
                                            }
                                        }
                                    }

                                    // ── Bulk strategy ─────────────────────
                                    div { class: "section",
                                        h4 { "Bulk strategy" }
                                        div { class: "sub-h",
                                            "Applied to all {selection_count} tables. Per-table strategies (normalize, pivot) must be set individually."
                                        }
                                        div { class: "strat-list",
                                            // Default
                                            button {
                                                class: "strat-btn",
                                                onclick: move |_| {
                                                    let names: Vec<String> = {
                                                        let s = state.read();
                                                        s.schema.selected_table_indices.iter()
                                                            .filter_map(|&i| s.schema.schemas.get(i).map(|t| t.name.clone()))
                                                            .collect()
                                                    };
                                                    let mut s = state.write();
                                                    for n in names { s.schema.strategy_overrides.remove(&n); }
                                                },
                                                span { class: "radio" }
                                                span { class: "nm",
                                                    "Default"
                                                    span { class: "dsc", "one row per JSON object" }
                                                }
                                                span { class: "badge default", "default" }
                                            }
                                            // JSONB
                                            button {
                                                class: "strat-btn",
                                                onclick: move |_| {
                                                    let names: Vec<String> = {
                                                        let s = state.read();
                                                        s.schema.selected_table_indices.iter()
                                                            .filter_map(|&i| s.schema.schemas.get(i).map(|t| t.name.clone()))
                                                            .collect()
                                                    };
                                                    let mut s = state.write();
                                                    for n in names { s.schema.strategy_overrides.insert(n, UserOverride::Jsonb); }
                                                },
                                                span { class: "radio" }
                                                span { class: "nm",
                                                    "JSONB — separate table"
                                                    span { class: "dsc", "payload in dedicated child table" }
                                                }
                                                span { class: "badge jsonb", "jsonb" }
                                            }
                                            // Pivot
                                            button {
                                                class: "strat-btn",
                                                onclick: move |_| {
                                                    let names: Vec<String> = {
                                                        let s = state.read();
                                                        s.schema.selected_table_indices.iter()
                                                            .filter_map(|&i| s.schema.schemas.get(i).map(|t| t.name.clone()))
                                                            .collect()
                                                    };
                                                    let mut s = state.write();
                                                    for n in names { s.schema.strategy_overrides.insert(n, UserOverride::Pivot); }
                                                },
                                                span { class: "radio" }
                                                span { class: "nm",
                                                    "Pivot (EAV)"
                                                    span { class: "dsc", "entity-attribute-value" }
                                                }
                                                span { class: "badge pivot", "pivot" }
                                            }
                                            // Skip
                                            button {
                                                class: "strat-btn",
                                                onclick: move |_| {
                                                    let names: Vec<String> = {
                                                        let s = state.read();
                                                        s.schema.selected_table_indices.iter()
                                                            .filter_map(|&i| s.schema.schemas.get(i).map(|t| t.name.clone()))
                                                            .collect()
                                                    };
                                                    let mut s = state.write();
                                                    for n in names { s.schema.strategy_overrides.insert(n, UserOverride::Skip); }
                                                },
                                                span { class: "radio" }
                                                span { class: "nm",
                                                    "Skip"
                                                    span { class: "dsc", "exclude from import" }
                                                }
                                                span { class: "badge skip", "skip" }
                                            }
                                        }
                                    }
                                } else {
                                    // ── Single-table strategy ─────────────
                                    div { class: "strat-list",
                                        // Default (= no user override — keep auto-detected)
                                        button {
                                            class: if current_override.is_none() { "strat-btn on" } else { "strat-btn" },
                                            onclick: move |_| {
                                                let name = state.read().schema.schemas[idx].name.clone();
                                                state.write().schema.strategy_overrides.remove(&name);
                                            },
                                            span { class: "radio" }
                                            span { class: "nm",
                                                "Default"
                                                span { class: "dsc", "use auto-detected strategy" }
                                            }
                                            span { class: "badge default", "default" }
                                        }
                                        // JSONB separate
                                        button {
                                            class: if matches!(current_override, Some(UserOverride::Jsonb)) { "strat-btn on" } else { "strat-btn" },
                                            onclick: move |_| {
                                                let name = state.read().schema.schemas[idx].name.clone();
                                                state.write().schema.strategy_overrides.insert(name, UserOverride::Jsonb);
                                            },
                                            span { class: "radio" }
                                            span { class: "nm",
                                                "JSONB — separate table"
                                                span { class: "dsc", "payload in dedicated child table" }
                                            }
                                            span { class: "badge jsonb", "jsonb" }
                                        }
                                        // Pivot
                                        button {
                                            class: if matches!(current_override, Some(UserOverride::Pivot)) { "strat-btn on" } else { "strat-btn" },
                                            onclick: move |_| {
                                                let name = state.read().schema.schemas[idx].name.clone();
                                                state.write().schema.strategy_overrides.insert(name, UserOverride::Pivot);
                                            },
                                            span { class: "radio" }
                                            span { class: "nm",
                                                "Pivot (EAV)"
                                                span { class: "dsc", "key/value rows in 3-column table" }
                                            }
                                            span { class: "badge pivot", "pivot" }
                                        }
                                        // Skip
                                        button {
                                            class: if matches!(current_override, Some(UserOverride::Skip)) { "strat-btn on" } else { "strat-btn" },
                                            onclick: move |_| {
                                                let name = state.read().schema.schemas[idx].name.clone();
                                                state.write().schema.strategy_overrides.insert(name, UserOverride::Skip);
                                            },
                                            span { class: "radio" }
                                            span { class: "nm",
                                                "Skip"
                                                span { class: "dsc", "exclude from import entirely" }
                                            }
                                            span { class: "badge skip", "skip" }
                                        }
                                    }

                                    // Auto-detected notice
                                    if current_override.is_none() && matches!(inferred_strategy,
                                        InferredStrategy::StructuredPivot(_) | InferredStrategy::SiblingCollapse(_) | InferredStrategy::AutoSplit { .. }
                                    ) {
                                        p { style: "font-size:var(--fs-xs);color:var(--fg-4);margin-top:12px;font-style:italic;line-height:1.5;",
                                            "Auto-detected strategy — configurable via CLI only. Override above if needed."
                                        }
                                    }

                                    div { style: "margin-top:auto;padding-top:16px;border-top:1px solid var(--bd);margin-top:20px;",
                                        p { style: "font-size:var(--fs-xs);color:var(--fg-4);line-height:1.6;",
                                            "Hold "
                                            span { class: "kbd", "⇧" }
                                            " to range-select or "
                                            span { class: "kbd", "⌃" }
                                            "+click to add to selection."
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } // split-3

            // ── Bottom action bar ─────────────────────────────────────────
            div { style: "border-top:1px solid var(--bd);padding:10px 16px;background:var(--bg-1);display:flex;justify-content:space-between;align-items:center;flex-shrink:0;",
                div { class: "row gap-md",
                    if overflow_count > 0 {
                        span { class: "badge warn",
                            "⚠ {overflow_count} table(s) auto-converted to JSONB"
                        }
                    }
                    span { class: "fs-xs fg-3",
                        if is_multi {
                            "bulk strategy active — {selection_count} tables"
                        } else {
                            "select 2+ tables to see bulk options"
                        }
                    }
                }
                div { class: "row gap-md",
                    button {
                        class: "btn secondary",
                        onclick: move |_| { state.write().screen = AppScreen::Analysis; },
                        "‹ Analysis"
                    }
                    button {
                        class: "btn primary",
                        onclick: move |_| { state.write().screen = AppScreen::Preview; },
                        "Preview SQL schema ›"
                    }
                }
            }
        }
    }
}
