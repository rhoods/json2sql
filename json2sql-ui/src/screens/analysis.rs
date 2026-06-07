//! Screen 2 — Schema Analysis (Pass 1) — dashboard cards style.
//!
//! Layout:
//!   subbar + breadcrumb + pulsing dot
//!   4-up stat tiles (tables · columns · records · log lines)
//!   large progress card
//!   two-column grid: schema overview | latest events log
//!   bottom bar: Cancel · Continue →
#![allow(clippy::disallowed_methods, clippy::derive_partial_eq_without_eq)]
use dioxus::prelude::*;

use json2sql::io::progress_event::ProgressEvent;

use crate::screens::{strategy_badge, progress_pct, ProgressBar};
use crate::state::{
    format_bytes, AppState, AppScreen,
    PASS1_TEXT_THRESHOLD, PASS1_WIDE_COLUMN_THRESHOLD, PASS1_SIBLING_THRESHOLD,
    PASS1_SIBLING_JACCARD, PASS1_STABLE_THRESHOLD, PASS1_RARE_THRESHOLD,
};

#[allow(clippy::cast_precision_loss)]
fn format_count(n: u64) -> String {
    if n >= 1_000_000 { format!("{:.1}M rec/s", n as f64 / 1_000_000.0) }
    else if n >= 1_000 { format!("{:.1}K rec/s", n as f64 / 1_000.0) }
    else { format!("{n} rec/s") }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[allow(clippy::derive_partial_eq_without_eq)]
#[component]
pub fn AnalysisScreen(mut state: Signal<AppState>) -> Element {
    // ── Elapsed timer ─────────────────────────────────────────────────────
    let elapsed_secs = crate::screens::use_elapsed_timer(move || state.read().schema.pass1_progress.done);

    // ── Pass 1 runner ────────────────────────────────────────────────────
    // once-flag: set synchronously (before first await) to prevent double-launch
    // if Dioxus remounts this component before abort_handle is written.
    let mut once = use_signal(|| false);
    use_coroutine(move |_: UnboundedReceiver<()>| async move {
        if *once.peek() { return; }
        once.set(true);
        if state.read().abort_handle.is_some() { return; }
        state.write().schema.pass1_progress = crate::state::Pass1Progress::default();

        let (source_file, root_table, workers) = {
            let source_file_opt = state.read().project.source_file.clone();
            let Some(path) = source_file_opt else {
                state.write().cancel();
                return;
            };
            let root = path.file_stem()
                .and_then(|s| s.to_str())
                .map_or_else(|| "root".to_string(), std::string::ToString::to_string);
            let workers = state.read().project.workers;
            (path, root, workers)
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
        let (workers, capped) = json2sql::pass1::runner::effective_workers(workers);
        if let Some(cap) = capped {
            state.write().schema.pass1_progress.push_log(format!(
                "WARNING: workers clamped to {cap} (requested value exceeds available CPUs)"
            ));
        }

        let disabled_strategies = state.read().project.disabled_strategies.clone();
        let handle = tokio::task::spawn_blocking(move || {
            let config = json2sql::pass1::runner::Pass1Config {
                root_table,
                text_threshold: PASS1_TEXT_THRESHOLD,
                array_as_pg_array: false,
                wide_column_threshold: PASS1_WIDE_COLUMN_THRESHOLD,
                sibling_threshold: PASS1_SIBLING_THRESHOLD,
                sibling_jaccard: PASS1_SIBLING_JACCARD,
                stable_threshold: PASS1_STABLE_THRESHOLD,
                rare_threshold: PASS1_RARE_THRESHOLD,
                disabled_strategies,
                num_workers: Some(workers),
            };
            if workers > 1 {
                json2sql::pass1::runner::run_parallel(&source_file, &config, Some(tx))
            } else {
                json2sql::pass1::runner::run(&source_file, &config, Some(tx))
            }
        });

        state.write().abort_handle = Some(handle.abort_handle());

        while let Some(event) = rx.recv().await {
            let done = matches!(event, ProgressEvent::Pass1Done { .. });
            state.write().apply_progress_event(event);
            if done { break; }
        }

        match handle.await {
            Ok(Ok(result)) => {
                let mut s = state.write();
                s.schema.schemas = result.schemas;
                s.schema.overflow_warnings = result.overflow_warnings;
                s.schema.truncated_names = result.truncated_names;
                s.schema.column_collisions = result.column_collisions;
                s.schema.pass1_stats = result.stats;
                s.schema.schema_snapshot_loaded = false;
            }
            Ok(Err(e)) => {
                state.write().schema.pass1_progress.push_log(format!("Pass 1 error: {e}"));
            }
            Err(_) => {}
        }

        state.write().abort_handle = None;
    });

    // ── Derived values ────────────────────────────────────────────────────
    let progress  = state.read().schema.pass1_progress.clone();
    let schemas   = state.read().schema.schemas.clone();
    let done      = progress.done;
    let elapsed   = *elapsed_secs.read();
    let workers   = state.read().project.workers;

    let pct: u32 = if done { 100 }
        else if progress.total_bytes > 0 {
            progress_pct(progress.bytes_read, progress.total_bytes)
        } else if progress.rows_scanned > 0 {
            #[allow(clippy::cast_possible_truncation)]
            { ((progress.rows_scanned / 1_000) as u32).min(89) }
        } else { 0 };

    let rate = if elapsed > 0 { progress.rows_scanned / u64::from(elapsed) } else { 0 };
    let eta_str = if pct > 0 && pct < 100 && elapsed > 0 {
        let remaining = u64::from(elapsed) * (100 - u64::from(pct)) / u64::from(pct);
        format!("~{:02}:{:02}", remaining / 60, remaining % 60)
    } else if done { "done".to_string() } else { "—".to_string() };

    let elapsed_str = format!("{:02}:{:02}", elapsed / 60, elapsed % 60);
    let rows_str = if progress.rows_scanned > 0 {
        let n = progress.rows_scanned;
        #[allow(clippy::cast_precision_loss)]
        if n >= 1_000_000 { format!("{:.1}M", n as f64 / 1_000_000.0) }
        else if n >= 1_000 { format!("{}K", n / 1_000) }
        else { n.to_string() }
    } else { "0".to_string() };

    let file_pct_str = if progress.total_bytes > 0 {
        format_bytes(progress.bytes_read)
    } else { String::new() };

    let status_label = if done { "Schema ready" } else { "Analyzing schema" };
    let scanning_label = if done { "Pass 1 complete" } else { "Pass 1 · scanning" };

    #[allow(clippy::cast_precision_loss)]
    let cols_str = if progress.columns_count >= 1_000_000 {
        format!("{:.1}M", progress.columns_count as f64 / 1_000_000.0)
    } else if progress.columns_count >= 1_000 {
        format!("{:.1}K", progress.columns_count as f64 / 1_000.0)
    } else {
        progress.columns_count.to_string()
    };
    let cols_avg_str = if progress.tables_count > 0 {
        format!("avg {} / table", progress.columns_count / progress.tables_count.max(1))
    } else {
        "—".to_string()
    };
    let rate_str = if rate > 0 {
        format_count(rate)
    } else {
        "—".to_string()
    };

    // ── Render ────────────────────────────────────────────────────────────
    rsx! {
        div { style: "display:flex;flex-direction:column;height:100vh;background:var(--bg);",

            // ── Subbar ────────────────────────────────────────────────────
            div { class: "subbar",
                div { class: "crumb",
                    button {
                        class: "step",
                        onclick: move |_| { state.write().cancel(); },
                        "Setup"
                    }
                    span { class: "sep", "›" }
                    button { class: "step active", "Analysis" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Strategy" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "SQL Preview" }
                }
                span { class: "grow" }
                span { class: "row gap-sm fs-xs fg-3",
                    if !done {
                        span { class: "pulse-dot" }
                    }
                    span { style: "color:var(--acc);font-weight:600;", "{scanning_label}" }
                    span { class: "mono", "— elapsed {elapsed_str}" }
                }
            }

            // ── Scrollable body ───────────────────────────────────────────
            div { style: "flex:1;overflow:auto;padding:18px 24px 24px;background:var(--bg);",

                // Header row
                div { class: "row gap-md mb-sm", style: "margin-bottom:14px;",
                    h2 { style: "margin:0;font-size:18px;font-weight:600;color:var(--fg);",
                        "{status_label}"
                    }
                    span { class: "badge info", style: "height:22px;", "PASS 1 / 2" }
                    span { class: "grow" }
                    if !done {
                        button {
                            class: "btn danger sm",
                            onclick: move |_| { state.write().cancel(); },
                            "✕ Cancel"
                        }
                    }
                }

                // ── 4-up stat tiles ───────────────────────────────────────
                div { style: "display:grid;grid-template-columns:repeat(4,1fr);gap:14px;",

                    // Tables detected
                    div { class: "stat-tile", style: "position:relative;",
                        span { class: "lbl", "Tables detected" }
                        span { class: "val", "{progress.tables_count}" }
                        span { class: "sub",
                            if done { "analysis complete" } else { "discovering…" }
                        }
                        svg {
                            class: "spark",
                            width: "60", height: "20", view_box: "0 0 60 20",
                            polyline {
                                points: "0,18 10,16 20,14 30,11 40,8 50,5 60,2",
                                fill: "none", stroke: "var(--info)", stroke_width: "1.5"
                            }
                        }
                    }

                    // Columns total
                    div { class: "stat-tile", style: "position:relative;",
                        span { class: "lbl", "Columns total" }
                        span { class: "val", "{cols_str}" }
                        span { class: "sub", "{cols_avg_str}" }
                        svg {
                            class: "spark",
                            width: "60", height: "20", view_box: "0 0 60 20",
                            polyline {
                                points: "0,18 10,15 20,12 30,9 40,6 50,3 60,1",
                                fill: "none", stroke: "var(--info)", stroke_width: "1.5"
                            }
                        }
                    }

                    // Records scanned
                    div { class: "stat-tile acc", style: "position:relative;",
                        span { class: "lbl", "Records scanned" }
                        span { class: "val",
                            "{rows_str}"
                            if !file_pct_str.is_empty() {
                                span { class: "u", " / {file_pct_str}" }
                            }
                        }
                        span { class: "sub", "{rate_str}" }
                        svg {
                            class: "spark",
                            width: "60", height: "20", view_box: "0 0 60 20",
                            polyline {
                                points: "0,18 10,14 20,11 30,8 40,5 50,3 60,1",
                                fill: "none", stroke: "var(--acc)", stroke_width: "1.5"
                            }
                        }
                    }

                    // Log events (proxy for activity)
                    div { class: "stat-tile", style: "position:relative;",
                        span { class: "lbl", "Log events" }
                        span { class: "val", "{progress.log_lines.len()}" }
                        span { class: "sub",
                            if done { "analysis complete" } else { "streaming…" }
                        }
                        svg {
                            class: "spark",
                            width: "60", height: "20", view_box: "0 0 60 20",
                            polyline {
                                points: "0,18 10,16 20,13 30,10 40,8 50,5 60,3",
                                fill: "none", stroke: "var(--fg-3)", stroke_width: "1.5"
                            }
                        }
                    }
                }

                // ── Progress card ─────────────────────────────────────────
                div { class: "card mt-lg", style: "padding:14px 18px;",
                    div { class: "row", style: "justify-content:space-between;margin-bottom:10px;",
                        span { style: "font-weight:600;font-size:var(--fs-md);color:var(--fg);",
                            "Pass 1 — schema discovery"
                        }
                        if rate > 0 {
                            span { class: "mono fs-sm fg-2", "{rate_str} · ETA {eta_str}" }
                        }
                    }
                    ProgressBar {
                        pct,
                        done,
                        label: if progress.total_bytes > 0 {
                            format!("{} / {} · {} elapsed",
                                format_bytes(progress.bytes_read),
                                format_bytes(progress.total_bytes),
                                elapsed_str)
                        } else {
                            format!("{elapsed_str} elapsed")
                        },
                        phase: "Scanning".to_string(),
                    }
                }

                // ── Two-column grid ───────────────────────────────────────
                div { style: "display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px;",

                    // Left: schema overview
                    div { class: "card",
                        div { class: "card-head",
                            span { class: "ti", "⬡ Schema overview" }
                            span { class: "count", "{schemas.len()}" }
                            span { class: "grow" }
                            span { class: "fs-xs fg-3 mono",
                                if done { "complete" } else { "live" }
                            }
                        }
                        div { class: "card-body", style: "padding:0;overflow-y:auto;max-height:360px;",
                            if schemas.is_empty() {
                                div { style: "padding:16px;text-align:center;",
                                    span { class: "fs-xs fg-3",
                                        if done { "No tables detected" } else { "Scanning…" }
                                    }
                                }
                            } else {
                                for table in schemas.iter() {
                                    {
                                        let (badge_cls, badge_lbl) = strategy_badge(&table.inferred_strategy);
                                        let is_wide = table.columns.len() > PASS1_WIDE_COLUMN_THRESHOLD;
                                        let col_count = table.columns.len();
                                        let col_style = if is_wide {
                                            "font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);color:var(--warning);font-weight:600;"
                                        } else {
                                            "font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);color:var(--fg-3);"
                                        };
                                        rsx! {
                                            div {
                                                key: "{table.name}",
                                                style: "padding:8px 12px;border-bottom:1px solid rgba(38,45,58,.5);display:flex;align-items:center;gap:8px;",
                                                span {
                                                    style: "font-family:'JetBrains Mono',monospace;font-size:var(--fs-sm);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--fg);",
                                                    "{table.name}"
                                                }
                                                span { style: "{col_style}", "{col_count}" }
                                                span { class: "badge {badge_cls}", "{badge_lbl}" }
                                                if is_wide { span { "⚠" } }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Right: latest events log
                    div { class: "card",
                        div { class: "card-head",
                            span { class: "ti", "Latest events" }
                            span { class: "grow" }
                            span { class: "fs-xs fg-3 mono", "{progress.log_lines.len()} lines" }
                        }
                        div { class: "card-body", style: "padding:0;",
                            div {
                                class: "log",
                                style: "border:none;border-radius:0;padding:10px 14px;max-height:360px;overflow-y:auto;line-height:1.6;",
                                for line in progress.log_lines.iter() {
                                    {
                                        let is_warn = line.contains("WARNING") || line.contains('⚠');
                                        let is_ok   = line.contains("complete") || line.contains("done");
                                        let line_class = if is_warn { "warn" } else if is_ok { "ok" } else { "" };
                                        rsx! {
                                            div {
                                                key: "{line}",
                                                class: "{line_class}",
                                                "{line}"
                                            }
                                        }
                                    }
                                }
                                if !done {
                                    div { style: "color:var(--success);", "▌" }
                                }
                            }
                        }
                    }
                }

                // ── Bottom bar ────────────────────────────────────────────
                div { class: "row mt-lg", style: "justify-content:space-between;padding-top:8px;",
                    div { class: "row gap-md fs-xs fg-3",
                        span {
                            b { class: "fg-2", "Workers" }
                            " "
                            span { class: "mono", "{workers} active" }
                        }
                        if progress.tables_count > 0 {
                            span { "·" }
                            span {
                                b { class: "fg-2", "Tables" }
                                " "
                                span { class: "mono", "{progress.tables_count}" }
                            }
                        }
                        if progress.columns_count > 0 {
                            span { "·" }
                            span {
                                b { class: "fg-2", "Columns" }
                                " "
                                span { class: "mono", "{progress.columns_count}" }
                            }
                        }
                    }
                    div { class: "row gap-md",
                        button {
                            class: "btn ghost",
                            onclick: move |_| { state.write().cancel(); },
                            "Cancel"
                        }
                        button {
                            class: "btn primary",
                            disabled: !done,
                            onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                            "Review schema ›"
                        }
                    }
                }

            } // scrollable body
        }
    }
}
