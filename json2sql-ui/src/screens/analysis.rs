/// Screen 2 — Schema Analysis (Pass 1)
///
/// Split layout: left 60% log panel, right 40% live stats.
/// Launches the Pass 1 runner on mount via a Dioxus coroutine.

// Pass 1 configuration constants
const TEXT_THRESHOLD: u32 = 256;
const WIDE_COLUMN_THRESHOLD: usize = 100;
const SIBLING_THRESHOLD: usize = 3;
const SIBLING_JACCARD: f64 = 0.5;
const STABLE_THRESHOLD: f64 = 0.10;
const RARE_THRESHOLD: f64 = 0.001;
const MAX_VISIBLE_LOG_LINES: usize = 1000;

use dioxus::prelude::*;

use json2sql::io::progress_event::ProgressEvent;

use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn AnalysisScreen(mut state: Signal<AppState>) -> Element {
    let progress = state.read().pass1_progress.clone();
    let pct = if progress.total_bytes > 0 {
        (progress.bytes_read as f64 / progress.total_bytes as f64 * 100.0) as u32
    } else if progress.rows_scanned > 0 {
        // Show some progress even when total_bytes is unknown
        ((progress.rows_scanned % 100) as u32).min(90)
    } else {
        0
    };
    let btn_style = if progress.done {
        theme::STYLE_BTN_PRIMARY.to_string()
    } else {
        format!("{}opacity:0.4;", theme::STYLE_BTN_PRIMARY)
    };

    // Determine status text based on current state
    let status_text = if progress.done {
        "Schema ready"
    } else if progress.rows_scanned == 0 && progress.bytes_read == 0 {
        "Starting analysis..."
    } else {
        "Analyzing schema..."
    };

    // Launch Pass 1 once on mount. The coroutine runs until the component unmounts.
    use_coroutine(move |_: UnboundedReceiver<()>| async move {
        // Derive root table name from file stem (same logic as CLI).
        let (source_file, root_table) = {
            let source_file_opt = state.read().source_file.clone();
            let Some(path) = source_file_opt else {
                // Should never happen — Setup disables "Start" when no file is selected.
                state.write().cancel();
                return;
            };
            let root = path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "root".to_string());
            (path, root)
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        // Run Pass 1 in a blocking thread — it's CPU/IO bound.
        let handle = tokio::task::spawn_blocking(move || {
            json2sql::pass1::runner::run(
                &source_file,
                &root_table,
                TEXT_THRESHOLD,
                false, // array_as_pg_array
                WIDE_COLUMN_THRESHOLD,
                SIBLING_THRESHOLD,
                SIBLING_JACCARD,
                STABLE_THRESHOLD,
                RARE_THRESHOLD,
                Some(tx),
            )
        });

        state.write().abort_handle = Some(handle.abort_handle());

        // Drain progress events into AppState — drives the UI reactively.
        while let Some(event) = rx.recv().await {
            let done = matches!(event, ProgressEvent::Pass1Done { .. });
            state.write().apply_progress_event(event);
            if done {
                break;
            }
        }

        // Retrieve schemas from the completed result.
        match handle.await {
            Ok(Ok(result)) => {
                state.write().schemas = result.schemas;
            }
            Ok(Err(e)) => {
                state
                    .write()
                    .pass1_progress
                    .push_log(format!("Pass 1 error: {e}"));
            }
            Err(_) => {
                // Task was aborted (Cancel button) — state already reset by cancel().
            }
        }

        state.write().abort_handle = None;
    });

    rsx! {
        div {
            style: "display:flex;flex-direction:column;height:100vh;background:{theme::BG_ROOT};",

            // Header
            div {
                style: "padding:16px 24px;background:{theme::BG_WORKSPACE};",
                span {
                    style: "color:{theme::ON_SURFACE};font-size:1rem;font-weight:600;",
                    "{status_text}"
                }
            }

            // Main split area
            div {
                style: "display:flex;flex:1;overflow:hidden;min-height:0;min-width:0;",

                // Left — log panel (60%)
                div {
                    style: "flex:0 1 60%;min-width:0;box-sizing:border-box;{theme::STYLE_LOG_PANEL}overflow-y:auto;",
                    for line in progress.log_lines.iter().rev().take(MAX_VISIBLE_LOG_LINES).rev() {
                        p { style: "margin:2px 0;", "{line}" }
                    }
                    if progress.log_lines.len() > MAX_VISIBLE_LOG_LINES {
                        p {
                            style: "margin:8px 0;color:{theme::ON_SURFACE_VARIANT};font-size:0.75rem;font-style:italic;",
                            "... and {progress.log_lines.len() - MAX_VISIBLE_LOG_LINES} more lines"
                        }
                    }
                }

                // Right — live counters (40%)
                div {
                    style: "flex:0 1 40%;min-width:0;min-height:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};padding:24px;",
                    p { style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.875rem;margin:0 0 8px 0;", "Tables detected: {progress.tables_count}" }
                    p { style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.875rem;margin:0 0 8px 0;", "Columns total: {progress.columns_count}" }
                    p { style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.875rem;margin:0;", "Records scanned: {progress.rows_scanned}" }
                }
            }

            // Bottom bar — progress + buttons
            div {
                style: "padding:16px 24px;background:{theme::BG_WORKSPACE};",
                div { style: "{theme::STYLE_PROGRESS_TRACK}margin-bottom:12px;",
                    div { style: "{theme::STYLE_PROGRESS_BAR}width:{pct}%;", "" }
                }
                div { style: "display:flex;justify-content:space-between;",
                    button {
                        style: "{theme::STYLE_BTN_GHOST}",
                        onclick: move |_| { state.write().cancel(); },
                        "Cancel"
                    }
                    button {
                        style: "{btn_style}",
                        disabled: !progress.done,
                        onclick: move |_| {
                            state.write().screen = AppScreen::Strategy;
                        },
                        "Continue to Schema Review →"
                    }
                }
            }
        }
    }
}
