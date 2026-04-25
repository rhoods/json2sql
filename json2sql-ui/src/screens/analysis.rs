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
use dioxus::prelude::*;

use json2sql::io::progress_event::ProgressEvent;

use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn AnalysisScreen(mut state: Signal<AppState>) -> Element {
    let progress = state.read().pass1_progress.clone();
    let pct = if progress.done {
        100
    } else if progress.total_bytes > 0 {
        (progress.bytes_read as f64 / progress.total_bytes as f64 * 100.0) as u32
    } else if progress.rows_scanned > 0 {
        // total_bytes unknown — show monotonically increasing fake progress capped at 89%.
        // Increments by 1% per 1 000 rows scanned; never oscillates, never reaches 100.
        ((progress.rows_scanned / 1_000) as u32).min(89)
    } else {
        0
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
        // Guard: abort_handle is Some while a runner is in flight.
        // If it is already set, a previous coroutine instance is still active
        // (Dioxus can re-run use_coroutine on component remount); bail out
        // immediately to avoid running two Pass 1 instances concurrently.
        if state.read().abort_handle.is_some() {
            return;
        }

        // Reset progress only after confirming no runner is active.
        state.write().pass1_progress = crate::state::Pass1Progress::default();

        // Derive root table name from file stem (same logic as CLI).
        let (source_file, root_table, workers) = {
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
            let workers = state.read().workers;
            (path, root, workers)
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        // Run Pass 1 in a blocking thread — it's CPU/IO bound.
        let handle = tokio::task::spawn_blocking(move || {
            if workers > 1 {
                json2sql::pass1::runner::run_parallel(
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
                    workers,
                )
            } else {
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
            }
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
                    class: "log-panel",
                    style: "flex:0 1 60%;min-width:0;box-sizing:border-box;",
                    for line in progress.log_lines.iter() {
                        p { style: "margin:2px 0;", "{line}" }
                    }
                }

                // Right — live counters (40%)
                div {
                    style: "flex:0 1 40%;min-width:0;min-height:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};padding:24px;",
                    {
                        let detecting = progress.rows_scanned > 0 && !progress.done;
                        let tables_str = if detecting && progress.tables_count == 0 {
                            "Detecting…".to_string()
                        } else {
                            progress.tables_count.to_string()
                        };
                        let cols_str = if detecting && progress.columns_count == 0 {
                            "Detecting…".to_string()
                        } else {
                            progress.columns_count.to_string()
                        };
                        rsx! {
                            p { style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.875rem;margin:0 0 8px 0;", "Tables detected: {tables_str}" }
                            p { style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.875rem;margin:0 0 8px 0;", "Columns total: {cols_str}" }
                            p { style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.875rem;margin:0;", "Records scanned: {progress.rows_scanned}" }
                        }
                    }
                }
            }

            // Bottom bar — progress + buttons
            div {
                style: "padding:16px 24px;background:{theme::BG_WORKSPACE};",
                div { class: "progress-track", style: "margin-bottom:12px;",
                    div { class: "progress-bar", style: "width:{pct}%;", "" }
                }
                div { style: "display:flex;justify-content:space-between;",
                    button {
                        class: "btn-ghost",
                        onclick: move |_| { state.write().cancel(); },
                        "Cancel"
                    }
                    button {
                        class: "btn-primary",
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
