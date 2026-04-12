/// Screen 2 — Schema Analysis (Pass 1)
///
/// Split layout: left 60% log panel, right 40% live stats.
/// Launches the Pass 1 runner on mount via a Dioxus coroutine.
use dioxus::prelude::*;

use json2sql::io::progress_event::ProgressEvent;

use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn AnalysisScreen(mut state: Signal<AppState>) -> Element {
    let progress = state.read().pass1_progress.clone();
    let pct = if progress.total_bytes > 0 {
        (progress.bytes_read as f64 / progress.total_bytes as f64 * 100.0) as u32
    } else {
        0
    };
    let btn_style = if progress.done {
        theme::STYLE_BTN_PRIMARY.to_string()
    } else {
        format!("{}opacity:0.4;", theme::STYLE_BTN_PRIMARY)
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
                .unwrap_or("root")
                .to_string();
            (path, root)
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        // Run Pass 1 in a blocking thread — it's CPU/IO bound.
        let handle = tokio::task::spawn_blocking(move || {
            json2sql::pass1::runner::run(
                &source_file,
                &root_table,
                256,   // text_threshold
                false, // array_as_pg_array
                100,   // wide_column_threshold
                3,     // sibling_threshold
                0.5,   // sibling_jaccard
                0.10,  // stable_threshold
                0.001, // rare_threshold
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
                    if progress.done { "Schema ready" } else { "Analyzing schema…" }
                }
            }

            // Main split area
            div {
                style: "display:flex;flex:1;overflow:hidden;",

                // Left — log panel (60%)
                div {
                    style: "flex:0 0 60%;{theme::STYLE_LOG_PANEL}overflow-y:auto;",
                    for line in progress.log_lines.iter() {
                        p { style: "margin:2px 0;", "{line}" }
                    }
                }

                // Right — live counters (40%)
                div {
                    style: "flex:0 0 40%;background:{theme::BG_SIDEBAR};padding:24px;",
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
