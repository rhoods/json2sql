/// Screen 5 — Import (Pass 2)
///
/// Split layout:
///   left 60%  — real-time import log
///   right 40% — per-table row counts
/// Launches the full Pass 2 pipeline on mount: connect → DDL → COPY.
use dioxus::prelude::*;

use json2sql::db::ddl;
use json2sql::io::progress_event::ProgressEvent;

use crate::state::AppState;
use crate::theme;

#[component]
pub fn ImportScreen(mut state: Signal<AppState>) -> Element {
    let progress = state.read().pass2_progress.clone();
    let pct = if progress.total_bytes > 0 {
        (progress.bytes_read as f64 / progress.total_bytes as f64 * 100.0) as u32
    } else {
        0
    };

    // Launch the full import pipeline once on mount.
    use_coroutine(move |_: UnboundedReceiver<()>| async move {
        let (source_file, root_table, pg_url, schemas, drop_existing) = {
            let source_file_opt = state.read().source_file.clone();
            let Some(path) = source_file_opt else {
                state.write().cancel();
                return;
            };
            let s = state.read();
            let root = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("root")
                .to_string();
            (path, root, s.pg.to_url(), s.schemas.clone(), s.drop_existing)
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        let handle = tokio::spawn(async move {
            // 1. Connect
            let (client, connection) =
                tokio_postgres::connect(&pg_url, tokio_postgres::NoTls).await?;
            tokio::spawn(async move {
                let _ = connection.await;
            });

            // 2. Create tables (behaviour driven by user's drop_existing choice)
            ddl::create_tables(&client, &schemas, "public", drop_existing).await?;

            // 3. Run Pass 2
            json2sql::pass2::runner::run(
                &source_file,
                &root_table,
                &schemas,
                &client,
                "public",
                100_000, // flush_threshold
                false,   // use_transaction
                None,    // db_url (parallel > 1 only)
                1,       // parallel
                None,    // anomaly_dir
                Some(tx),
            )
            .await
        });

        state.write().abort_handle = Some(handle.abort_handle());

        // Drain progress events into AppState.
        while let Some(event) = rx.recv().await {
            let done = matches!(event, ProgressEvent::Pass2Done { .. });
            state.write().apply_progress_event(event);
            if done {
                break;
            }
        }

        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                state
                    .write()
                    .pass2_progress
                    .push_log(format!("Import error: {e}"));
            }
            Err(_) => {
                // Aborted via Cancel — state already reset by cancel().
            }
        }

        state.write().abort_handle = None;
    });

    // Per-table rows, sorted descending by count for readability.
    let mut table_rows: Vec<(String, u64)> = progress
        .rows_per_table
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    table_rows.sort_by(|a, b| b.1.cmp(&a.1));

    let total_rows_known: u64 = table_rows.iter().map(|(_, n)| n).sum();

    rsx! {
        div {
            style: "display:flex;flex-direction:column;height:100vh;background:{theme::BG_ROOT};",

            // ── Header ───────────────────────────────────────────────────
            div {
                style: "padding:16px 24px;background:{theme::BG_WORKSPACE};",
                span {
                    style: "color:{theme::ON_SURFACE};font-size:1rem;font-weight:600;",
                    if progress.done { "Import complete" } else { "Importing data…" }
                }
            }

            // ── Main split ───────────────────────────────────────────────
            div {
                style: "display:flex;flex:1;overflow:hidden;",

                // Left — import log (60%)
                div {
                    style: "flex:0 0 60%;{theme::STYLE_LOG_PANEL}overflow-y:auto;",
                    for line in progress.log_lines.iter() {
                        p { style: "margin:2px 0;", "{line}" }
                    }
                }

                // Right — per-table row counts (40%)
                div {
                    style: "flex:0 0 40%;background:{theme::BG_SIDEBAR};padding:16px;overflow-y:auto;",
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;margin:0 0 12px 0;",
                        "Rows flushed"
                    }
                    if table_rows.is_empty() {
                        p { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "Waiting for first flush…" }
                    } else {
                        div {
                            style: "display:flex;flex-direction:column;gap:8px;",
                            for (table_name, count) in table_rows.iter() {
                                div {
                                    key: "{table_name}",
                                    // Progress bar proportional to the largest table
                                    {
                                        let bar_pct = if total_rows_known > 0 {
                                            (count * 100 / total_rows_known).min(100)
                                        } else { 0 };
                                        rsx! {
                                            div {
                                                p {
                                                    style: "display:flex;justify-content:space-between;margin:0 0 3px 0;",
                                                    span {
                                                        style: "font-family:{theme::FONT_CODE};font-size:0.75rem;color:{theme::ON_SURFACE};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:70%;",
                                                        "{table_name}"
                                                    }
                                                    span {
                                                        style: "font-family:{theme::FONT_CODE};font-size:0.75rem;color:{theme::ON_SURFACE_VARIANT};flex-shrink:0;",
                                                        "{count}"
                                                    }
                                                }
                                                div { style: "{theme::STYLE_PROGRESS_TRACK}",
                                                    div { style: "{theme::STYLE_PROGRESS_BAR}width:{bar_pct}%;", "" }
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

            // ── Bottom bar ───────────────────────────────────────────────
            div {
                style: "padding:16px 24px;background:{theme::BG_WORKSPACE};",
                // Overall progress
                div { style: "{theme::STYLE_PROGRESS_TRACK}margin-bottom:12px;",
                    div { style: "{theme::STYLE_PROGRESS_BAR}width:{pct}%;", "" }
                }

                // Success banner
                if progress.done {
                    div {
                        style: "background:{theme::BG_SIDEBAR};border-radius:4px;padding:16px;margin-bottom:12px;",
                        p {
                            style: "color:{theme::SECONDARY};font-weight:600;margin:0 0 12px 0;",
                            "✓ {progress.rows_processed} rows imported · {progress.total_anomalies} anomalies"
                        }
                        div { style: "display:flex;gap:12px;",
                            button {
                                style: "{theme::STYLE_BTN_PRIMARY}",
                                onclick: move |_| { state.write().cancel(); },
                                "New Import"
                            }
                        }
                    }
                }

                div { style: "display:flex;justify-content:flex-start;",
                    if !progress.done {
                        button {
                            style: "{theme::STYLE_BTN_GHOST}",
                            onclick: move |_| { state.write().cancel(); },
                            "Cancel"
                        }
                    }
                }
            }
        }
    }
}
