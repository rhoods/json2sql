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
    let pct = if progress.done {
        100
    } else if progress.total_bytes > 0 {
        (progress.bytes_read as f64 / progress.total_bytes as f64 * 100.0) as u32
    } else {
        0
    };
    let progress_caption = if progress.done {
        format!(
            "Rows imported: {} · File read: {} MB / {} MB",
            progress.rows_processed,
            progress.bytes_read / 1_000_000,
            progress.total_bytes / 1_000_000,
        )
    } else {
        format!(
            "Objects processed: {} · File read: {} MB / {} MB",
            progress.rows_processed,
            progress.bytes_read / 1_000_000,
            progress.total_bytes / 1_000_000,
        )
    };

    // Launch the full import pipeline once on mount.
    use_coroutine(move |_: UnboundedReceiver<()>| async move {
        // Guard: abort_handle is Some while a runner is in flight.
        // Bail out on remount to prevent two Pass 2 pipelines writing concurrently.
        if state.read().abort_handle.is_some() {
            return;
        }

        // Reset progress only after confirming no runner is active.
        state.write().pass2_progress = crate::state::Pass2Progress::default();

        let (source_file, root_table, pg_url, schemas, drop_existing, anomaly_dir, pg_schema) = {
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
            (path, root, s.pg.to_url(), s.schemas.clone(), s.drop_existing, s.anomaly_dir.clone(), s.pg_schema.clone())
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        let handle = tokio::spawn(async move {
            // 1. Connect (10s timeout — import may be to a remote server)
            let (client, connection) = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                tokio_postgres::connect(&pg_url, tokio_postgres::NoTls),
            )
            .await
            .map_err(|_| std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "PostgreSQL connection timed out (10s)",
            ))??;
            tokio::spawn(async move {
                let _ = connection.await;
            });

            // 2. Create tables (behaviour driven by user's drop_existing choice)
            ddl::create_tables(&client, &schemas, &pg_schema, drop_existing).await?;

            // 3. Run Pass 2
            json2sql::pass2::runner::run(
                &source_file,
                &root_table,
                &schemas,
                &client,
                &pg_schema,
                100_000,    // flush_threshold
                false,      // use_transaction
                None,       // db_url (parallel > 1 only)
                1,          // parallel
                anomaly_dir,
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

    let total_rows_flushed: u64 = table_rows.iter().map(|(_, n)| n).sum();
    let status_text = if progress.done { "Import complete" } else { "Importing data…" };
    let anomaly_dir_label = state.read().anomaly_dir
        .as_ref()
        .map(|p| p.display().to_string());

    rsx! {
        div {
            style: "display:flex;flex-direction:column;height:100vh;background:{theme::BG_ROOT};",

            // ── Header ───────────────────────────────────────────────────
            div {
                style: "padding:16px 24px;background:{theme::BG_WORKSPACE};display:flex;align-items:center;gap:16px;",
                span {
                    style: "color:{theme::ON_SURFACE};font-size:1rem;font-weight:600;",
                    "{status_text}"
                }
                if let Some(ref dir) = anomaly_dir_label {
                    span {
                        style: "font-family:{theme::FONT_CODE};font-size:0.75rem;color:{theme::ON_SURFACE_DIM};",
                        "anomalies → {dir}"
                    }
                }
            }

            // ── Main split ───────────────────────────────────────────────
            div {
                    style: "display:flex;flex:1;overflow:hidden;min-height:0;min-width:0;",
                // Left — import log (60%)
                div {
                    class: "log-panel",
                    style: "flex:0 1 60%;min-width:0;box-sizing:border-box;",
                    for line in progress.log_lines.iter() {
                        p { style: "margin:2px 0;", "{line}" }
                    }
                }

                // Right — per-table row counts (40%)
                div {
                    style: "flex:0 1 40%;min-width:0;min-height:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};padding:16px;overflow-y:auto;",
                    p {
                        style: "color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;margin:0 0 12px 0;",
                        "Rows imported per table"
                    }
                    if table_rows.is_empty() {
                        p { style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;", "Waiting for first flush…" }
                    } else {
                        div {
                            style: "display:flex;flex-direction:column;gap:8px;min-height:0;min-width:0;",
                            for (table_name, count) in table_rows.iter() {
                                div {
                                    key: "{table_name}",
                                    style: "display:flex;justify-content:space-between;align-items:center;padding:8px 12px;background:{theme::BG_WORKSPACE};border-radius:4px;",
                                    span {
                                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::ON_SURFACE};",
                                        "{table_name}"
                                    }
                                    span {
                                        style: "font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::SECONDARY};font-weight:600;",
                                        "{count}"
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
                div { class: "progress-track", style: "margin-bottom:8px;",
                    div { class: "progress-bar", style: "width:{pct}%;", "" }
                }
                p {
                    style: "color:{theme::ON_SURFACE_VARIANT};font-size:0.8125rem;margin:4px 0 12px 0;",
                    "{progress_caption}"
                }

                // Success banner
                div { style: "display:flex;justify-content:flex-start;",
                    if progress.done {
                        div {
                            style: "width:100%;",
                            p {
                                style: "color:{theme::SECONDARY};font-weight:600;margin:0 0 8px 0;",
                                "✓ {total_rows_flushed} rows imported into {table_rows.len()} tables · {progress.total_anomalies} anomalies"
                            }
                            button {
                                class: "btn-primary",
                                onclick: move |_| { state.write().cancel(); },
                                "New Import"
                            }
                        }
                    } else {
                        button {
                            class: "btn-ghost",
                            onclick: move |_| { state.write().cancel(); },
                            "Cancel"
                        }
                    }
                }
            }
        }
    }
}
