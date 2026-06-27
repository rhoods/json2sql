//! Screen 5 — Import (Pass 2)
//!
//! Layout:
//!   subbar  — breadcrumb + pulse + elapsed
//!   split-60-40 (left: real-time log · right: per-table row counts)
//!   bottom  — overall progress bar + caption + action buttons
#![allow(clippy::disallowed_methods, clippy::derive_partial_eq_without_eq)]

use dioxus::prelude::*;

use json2sql::db::ddl;
use json2sql::io::progress_event::ProgressEvent;

use crate::screens::{progress_pct, ProgressBar};
use crate::state::{format_bytes, AppScreen, AppState};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[allow(clippy::derive_partial_eq_without_eq)]
#[component]
pub fn ImportScreen(mut state: Signal<AppState>) -> Element {
    // ── Elapsed timer ─────────────────────────────────────────────────────
    let elapsed_secs = crate::screens::use_elapsed_timer(move || state.read().import.pass2_progress.done);

    // ── Pass 2 runner ────────────────────────────────────────────────────
    let mut once = use_signal(|| false);
    use_coroutine(move |_: UnboundedReceiver<()>| async move {
        if *once.peek() { return; }
        once.set(true);
        if state.read().abort_handle.is_some() {
            return;
        }
        state.write().import.pass2_progress = crate::state::Pass2Progress::default();

        let (source_file, root_table, pg_url, schemas, drop_existing, anomaly_dir, pg_schema, pass2_parallel, import_limit, verbose_logs, hint_format, skip_constraints_flag) = {
            let source_file_opt = state.read().project.source_file.clone();
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
            (
                path,
                root,
                s.project.pg.to_url(),
                crate::screens::build_effective_schemas(&s.schema.schemas, &s.schema.strategy_overrides),
                s.project.drop_existing,
                s.project.anomaly_dir.clone(),
                s.project.pg_schema.clone(),
                s.project.pass2_parallel,
                s.project.import_limit,
                s.project.verbose_logs,
                s.schema.detected_format.clone(),
                s.project.skip_constraints,
            )
        };

        if skip_constraints_flag {
            state.write().import.pass2_progress.constraints_skipped = true;
        }

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        let handle = tokio::spawn(async move {
            let (client, connection) = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                tokio_postgres::connect(&pg_url, tokio_postgres::NoTls),
            )
            .await
            .map_err(|_| std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "PostgreSQL connection timed out (10s)",
            ))??;
            tokio::spawn(async move { let _ = connection.await; });

            ddl::create_tables_no_constraints(&client, &schemas, &pg_schema, drop_existing, Some(&tx)).await?;

            let cfg = json2sql::pass2::Pass2Config {
                root_table,
                pg_schema,
                parallel: pass2_parallel,
                anomaly_dir,
                limit: import_limit,
                mem_flush_threshold_bytes: None,
                ram_high_watermark: None,
                ram_low_watermark: None,
                verbose: verbose_logs,
                hint_format,
                skip_constraints: skip_constraints_flag,
            };
            json2sql::pass2::runner::run(&source_file, &schemas, &client, &pg_url, &cfg, Some(tx))
            .await
        });

        state.write().abort_handle = Some(handle.abort_handle());

        // Throttle UI updates to ~5 Hz — halves DOM patch frequency vs previous 100ms.
        // Critical on large files (240+ tables): reduces IPC pipe pressure during user interaction.
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pending: Vec<ProgressEvent> = Vec::new();
        let mut finished = false;

        loop {
            tokio::select! {
                biased;
                maybe = rx.recv() => match maybe {
                    Some(event) => {
                        if matches!(event, ProgressEvent::Pass2Done { .. }) {
                            finished = true;
                        }
                        pending.push(event);
                    }
                    None => break,
                },
                _ = interval.tick() => {
                    if !pending.is_empty() {
                        let batch = std::mem::take(&mut pending);
                        let mut s = state.write();
                        for e in batch { s.apply_progress_event(e); }
                    }
                    if finished { break; }
                }
            }
        }
        // Flush residual events buffered between the last tick and channel close.
        if !pending.is_empty() {
            let mut s = state.write();
            for e in pending { s.apply_progress_event(e); }
        }

        if let Ok(Err(e)) = handle.await {
            state.write().import.pass2_progress.push_log(format!("Import error: {e}"));
        }

        state.write().abort_handle = None;
    });

    // ── Derived values ────────────────────────────────────────────────────
    let import_limit_display = state.read().project.import_limit;
    let progress      = state.read().import.pass2_progress.clone();
    let done          = progress.done;
    let elapsed       = *elapsed_secs.read();
    let tables_total  = state.read().schema.schemas.len() as u64;

    // Phase 0 — DDL (tables created / total)
    let pct_0: u32 = if progress.ddl_complete {
        100
    } else {
        progress_pct(progress.ddl_done as u64, progress.ddl_table_count.max(1) as u64)
    };
    let label_0 = if progress.ddl_complete {
        format!("{} tables · done", progress.ddl_table_count)
    } else if progress.ddl_table_count == 0 {
        "Waiting…".to_string()
    } else {
        format!("{} / {} tables", progress.ddl_done, progress.ddl_table_count)
    };

    // Phase A — streaming (bytes_read / total_bytes)
    // copy_complete is the authoritative signal: all workers done, regardless of file size.
    let pct_a: u32 = if progress.copy_complete || done {
        100
    } else {
        progress_pct(progress.bytes_read, progress.total_bytes)
    };
    let streaming_done = pct_a == 100;

    // Phase B — COPY (tables flushed / tables total)
    // Use copy_complete to handle empty tables (they never emit Pass2Flush).
    let tables_done = progress.rows_per_table.len() as u64;
    let pct_b: u32 = if progress.copy_complete || done { 100 } else { progress_pct(tables_done, tables_total) };

    // Phase D — constraints (PK + FK applied)
    let pct_d: u32 = if progress.constraints_complete || progress.constraints_skipped {
        100
    } else {
        progress_pct(progress.constraints_done as u64, progress.constraints_total.max(1) as u64)
    };
    let label_d = if progress.constraints_skipped {
        "Skipped".to_string()
    } else if progress.constraints_complete {
        "done".to_string()
    } else if progress.constraints_total == 0 {
        "Waiting…".to_string()
    } else {
        format!("{} / {} ops", progress.constraints_done, progress.constraints_total)
    };

    let elapsed_str = format!("{:02}:{:02}", elapsed / 60, elapsed % 60);
    let status_label = if done {
        "Import complete"
    } else if progress.ddl_table_count > 0 && !progress.ddl_complete {
        "Creating tables…"
    } else if progress.constraints_done > 0 && !progress.constraints_complete {
        "Applying constraints…"
    } else {
        "Importing data…"
    };

    let label_a = if streaming_done {
        format!("{} · done", format_bytes(progress.total_bytes))
    } else {
        format!("{} / {} · {}",
            format_bytes(progress.bytes_read),
            format_bytes(progress.total_bytes),
            elapsed_str)
    };

    let label_b = if done {
        format!("{tables_total} tables · {}", progress.rows_processed)
    } else {
        format!("{tables_done} / {tables_total} tables · {} rows so far", progress.rows_processed)
    };

    let mut table_rows: Vec<(String, u64)> = progress
        .rows_per_table
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    table_rows.sort_by_key(|r| std::cmp::Reverse(r.1));
    // Totaux calculés sur le jeu complet avant le cap — le success banner et label_b
    // doivent refléter le total réel, pas seulement les 100 premières tables.
    let total_rows: u64   = table_rows.iter().map(|(_, n)| n).sum();
    let table_count       = table_rows.len();
    let hidden_tables     = table_count.saturating_sub(100);
    table_rows.truncate(100);
    let anomaly_dir_label = state.read().project.anomaly_dir
        .as_ref()
        .map(|p| p.display().to_string());

    rsx! {
        div { style: "display:flex;flex-direction:column;height:100vh;background:var(--bg);overflow:hidden;",

            // ── Subbar ────────────────────────────────────────────────────
            div { class: "subbar",
                div { class: "crumb",
                    button {
                        class: "step",
                        onclick: move |_| { state.write().cancel(); },
                        "Setup"
                    }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Analysis" }
                    span { class: "sep", "›" }
                    button {
                        class: "step",
                        onclick: move |_| { state.write().screen = AppScreen::Preview; },
                        "Preview"
                    }
                    span { class: "sep", "›" }
                    button { class: "step active", "Import" }
                }
                span { class: "grow" }
                div { class: "row gap-md",
                    if !done {
                        span { class: "pulse-dot" }
                    }
                    span { style: "color:var(--acc);font-weight:600;font-size:var(--fs-xs);",
                        "{status_label}"
                    }
                    if !done {
                        span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;",
                            "{elapsed_str}"
                        }
                    }
                    span { class: "badge muted sq", "PASS 2 / 2" }
                    if let Some(n) = import_limit_display {
                        span { class: "badge warn", "⚠ SAMPLE MODE — first {n} rows only" }
                    }
                }
            }

            // ── Success banner ────────────────────────────────────────────
            if done {
                div { class: "alert success", style: "border-radius:0;border-left:none;border-right:none;border-top:none;",
                    span { style: "font-weight:600;", "✓ Import complete" }
                    span { " — " }
                    span { "{total_rows} rows across {table_count} tables" }
                    if progress.total_anomalies > 0 {
                        span { style: "margin-left:8px;", "· " }
                        span { class: "badge warn", "{progress.total_anomalies} anomalies" }
                        if let Some(ref dir) = anomaly_dir_label {
                            span { style: "margin-left:6px;font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);color:inherit;",
                                "→ {dir}"
                            }
                        }
                    }
                    if progress.constraint_warning_count > 0 {
                        span { style: "margin-left:8px;", "· " }
                        span { class: "badge warn", "⚠ {progress.constraint_warning_count} FK warnings" }
                    }
                }
            }

            // ── Split 60 / 40 ─────────────────────────────────────────────
            div { class: "split-60-40 grow",

                // ── Left pane — import log ────────────────────────────────
                div { class: "pane",

                    div { class: "pane-head",
                        span { class: "ttl",
                            span { style: "font-size:var(--fs-xs);font-weight:600;color:var(--fg-3);text-transform:uppercase;letter-spacing:.06em;",
                                "Import log"
                            }
                            span { class: "meta", "{progress.log_lines.len()} lines" }
                        }
                    }

                    div { class: "pane-body no-pad",
                        div {
                            class: "log",
                            style: "border:none;border-radius:0;padding:10px 14px;height:100%;overflow-y:auto;line-height:1.5;",
                            for line in progress.log_lines.iter().skip(progress.log_lines.len().saturating_sub(50)) {
                                {
                                    let is_err  = line.to_lowercase().contains("error");
                                    let is_warn = !is_err && (line.contains("Anomaly") || line.contains("WARNING") || line.contains("Warning") || line.contains('⚠'));
                                    let is_ok   = !is_err && !is_warn && (line.contains("complete") || line.contains("Done") || line.contains("flushed"));
                                    let cls = if is_err { "err" } else if is_warn { "warn" } else if is_ok { "ok" } else { "" };
                                    let l = line.clone();
                                    rsx! { div { key: "{l}", class: "{cls}", "{l}" } }
                                }
                            }
                            if !done {
                                div { style: "color:var(--success);", "▌" }
                            }
                        }
                    }
                }

                // ── Right pane — per-table rows ───────────────────────────
                div { class: "pane",

                    div { class: "pane-head",
                        span { class: "ttl",
                            span { style: "font-size:var(--fs-xs);font-weight:600;color:var(--fg-3);text-transform:uppercase;letter-spacing:.06em;",
                                "Per-table rows"
                            }
                            if total_rows > 0 {
                                span { class: "meta", "{total_rows} total" }
                            }
                        }
                    }

                    div { class: "pane-body no-pad",
                        if table_rows.is_empty() {
                            div { style: "padding:16px;",
                                span { style: "font-size:var(--fs-xs);color:var(--fg-4);", "Waiting for first flush…" }
                            }
                        } else {
                            table { class: "t", style: "width:100%;",
                                tbody {
                                    for (tname, count) in table_rows.iter() {
                                        tr {
                                            key: "{tname}",
                                            td { style: "font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:0;width:100%;",
                                                "{tname}"
                                            }
                                            td { style: "font-family:'JetBrains Mono',monospace;font-size:var(--fs-xs);color:var(--acc);font-weight:600;white-space:nowrap;text-align:right;",
                                                "{count}"
                                            }
                                        }
                                    }
                                    if hidden_tables > 0 {
                                        tr {
                                            td { colspan: "2", style: "font-size:var(--fs-xs);color:var(--fg-4);padding:4px 0;",
                                                "+ {hidden_tables} autres tables…"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── Bottom bar — double progress + actions ────────────────────
            div { style: "padding:10px 20px 14px;background:var(--bg-1);border-top:1px solid var(--bd);flex-shrink:0;",

                div { style: "display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:10px;",
                    ProgressBar {
                        pct: pct_0,
                        done: progress.ddl_complete,
                        label: label_0,
                        phase: "0 · Tables".to_string(),
                    }
                    ProgressBar {
                        pct: pct_a,
                        done: streaming_done,
                        label: label_a,
                        phase: "A · Streaming".to_string(),
                    }
                    ProgressBar {
                        pct: pct_b,
                        done,
                        label: label_b,
                        phase: "B · Inserting".to_string(),
                    }
                    ProgressBar {
                        pct: pct_d,
                        done: progress.constraints_complete,
                        label: label_d,
                        phase: "D · Constraints".to_string(),
                    }
                }

                // Actions row
                div { class: "row", style: "justify-content:flex-end;",
                    div { class: "row gap-md",
                        if done {
                            button {
                                class: "btn primary",
                                onclick: move |_| { state.write().cancel(); },
                                "New Import"
                            }
                        } else {
                            button {
                                class: "btn ghost",
                                onclick: move |_| { state.write().cancel(); },
                                "Cancel"
                            }
                        }
                    }
                }
            }
        }
    }
}
