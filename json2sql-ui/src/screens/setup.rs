/// Screen 1 — Project Setup
///
/// File picker + PostgreSQL connection form.
/// Transitions to Screen 2 (Analysis) when both source and target are configured.
use dioxus::prelude::*;

use crate::state::{format_bytes, AppScreen, AppState};
use crate::theme;

/// Result of a zenity picker invocation.
enum PickResult {
    /// User selected a path.
    Selected(std::path::PathBuf),
    /// User closed or cancelled the dialog.
    Cancelled,
    /// `zenity` binary is not installed on this system.
    NotAvailable,
}

/// Run zenity with the given args. Returns `NotAvailable` when the binary is
/// missing (`io::ErrorKind::NotFound`), `Cancelled` on any other failure or
/// empty selection, and `Selected` on success.
async fn run_zenity(args: Vec<String>) -> PickResult {
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("zenity").args(&args).output()
    })
    .await;

    let output = match output {
        Ok(Ok(o)) => o,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => return PickResult::NotAvailable,
        _ => return PickResult::Cancelled,
    };

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            PickResult::Cancelled
        } else {
            PickResult::Selected(std::path::PathBuf::from(path))
        }
    } else {
        PickResult::Cancelled
    }
}

async fn pick_file_zenity(filters: &[(&str, &str)]) -> PickResult {
    let mut args = vec!["--file-selection".to_string(), "--title=Select file".to_string()];
    for (_, glob) in filters {
        args.push(format!("--file-filter={}", glob));
    }
    run_zenity(args).await
}

async fn pick_folder_zenity() -> PickResult {
    run_zenity(vec![
        "--file-selection".to_string(),
        "--directory".to_string(),
        "--title=Select folder".to_string(),
    ])
    .await
}

#[component]
pub fn SetupScreen(mut state: Signal<AppState>) -> Element {
    let ready = state.read().ready_to_start();
    let source_path = state.read().source_file.clone();
    let source_label = source_path
        .as_ref()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "No file selected".to_string());
    // File size for display — computed once per render from already-known path.
    let source_size_bytes: Option<u64> = source_path.as_ref()
        .and_then(|p| std::fs::metadata(p).ok())
        .map(|m| m.len());
    let source_size_label: Option<String> = source_size_bytes.map(format_bytes);
    // Warn if > 5 GB — analysis may be slow.
    let source_large = source_size_bytes.map(|b| b > 5 * 1_073_741_824).unwrap_or(false);

    let pg_ok = state.read().pg_ok;
    let pg_testing = state.read().pg_testing;
    let pg_error = state.read().pg_error.clone();
    let drop_existing = state.read().drop_existing;
    let pg_schema = state.read().pg_schema.clone();
    let anomaly_label = state
        .read()
        .anomaly_dir
        .as_ref()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "None".to_string());
    let pg_host = state.read().pg.host.clone();
    let pg_port = if state.read().pg.port == 0 {
        String::new()
    } else {
        state.read().pg.port.to_string()
    };
    let pg_database = state.read().pg.database.clone();
    let pg_username = state.read().pg.username.clone();
    let pg_password = state.read().pg.password.clone();
    let test_btn_label = if pg_testing { "Testing…" } else { "Test connection" };

    // Guards against concurrent picks — zenity is blocking (spawn_blocking),
    // a second click while the dialog is open is silently ignored.
    let mut picking_source  = use_signal(|| false);
    let mut picking_anomaly = use_signal(|| false);
    // Set to Some(msg) when zenity is not installed; displayed near the Browse button.
    let mut picker_error: Signal<Option<String>> = use_signal(|| None);

    rsx! {
        div {
            style: "display:flex;align-items:center;justify-content:center;height:100vh;background:{theme::BG_ROOT};min-width:0;",
            div {
                style: "background:{theme::BG_SIDEBAR};border-radius:6px;padding:40px;width:100%;max-width:520px;box-sizing:border-box;",

                // App title
                h1 {
                    style: "color:{theme::ON_SURFACE};font-family:{theme::FONT_CODE};font-size:1.5rem;letter-spacing:-0.02em;margin:0 0 32px 0;",
                    "json2sql"
                }

                // ── Source ────────────────────────────────────────────────
                section {
                    style: "margin-bottom:24px;",
                    label {
                        style: "display:block;color:{theme::ON_SURFACE_VARIANT};font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;",
                        "Source"
                    }
                    div {
                        style: "display:flex;align-items:center;gap:10px;",
                        span {
                            style: "flex:1;font-family:{theme::FONT_CODE};font-size:0.8125rem;color:{theme::ON_SURFACE_VARIANT};background:{theme::BG_INPUT};padding:7px 10px;border-radius:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                            "{source_label}"
                            if let Some(ref size) = source_size_label {
                                span {
                                    style: "margin-left:8px;color:{theme::ON_SURFACE_DIM};font-size:0.75rem;",
                                    "({size})"
                                }
                            }
                        }
                        if source_large {
                            span {
                                style: "color:{theme::TERTIARY};font-size:0.75rem;",
                                "Large file — analysis may take several minutes"
                            }
                        }
                        button {
                            class: "btn-ghost",
                            style: "white-space:nowrap;",
                            disabled: picking_source(),
                            onclick: move |_| async move {
                                if picking_source() { return; }
                                picking_source.set(true);
                                picker_error.set(None);
                                let result = pick_file_zenity(&[
                                    ("JSON / JSONL", "*.json *.jsonl *.ndjson"),
                                ]).await;
                                picking_source.set(false);
                                match result {
                                    PickResult::Selected(path) => { state.write().source_file = Some(path); }
                                    PickResult::Cancelled => {}
                                    PickResult::NotAvailable => {
                                        picker_error.set(Some("zenity not found — install it: sudo apt install zenity".to_string()));
                                    }
                                }
                            },
                            "Browse…"
                        }
                        if let Some(ref err) = picker_error() {
                            span {
                                style: "color:{theme::ERROR};font-size:0.75rem;",
                                "{err}"
                            }
                        }
                    }
                }

                // ── Target — PostgreSQL ───────────────────────────────────
                section {
                    style: "margin-bottom:32px;",
                    label {
                        style: "display:block;color:{theme::ON_SURFACE_VARIANT};font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:12px;",
                        "Target — PostgreSQL"
                    }

                    // Host + Port on one row
                    div {
                        style: "display:flex;gap:8px;margin-bottom:8px;",
                        div {
                            style: "flex:1;",
                            label {
                                style: "display:block;color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin-bottom:3px;",
                                "Host"
                            }
                            input {
                                class: "input-field",
                                r#type: "text",
                                value: "{pg_host}",
                                placeholder: "localhost",
                                oninput: move |e| { state.write().pg.host = e.value(); },
                            }
                        }
                        div {
                            label {
                                style: "display:block;color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin-bottom:3px;",
                                "Port"
                            }
                            input {
                                class: "input-field",
                                style: "width:80px;",
                                r#type: "number",
                                value: "{pg_port}",
                                placeholder: "5432",
                                oninput: move |e| {
                                    let value = e.value();
                                    if value.is_empty() {
                                        state.write().pg.port = 0;
                                    } else if let Ok(p) = value.parse::<u16>() {
                                        state.write().pg.port = p;
                                    }
                                },
                            }
                            if state.read().pg.port == 0 {
                                span {
                                    style: "font-size:0.6875rem;color:{theme::ERROR};",
                                    "Required"
                                }
                            }
                        }
                    }

                    // Database
                    div {
                        style: "margin-bottom:8px;",
                        label {
                            style: "display:block;color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin-bottom:3px;",
                            "Database"
                        }
                        input {
                            class: "input-field",
                            r#type: "text",
                            value: "{pg_database}",
                            placeholder: "my_database",
                            oninput: move |e| { state.write().pg.database = e.value(); },
                        }
                    }

                    // Username
                    div {
                        style: "margin-bottom:8px;",
                        label {
                            style: "display:block;color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin-bottom:3px;",
                            "Username"
                        }
                        input {
                            class: "input-field",
                            r#type: "text",
                            value: "{pg_username}",
                            placeholder: "postgres",
                            oninput: move |e| { state.write().pg.username = e.value(); },
                        }
                    }

                    // Password
                    div {
                        style: "margin-bottom:12px;",
                        label {
                            style: "display:block;color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin-bottom:3px;",
                            "Password"
                        }
                        input {
                            class: "input-field",
                            r#type: "password",
                            value: "{pg_password}",
                            placeholder: "••••••••",
                            oninput: move |e| { state.write().pg.password = e.value(); },
                        }
                    }

                    // Schema
                    {
                        let schema_invalid = !pg_schema.chars().all(|c| c.is_alphanumeric() || c == '_');
                        rsx! {
                            div {
                                style: "margin-bottom:12px;",
                                label {
                                    style: "display:block;color:{theme::ON_SURFACE_DIM};font-size:0.6875rem;margin-bottom:3px;",
                                    "Schema"
                                }
                                input {
                                    class: "input-field",
                                    r#type: "text",
                                    value: "{pg_schema}",
                                    placeholder: "public",
                                    oninput: move |e| {
                                        let v = e.value().trim().to_string();
                                        state.write().pg_schema = if v.is_empty() { "public".to_string() } else { v };
                                    },
                                }
                                if schema_invalid {
                                    span {
                                        style: "font-size:0.6875rem;color:{theme::ERROR};",
                                        "Only letters, digits and underscores allowed"
                                    }
                                }
                            }
                        }
                    }

                    // Test connection
                    div {
                        style: "display:flex;align-items:center;gap:12px;",
                        button {
                            class: "btn-ghost",
                            disabled: pg_testing,
                            onclick: move |_| async move {
                                let url = state.read().pg.to_url();
                                state.write().pg_testing = true;
                                state.write().pg_ok = None;
                                state.write().pg_error = None;
                                let result = tokio::time::timeout(
                                    std::time::Duration::from_secs(5),
                                    tokio_postgres::connect(&url, tokio_postgres::NoTls),
                                ).await;
                                let (ok, err_msg) = match result {
                                    Ok(Ok(_))    => (true, None),
                                    Ok(Err(e))   => (false, Some(e.to_string())),
                                    Err(_)       => (false, Some("Connection timed out (5s)".to_string())),
                                };
                                state.write().pg_testing = false;
                                state.write().pg_ok = Some(ok);
                                state.write().pg_error = err_msg;
                            },
                            "{test_btn_label}"
                        }
                        if let Some(true) = pg_ok {
                            span { style: "color:{theme::SECONDARY};font-size:0.8125rem;", "Connected" }
                        } else if let Some(false) = pg_ok {
                            span { style: "color:{theme::ERROR};font-size:0.8125rem;", "Connection failed" }
                        }
                        if let Some(error) = pg_error.as_ref() {
                            span {
                                style: "color:{theme::ERROR};font-size:0.75rem;display:block;margin-top:4px;",
                                "{error}"
                            }
                        }
                    }
                }

                // ── Options ──────────────────────────────────────────────
                div {
                    style: "margin-bottom:20px;display:flex;flex-direction:column;gap:10px;",

                    div {
                        style: "display:flex;align-items:center;gap:8px;",
                        input {
                            r#type: "checkbox",
                            id: "drop_existing",
                            checked: drop_existing,
                            onchange: move |e| { state.write().drop_existing = e.checked(); },
                        }
                        label {
                            r#for: "drop_existing",
                            style: "color:{theme::TERTIARY};font-size:0.8125rem;cursor:pointer;",
                            "Drop existing tables before import (CASCADE)"
                        }
                    }

                    div {
                        style: "display:flex;align-items:center;gap:8px;",
                        label {
                            style: "color:{theme::ON_SURFACE_DIM};font-size:0.8125rem;white-space:nowrap;",
                            "Anomaly output:"
                        }
                        span {
                            style: "flex:1;font-family:{theme::FONT_CODE};font-size:0.75rem;color:{theme::ON_SURFACE_VARIANT};background:{theme::BG_INPUT};padding:5px 8px;border-radius:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                            "{anomaly_label}"
                        }
                        button {
                            class: "btn-ghost btn-ghost--sm",
                            style: "white-space:nowrap;",
                            disabled: picking_anomaly(),
                            onclick: move |_| async move {
                                if picking_anomaly() { return; }
                                picking_anomaly.set(true);
                                let result = pick_folder_zenity().await;
                                picking_anomaly.set(false);
                                match result {
                                    PickResult::Selected(dir) => { state.write().anomaly_dir = Some(dir); }
                                    PickResult::Cancelled => {}
                                    PickResult::NotAvailable => {
                                        picker_error.set(Some("zenity not found — install it: sudo apt install zenity".to_string()));
                                    }
                                }
                            },
                            "Browse…"
                        }
                        if state.read().anomaly_dir.is_some() {
                            button {
                                class: "btn-ghost btn-ghost--sm",
                                onclick: move |_| { state.write().anomaly_dir = None; },
                                "✕"
                            }
                        }
                    }
                }

                // ── CTA ───────────────────────────────────────────────────
                button {
                    class: "btn-primary",
                    style: "width:100%;",
                    disabled: !ready,
                    onclick: move |_| {
                        state.write().screen = AppScreen::Analysis;
                    },
                    "Start Analysis →"
                }
            }
        }
    }
}
