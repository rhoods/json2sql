/// Screen 1 — Project Setup
///
/// File picker + PostgreSQL connection form.
/// Transitions to Screen 2 (Analysis) when both source and target are configured.
use dioxus::prelude::*;

use crate::state::{AppScreen, AppState};
use crate::theme;

#[component]
pub fn SetupScreen(mut state: Signal<AppState>) -> Element {
    let ready = state.read().ready_to_start();
    let source_label = state
        .read()
        .source_file
        .as_ref()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "No file selected".to_string());

    let pg_ok = state.read().pg_ok;
    let pg_testing = state.read().pg_testing;
    let drop_existing = state.read().drop_existing;

    let btn_style = if ready {
        theme::STYLE_BTN_PRIMARY.to_string()
    } else {
        format!("{}opacity:0.4;width:100%;", theme::STYLE_BTN_PRIMARY)
    };

    rsx! {
        div {
            style: "display:flex;align-items:center;justify-content:center;height:100vh;background:{theme::BG_ROOT};",
            div {
                style: "background:{theme::BG_SIDEBAR};border-radius:6px;padding:40px;width:520px;",

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
                        }
                        button {
                            style: "{theme::STYLE_BTN_GHOST}white-space:nowrap;",
                            onclick: move |_| async move {
                                if let Some(path) = rfd::AsyncFileDialog::new()
                                    .add_filter("JSON / JSONL", &["json", "jsonl", "ndjson"])
                                    .pick_file()
                                    .await
                                {
                                    state.write().source_file = Some(path.path().to_path_buf());
                                }
                            },
                            "Browse…"
                        }
                    }
                }

                // ── Target — PostgreSQL ─────────────────────────────��─────
                section {
                    style: "margin-bottom:32px;",
                    label {
                        style: "display:block;color:{theme::ON_SURFACE_VARIANT};font-size:0.6875rem;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:12px;",
                        "Target — PostgreSQL"
                    }

                    // Host + Port on one row
                    div {
                        style: "display:flex;gap:8px;margin-bottom:8px;",
                        input {
                            style: "{theme::STYLE_INPUT}",
                            r#type: "text",
                            placeholder: "Host",
                            value: "{state.read().pg.host}",
                            oninput: move |e| { state.write().pg.host = e.value(); },
                        }
                        input {
                            style: "background:{theme::BG_INPUT};color:{theme::ON_SURFACE};border:none;border-bottom:1px solid #40475266;border-radius:2px 2px 0 0;padding:6px 10px;width:80px;box-sizing:border-box;font-family:Inter,system-ui,sans-serif;",
                            r#type: "number",
                            placeholder: "Port",
                            value: "{state.read().pg.port}",
                            oninput: move |e| {
                                if let Ok(p) = e.value().parse::<u16>() {
                                    state.write().pg.port = p;
                                }
                            },
                        }
                    }

                    // Database
                    input {
                        style: "{theme::STYLE_INPUT}margin-bottom:8px;",
                        r#type: "text",
                        placeholder: "Database",
                        value: "{state.read().pg.database}",
                        oninput: move |e| { state.write().pg.database = e.value(); },
                    }

                    // Username
                    input {
                        style: "{theme::STYLE_INPUT}margin-bottom:8px;",
                        r#type: "text",
                        placeholder: "Username",
                        value: "{state.read().pg.username}",
                        oninput: move |e| { state.write().pg.username = e.value(); },
                    }

                    // Password
                    input {
                        style: "{theme::STYLE_INPUT}margin-bottom:12px;",
                        r#type: "password",
                        placeholder: "Password",
                        value: "{state.read().pg.password}",
                        oninput: move |e| { state.write().pg.password = e.value(); },
                    }

                    // Test connection
                    div {
                        style: "display:flex;align-items:center;gap:12px;",
                        button {
                            style: "{theme::STYLE_BTN_GHOST}",
                            disabled: pg_testing,
                            onclick: move |_| async move {
                                let url = state.read().pg.to_url();
                                state.write().pg_testing = true;
                                state.write().pg_ok = None;
                                let ok = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                                    .await
                                    .is_ok();
                                state.write().pg_testing = false;
                                state.write().pg_ok = Some(ok);
                            },
                            if pg_testing { "Testing…" } else { "Test connection" }
                        }
                        match pg_ok {
                            Some(true)  => rsx! { span { style: "color:{theme::SECONDARY};font-size:0.8125rem;", "Connected" } },
                            Some(false) => rsx! { span { style: "color:{theme::ERROR};font-size:0.8125rem;",    "Connection failed" } },
                            None        => rsx! { span {} },
                        }
                    }
                }

                // ── Options ──────────────────────────────────────────────
                div {
                    style: "display:flex;align-items:center;gap:8px;margin-bottom:20px;",
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

                // ── CTA ───────────────────────────────────────────────────
                button {
                    style: "{btn_style}width:100%;",
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
