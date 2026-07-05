//! Screen — Resume import in progress
//!
//! Displayed at startup when an active worker socket is detected.
//! The user can either resume (reconnect to the running worker) or
//! abandon the import (send cancel + navigate to Setup).
//!
//! Fonctions :
//! - `ResumeScreen` — composant unique : propose reconnexion ou abandon du worker détecté.
#![allow(clippy::disallowed_methods, clippy::derive_partial_eq_without_eq)]

use dioxus::prelude::*;

use crate::state::{AppScreen, AppState};

#[component]
pub fn ResumeScreen(mut state: Signal<AppState>) -> Element {
    // Path of the active worker socket detected at startup.
    let socket_path = state.read().resume_socket.clone();
    let socket_label = socket_path
        .as_ref()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_string());

    rsx! {
        div {
            style: "display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;background:var(--bg);gap:24px;",

            div {
                style: "text-align:center;",
                div {
                    style: "font-size:20px;font-weight:600;color:var(--fg);margin-bottom:8px;",
                    "Import en cours — reprendre ?"
                }
                div {
                    style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;",
                    "{socket_label}"
                }
            }

            div {
                style: "display:flex;gap:12px;",

                // Reprendre — navigate to Import screen (coroutine will reconnect)
                button {
                    class: "btn primary",
                    onclick: move |_| {
                        let mut s = state.write();
                        s.screen = AppScreen::Import;
                    },
                    "Reprendre"
                }

                // Abandonner — send cancel command, clean up, go to Setup
                button {
                    class: "btn ghost",
                    onclick: move |_| {
                        let path = state.read().resume_socket.clone();
                        if let Some(socket_path) = path {
                            // Spawn a fire-and-forget task to send cancel and clean up.
                            tokio::spawn(async move {
                                if let Ok(mut stream) =
                                    tokio::net::UnixStream::connect(&socket_path).await
                                {
                                    let _ = tokio::io::AsyncWriteExt::write_all(
                                        &mut stream,
                                        b"{\"cmd\":\"cancel\"}\n",
                                    )
                                    .await;
                                }
                                // Remove the socket file (best effort).
                                let _ = std::fs::remove_file(&socket_path);
                                // Also remove the result file if present.
                                let _ = std::fs::remove_file(socket_path.with_extension("json"));
                            });
                        }
                        state.write().cancel();
                    },
                    "Abandonner l'import"
                }
            }
        }
    }
}
