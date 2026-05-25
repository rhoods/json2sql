/// Screen 4 — SQL Schema Preview (read-only)
///
/// Three-pane split layout:
///   left  300px (collapsible) — table list with strategy badges
///   center fluid              — generated DDL with syntax highlighting
///   right 300px (collapsible) — selected table summary + strategy diff

use dioxus::prelude::*;

use json2sql::db::ddl::generate_ddl_preview;

use crate::screens::{build_effective_schemas, build_table_rows, strategy_badge};
use crate::screens::table_list::TableListPanel;
use crate::state::{AppScreen, AppState};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[component]
pub fn PreviewScreen(mut state: Signal<AppState>) -> Element {
    let (schemas_orig, strategy_overrides) = {
        let s = state.read();
        (s.schema.schemas.clone(), s.schema.strategy_overrides.clone())
    };

    let schemas = build_effective_schemas(&schemas_orig, &strategy_overrides);

    if schemas.is_empty() {
        return rsx! {
            div { style: "display:flex;align-items:center;justify-content:center;height:100vh;background:var(--bg);",
                p { style: "color:var(--fg-3);", "No schema loaded." }
            }
        };
    }

    let mut left_collapsed:  Signal<bool> = use_signal(|| false);
    let mut right_collapsed: Signal<bool> = use_signal(|| false);

    let pg_schema = state.read().project.pg_schema.clone();
    let idx = state.read().schema.last_selected_idx.min(schemas.len().saturating_sub(1));

    let selected = &schemas[idx];
    let ddl = generate_ddl_preview(selected, &pg_schema);

    let (data_cols, gen_cols) = selected.columns.iter()
        .fold((0usize, 0usize), |(d, g), col| {
            if col.is_generated { (d, g + 1) } else { (d + 1, g) }
        });

    // Strategy diff — tables whose override changed the strategy type.
    let diffs: Vec<DiffEntry> = schemas_orig.iter()
        .filter_map(|orig| {
            let ov = strategy_overrides.get(&orig.name)?;
            if std::mem::discriminant(ov) == std::mem::discriminant(&orig.wide_strategy) {
                return None;
            }
            let (_from_cls, from_lbl) = strategy_badge(&orig.wide_strategy);
            let (to_cls,    to_lbl)   = strategy_badge(ov);
            Some(DiffEntry {
                table_name: orig.name.clone(),
                from_lbl,
                to_cls,
                to_lbl,
            })
        })
        .collect();

    let (sel_cls, sel_lbl) = strategy_badge(&selected.wide_strategy);
    let selected_name = selected.name.clone();
    let parent_table  = selected.parent_table.clone();

    rsx! {
        div { style: "display:flex;flex-direction:column;height:100vh;background:var(--bg);overflow:hidden;",

            // ── Subbar ────────────────────────────────────────────────────
            div { class: "subbar",
                div { class: "crumb",
                    button {
                        class: "step",
                        onclick: move |_| { state.write().screen = AppScreen::Setup; },
                        "Setup"
                    }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Analysis" }
                    span { class: "sep", "›" }
                    button {
                        class: "step",
                        onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                        "Strategy"
                    }
                    span { class: "sep", "›" }
                    button { class: "step active", "SQL Preview" }
                }
                span { class: "grow" }
                div { class: "row gap-md",
                    span { class: "badge muted sq", "{schemas.len()} tables" }
                    if !diffs.is_empty() {
                        span { class: "badge acc sq", "{diffs.len()} overrides" }
                    }
                }
            }

            // ── Split-3 workspace ─────────────────────────────────────────
            div { class: "split-3", style: "flex:1;min-height:0;",

                // ── Left pane — table list ────────────────────────────────
                div {
                    class: if *left_collapsed.read() { "pane collapsed" } else { "pane" },
                    style: "width:280px;flex-shrink:0;",

                    div { class: "pane-head",
                        span { class: "ttl",
                            span { style: "font-size:var(--fs-xs);font-weight:600;color:var(--fg-3);text-transform:uppercase;letter-spacing:.06em;",
                                "Tables"
                            }
                            span { class: "meta", "({schemas.len()})" }
                        }
                        span { class: "grow" }
                        button {
                            class: "collapse-btn",
                            onclick: move |_| { let v = !*left_collapsed.read(); left_collapsed.set(v); },
                            if *left_collapsed.read() { "›" } else { "‹" }
                        }
                    }

                    div { class: "pane-body no-pad",
                        TableListPanel {
                            rows: build_table_rows(
                                &schemas, &strategy_overrides,
                                &std::collections::HashSet::new(),
                                &std::collections::HashSet::from([idx]),
                                "", false,
                            ),
                            show_checkboxes: false,
                            on_select: move |i| {
                                let mut s = state.write();
                                s.schema.selected_table_indices = std::collections::HashSet::from([i]);
                                s.schema.last_selected_idx = i;
                            },
                        }
                    }

                    div { class: "collapsed-strip",
                        onclick: move |_| { left_collapsed.set(false); },
                        div { class: "ic-wrap", "T" }
                        span { class: "lbl", "Tables" }
                    }
                }

                div { class: "splitter" }

                // ── Center pane — DDL ─────────────────────────────────────
                div { class: "pane fluid wide-mid", style: "overflow:hidden;display:flex;flex-direction:column;",

                    div { class: "pane-head",
                        span { class: "ttl",
                            span { style: "font-size:var(--fs-xs);font-weight:600;color:var(--fg-3);text-transform:uppercase;letter-spacing:.06em;",
                                "DDL"
                            }
                            span { class: "meta", "— {selected_name}" }
                        }
                    }

                    div { class: "pane-body", style: "flex:1;overflow-y:auto;",
                        pre { class: "code",
                            for line in ddl.lines() {
                                DdlLine { line: line.to_string() }
                            }
                        }
                    }
                }

                div { class: "splitter" }

                // ── Right pane — summary + diff ───────────────────────────
                div {
                    class: if *right_collapsed.read() { "pane collapsed" } else { "pane" },
                    style: "width:300px;flex-shrink:0;",

                    div { class: "pane-head",
                        span { class: "ttl",
                            span { style: "font-size:var(--fs-xs);font-weight:600;color:var(--fg-3);text-transform:uppercase;letter-spacing:.06em;",
                                "Summary"
                            }
                        }
                        span { class: "grow" }
                        button {
                            class: "collapse-btn",
                            onclick: move |_| { let v = !*right_collapsed.read(); right_collapsed.set(v); },
                            if *right_collapsed.read() { "‹" } else { "›" }
                        }
                    }

                    div { class: "pane-body", style: "overflow-y:auto;padding:0;",

                        // Selected table card
                        div { style: "padding:14px 16px;border-bottom:1px solid var(--bd);",
                            div { style: "display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px;",
                                span { style: "font-size:var(--fs-sm);font-weight:600;font-family:var(--font-code);color:var(--fg);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                                    "{selected_name}"
                                }
                                span { class: "badge {sel_cls}", "{sel_lbl}" }
                            }
                            div { style: "display:flex;gap:6px;flex-wrap:wrap;",
                                span { class: "badge muted", "{data_cols} cols" }
                                if gen_cols > 0 {
                                    span { class: "badge muted", "{gen_cols} gen" }
                                }
                                if let Some(ref p) = parent_table {
                                    span { class: "badge info", "FK → {p}" }
                                }
                            }
                        }

                        // Strategy diff section
                        div { style: "padding:14px 16px;",
                            div { style: "font-size:var(--fs-xs);font-weight:600;color:var(--fg-3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:10px;",
                                "Strategy overrides"
                            }
                            if diffs.is_empty() {
                                p { style: "font-size:var(--fs-xs);color:var(--fg-4);margin:0;",
                                    "No strategy overrides applied."
                                }
                            }
                            for diff in &diffs {
                                div {
                                    key: "{diff.table_name}",
                                    style: "margin-bottom:12px;",
                                    div { style: "font-size:var(--fs-xs);font-family:var(--font-code);color:var(--fg);margin-bottom:5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                                        "{diff.table_name}"
                                    }
                                    div { style: "display:flex;align-items:center;gap:5px;flex-wrap:wrap;",
                                        span { class: "badge muted", "{diff.from_lbl}" }
                                        span { style: "color:var(--fg-4);font-size:var(--fs-xs);", "→" }
                                        span { class: "badge {diff.to_cls}", "{diff.to_lbl}" }
                                    }
                                }
                            }
                        }
                    }

                    div { class: "collapsed-strip",
                        onclick: move |_| { right_collapsed.set(false); },
                        div { class: "ic-wrap", "S" }
                        span { class: "lbl", "Summary" }
                    }
                }
            }

            // ── Bottom bar ─────────────────────────────────────────────────
            div { class: "subbar",
                button {
                    class: "btn ghost",
                    onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                    "← Back to Strategies"
                }
                span { class: "grow" }
                button {
                    class: "btn primary",
                    onclick: move |_| { state.write().screen = AppScreen::Import; },
                    "Start Import →"
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// DDL syntax-highlighted line
// ---------------------------------------------------------------------------

#[component]
fn DdlLine(line: String) -> Element {
    let tokens = tokenize_ddl(&line);
    rsx! {
        span {
            for tok in &tokens {
                span { class: "{tok.cls}", "{tok.text}" }
            }
            "\n"
        }
    }
}

struct DdlToken {
    cls: &'static str,
    text: String,
}

fn tokenize_ddl(line: &str) -> Vec<DdlToken> {
    const KW: &[&str] = &[
        "CREATE", "TABLE", "IF", "NOT", "EXISTS",
        "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
        "ON", "DELETE", "CASCADE", "SET",
        "NULL", "DEFAULT", "GENERATED", "ALWAYS", "AS", "IDENTITY",
        "CONSTRAINT", "UNIQUE",
    ];
    const TY: &[&str] = &[
        "BIGINT", "INTEGER", "INT", "SMALLINT",
        "VARCHAR", "TEXT", "NUMERIC", "DECIMAL", "FLOAT",
        "BOOLEAN", "BOOL", "JSONB", "JSON",
        "TIMESTAMPTZ", "TIMESTAMP", "DATE", "UUID", "SERIAL",
    ];

    let mut tokens = Vec::new();
    let mut remaining = line;

    while !remaining.is_empty() {
        // Whitespace run
        let ws = remaining.find(|c: char| !c.is_whitespace()).unwrap_or(remaining.len());
        if ws > 0 {
            tokens.push(DdlToken { cls: "", text: remaining[..ws].to_string() });
            remaining = &remaining[ws..];
            continue;
        }
        // Single-char punctuation
        if remaining.starts_with(|c: char| "(),;".contains(c)) {
            tokens.push(DdlToken { cls: "pn", text: remaining[..1].to_string() });
            remaining = &remaining[1..];
            continue;
        }
        // Word token
        let word_end = remaining
            .find(|c: char| c.is_whitespace() || "(),;".contains(c))
            .unwrap_or(remaining.len());
        let word  = &remaining[..word_end];
        let upper = word.to_ascii_uppercase();
        let cls = if KW.contains(&upper.as_str()) {
            "kw"
        } else if TY.contains(&upper.as_str()) {
            "ty"
        } else {
            ""
        };
        tokens.push(DdlToken { cls, text: word.to_string() });
        remaining = &remaining[word_end..];
    }

    tokens
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct DiffEntry {
    table_name: String,
    from_lbl:   &'static str,
    to_cls:     &'static str,
    to_lbl:     &'static str,
}
