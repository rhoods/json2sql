//! Screen 1 — Project Setup (vertical stepper wizard)
//!
//! 4 accordion step-cards: Source · Optional outputs · Target · Advanced.
//! CSS classes come from the embedded design system (assets/styles.css).
#![allow(clippy::disallowed_methods, clippy::derive_partial_eq_without_eq)]
use dioxus::prelude::*;

use json2sql::schema::strategies::StrategyName;
use crate::screens::{pick_file, pick_folder, PickResult};
use crate::state::{format_bytes, AppScreen, AppState};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

#[allow(clippy::derive_partial_eq_without_eq)]
#[component]
pub fn SetupScreen(mut state: Signal<AppState>) -> Element {
    // ── Accordion state ───────────────────────────────────────────────────
    let init_workers   = state.read().project.workers;
    let init_p2        = state.read().project.pass2_parallel;
    let init_step: usize = if state.read().project.source_file.is_none() { 0 }
        else if !state.read().project.pg.is_complete() { 2 }
        else { 3 };

    let mut active_step: Signal<usize> = use_signal(|| init_step);

    // Pass 1 / Pass 2 parallel toggle + saved value (for restore on re-enable)
    let mut p1_on   = use_signal(|| init_workers > 1);
    let mut p1_save = use_signal(|| init_workers.max(4));
    let mut p2_on   = use_signal(|| init_p2 > 1);
    let mut p2_save = use_signal(|| init_p2.max(4));

    // ── Async file-size probe ─────────────────────────────────────────────
    let mut source_size: Signal<Option<u64>> = use_signal(|| None);
    use_effect(move || {
        let path = state.read().project.source_file.clone();
        spawn(async move {
            let size = tokio::task::spawn_blocking(move || {
                path.as_ref().and_then(|p| std::fs::metadata(p).ok()).map(|m| m.len())
            })
            .await.ok().flatten();
            source_size.set(size);
        });
    });

    // ── Dialog guards ─────────────────────────────────────────────────────
    let mut picking_source  = use_signal(|| false);
    let mut picking_anomaly = use_signal(|| false);
    let mut picking_schema  = use_signal(|| false);
    let mut picker_error: Signal<Option<String>> = use_signal(|| None);
    let mut load_error:   Signal<Option<String>> = use_signal(|| None);

    // ── Derived values ────────────────────────────────────────────────────
    let source_file     = state.read().project.source_file.clone();
    let source_label    = source_file.as_ref()
        .and_then(|p| p.file_name()).map_or_else(|| "No file selected".to_string(), |n| n.to_string_lossy().into_owned());
    let size_bytes      = *source_size.read();
    let size_label      = size_bytes.map(format_bytes);
    let source_large    = size_bytes.is_some_and(|b| b > 5 * 1_073_741_824);

    let snapshot_loaded = state.read().schema.schema_snapshot_loaded;
    let snapshot_tables = state.read().schema.schemas.len();

    let pg_ok      = state.read().project.pg_ok;
    let pg_testing = state.read().project.pg_testing;
    let pg_host    = state.read().project.pg.host.clone();
    let pg_db      = state.read().project.pg.database.clone();
    let pg_schema  = state.read().project.pg_schema.clone();
    let schema_invalid = !pg_schema.chars().all(|c| c.is_alphanumeric() || c == '_');
    let drop_existing   = state.read().project.drop_existing;
    let workers         = state.read().project.workers;
    let pass2_parallel  = state.read().project.pass2_parallel;
    let available_cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);

    let anomaly_dir = state.read().project.anomaly_dir.clone();
    let anomaly_label = anomaly_dir.as_ref()
        .and_then(|p| p.file_name()).map_or_else(|| "None (anomalies discarded)".to_string(), |n| n.to_string_lossy().into_owned());

    let import_limit = state.read().project.import_limit;
    let verbose_logs = state.read().project.verbose_logs;
    let skip_constraints = state.read().project.skip_constraints;

    let step1_done = source_file.is_some();
    let step2_done = true; // optional — always OK
    let step3_done = state.read().project.pg.is_complete()
        && !pg_schema.is_empty() && !schema_invalid;
    let complete_count = [step1_done, step2_done, step3_done].iter().filter(|&&b| b).count();
    let ready = state.read().ready_to_start();

    // Step-card CSS class (done / active / todo)
    let act = *active_step.read();
    let s1_class = if act == 0 { "step-card active" } else if step1_done { "step-card done" } else { "step-card todo" };
    let s2_class = if act == 1 { "step-card active" } else if step2_done { "step-card done" } else { "step-card todo" };
    let s3_class = if act == 2 { "step-card active" } else if step3_done { "step-card done" } else { "step-card todo" };
    let s4_class = if act == 3 { "step-card active" } else { "step-card todo" };

    let p1_class = if *p1_on.read() { "toggle on" } else { "toggle" };
    let p2_class = if *p2_on.read() { "toggle on" } else { "toggle" };

    rsx! {
        div {
            style: "display:flex;flex-direction:column;height:100vh;background:var(--bg);",

            // ── Breadcrumb subbar ─────────────────────────────────────────
            div { class: "subbar",
                div { class: "crumb",
                    button { class: "step active", "Setup" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Analysis" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Strategy" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "SQL Preview" }
                    span { class: "sep", "›" }
                    button { class: "step fg-4", "Import" }
                }
                span { class: "grow" }
            }

            // ── Scrollable body ───────────────────────────────────────────
            div {
                style: "flex:1;overflow-y:auto;padding:36px 24px 80px;background:var(--bg);",
                div { style: "max-width:720px;margin:0 auto;",

                    // Header
                    header { style: "margin-bottom:24px;display:flex;align-items:flex-end;gap:14px;",
                        div {
                            h1 { style: "margin:0;font-size:24px;font-weight:700;letter-spacing:-0.01em;color:var(--fg);",
                                "New import project"
                            }
                            p { style: "margin:4px 0 0;color:var(--fg-3);font-size:var(--fs-sm);",
                                "Configure source, target, and options."
                            }
                        }
                        span { class: "grow" }
                        button {
                            class: "btn ghost sm",
                            onclick: move |_| {
                                // Reset project to defaults; keep screen
                                state.write().project = crate::state::ProjectState::default();
                                p1_on.set(false);
                                p2_on.set(false);
                                active_step.set(0);
                            },
                            "↩ Reset"
                        }
                    }

                    // ── Stepper ───────────────────────────────────────────
                    div { class: "stepper",

                        // ── Step 1 — Source file ──────────────────────────
                        section { class: "{s1_class}",
                            header {
                                class: "step-head",
                                onclick: move |_| {
                                    active_step.set(if act == 0 { usize::MAX } else { 0 });
                                },
                                span { class: "ix",
                                    if step1_done && act != 0 { "✓" } else { "1" }
                                }
                                div { class: "ti",
                                    div { class: "h", "Source file" }
                                    div { class: "s", "Local JSON or JSONL — what we'll analyze" }
                                }
                                div { class: "right",
                                    if step1_done {
                                        span { "{source_label}" }
                                        if let Some(ref sz) = size_label {
                                            span { class: "badge muted sq", "{sz}" }
                                        }
                                    } else {
                                        span { class: "fg-4", "not set" }
                                    }
                                }
                                span { class: "caret", "›" }
                            }
                            div { class: "step-body",
                                div { class: "field-row",
                                    label { "File" }
                                    input {
                                        class: "input grow",
                                        r#type: "text",
                                        readonly: true,
                                        value: "{source_label}",
                                    }
                                    button {
                                        class: "btn",
                                        disabled: picking_source(),
                                        onclick: move |_| async move {
                                            if picking_source() { return; }
                                            picking_source.set(true);
                                            picker_error.set(None);
                                            let result = pick_file(&[
                                                ("JSON / JSONL", "*.json *.jsonl *.ndjson"),
                                            ]).await;
                                            picking_source.set(false);
                                            match result {
                                                PickResult::Selected(path) => {
                                                    state.write().project.source_file = Some(path);
                                                    crate::config::try_save(&state.read().project);
                                                    active_step.set(if state.read().project.pg.is_complete() { 3 } else { 2 });
                                                }
                                                PickResult::Cancelled => {}
                                                PickResult::NotAvailable => {
                                                    picker_error.set(Some("File picker unavailable".to_string()));
                                                }
                                            }
                                        },
                                        if picking_source() { "Opening…" } else { "Browse…" }
                                    }
                                }
                                if let Some(ref err) = picker_error() {
                                    p { style: "color:var(--danger);font-size:var(--fs-xs);margin:6px 0 0;", "{err}" }
                                }
                                if step1_done {
                                    div { class: "row mt-md fs-xs fg-3",
                                        if let Some(ref sz) = size_label {
                                            span { b { class: "fg-2", "Size" } " {sz}" }
                                            span { "·" }
                                        }
                                        span { b { class: "fg-2", "Format" } " JSON / JSONL" }
                                    }
                                    if source_large {
                                        div { class: "alert warn mt-md",
                                            span { "⚠" }
                                            div { b { "Large file" } " — analysis may take several minutes. We recommend at least 4 workers." }
                                        }
                                    }
                                }
                            }
                        }

                        // ── Step 2 — Optional outputs ─────────────────────
                        section { class: "{s2_class}",
                            header {
                                class: "step-head",
                                onclick: move |_| {
                                    active_step.set(if act == 1 { usize::MAX } else { 1 });
                                },
                                span { class: "ix",
                                    if act != 1 { "✓" } else { "2" }
                                }
                                div { class: "ti",
                                    div { class: "h", "Optional outputs" }
                                    div { class: "s", "Anomaly folder · schema snapshot to load" }
                                }
                                div { class: "right",
                                    if anomaly_dir.is_some() {
                                        span { "{anomaly_label}" }
                                    } else {
                                        span { class: "fg-4", "no anomaly folder" }
                                    }
                                    span { class: "fg-4", "·" }
                                    if snapshot_loaded {
                                        span { class: "badge success sq", "schema loaded" }
                                    } else {
                                        span { class: "fg-4", "no snapshot" }
                                    }
                                }
                                span { class: "caret", "›" }
                            }
                            div { class: "step-body",
                                // Anomaly folder
                                div { class: "field-row",
                                    label { "Anomaly folder" }
                                    input {
                                        class: "input grow",
                                        r#type: "text",
                                        readonly: true,
                                        value: "{anomaly_label}",
                                    }
                                    button {
                                        class: "btn sm",
                                        disabled: picking_anomaly(),
                                        onclick: move |_| async move {
                                            if picking_anomaly() { return; }
                                            picking_anomaly.set(true);
                                            let result = pick_folder().await;
                                            picking_anomaly.set(false);
                                            match result {
                                                PickResult::Selected(dir) => {
                                                    state.write().project.anomaly_dir = Some(dir);
                                                    crate::config::try_save(&state.read().project);
                                                }
                                                PickResult::Cancelled => {}
                                                PickResult::NotAvailable => {
                                                    picker_error.set(Some("File picker unavailable".to_string()));
                                                }
                                            }
                                        },
                                        "Choose…"
                                    }
                                    if anomaly_dir.is_some() {
                                        button {
                                            class: "btn ghost sm",
                                            onclick: move |_| {
                                                state.write().project.anomaly_dir = None;
                                                crate::config::try_save(&state.read().project);
                                            },
                                            "✕"
                                        }
                                    }
                                }
                                // Schema snapshot
                                div { class: "field-row mt-sm",
                                    label { "Schema snapshot" }
                                    input {
                                        class: "input grow",
                                        r#type: "text",
                                        readonly: true,
                                        placeholder: "— none —",
                                        value: if snapshot_loaded { format!("{snapshot_tables} tables") } else { String::new() },
                                    }
                                    button {
                                        class: "btn sm",
                                        disabled: picking_schema(),
                                        onclick: move |_| async move {
                                            if picking_schema() { return; }
                                            picking_schema.set(true);
                                            load_error.set(None);
                                            let result = pick_file(&[("Schema snapshot", "*.json")]).await;
                                            match result {
                                                PickResult::Selected(path) => {
                                                    let res = tokio::task::spawn_blocking(move || {
                                                        json2sql::schema::persistence::load(&path)
                                                    }).await;
                                                    picking_schema.set(false);
                                                    match res {
                                                        Ok(Ok(snap)) => {
                                                            state.write().load_snapshot(snap);
                                                            active_step.set(2);
                                                        }
                                                        Ok(Err(e)) => load_error.set(Some(format!("Failed: {e}"))),
                                                        Err(_) => load_error.set(Some("Task panicked".to_string())),
                                                    }
                                                }
                                                PickResult::Cancelled => { picking_schema.set(false); }
                                                PickResult::NotAvailable => {
                                                    picking_schema.set(false);
                                                    load_error.set(Some("File picker unavailable".to_string()));
                                                }
                                            }
                                        },
                                        if picking_schema() { "Loading…" } else { "Load .json…" }
                                    }
                                    if snapshot_loaded {
                                        button {
                                            class: "btn ghost sm",
                                            style: "color:var(--danger);",
                                            onclick: move |_| { state.write().clear_snapshot(); },
                                            "✕"
                                        }
                                    }
                                }
                                if let Some(ref err) = load_error() {
                                    p { style: "color:var(--danger);font-size:var(--fs-xs);margin:6px 0 0;", "{err}" }
                                }
                                if snapshot_loaded {
                                    div { class: "alert success compact mt-sm",
                                        span { "✓" }
                                        div {
                                            b { "{snapshot_tables} tables" }
                                            " loaded from snapshot — "
                                            button {
                                                class: "btn ghost sm",
                                                onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                                                "Visualize →"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // ── Step 3 — Target — PostgreSQL ──────────────────
                        section { class: "{s3_class}",
                            header {
                                class: "step-head",
                                onclick: move |_| {
                                    active_step.set(if act == 2 { usize::MAX } else { 2 });
                                },
                                span { class: "ix",
                                    if step3_done && act != 2 { "✓" } else { "3" }
                                }
                                div { class: "ti",
                                    div { class: "h", "Target — PostgreSQL" }
                                    div { class: "s", "Where the migrated schema and rows will live" }
                                }
                                div { class: "right",
                                    if let Some(ok) = pg_ok {
                                        span {
                                            class: if ok { "cdot ok" } else { "cdot err" },
                                        }
                                        span {
                                            style: if ok { "color:var(--success);" } else { "color:var(--danger);" },
                                            if ok { "connected" } else { "failed" }
                                        }
                                    } else if pg_testing {
                                        span { class: "cdot warn" }
                                        span { style: "color:var(--warning);", "testing…" }
                                    } else if step3_done {
                                        span { style: "color:var(--fg-3);font-family:'JetBrains Mono',monospace;",
                                            "{pg_host}:{state.read().project.pg.port}/{pg_db}"
                                        }
                                    } else {
                                        span { class: "fg-4", "not configured" }
                                    }
                                }
                                span { class: "caret", "›" }
                            }
                            PgConnectionForm { state }
                        }

                        // ── Step 4 — Advanced ─────────────────────────────
                        section { class: "{s4_class}",
                            header {
                                class: "step-head",
                                onclick: move |_| {
                                    active_step.set(if act == 3 { usize::MAX } else { 3 });
                                },
                                span { class: "ix", "4" }
                                div { class: "ti",
                                    div { class: "h",
                                        "Advanced "
                                        span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-weight:400;", "— optional" }
                                    }
                                    div { class: "s", "Parallelism per pass, table handling" }
                                }
                                div { class: "right",
                                    span { class: "mono",
                                        "p1: {workers}w · p2: {pass2_parallel}w"
                                        if drop_existing { " · drop existing" }
                                    }
                                }
                                span { class: "caret", "›" }
                            }
                            div { class: "step-body",
                                // Two parallelism cards
                                div { style: "display:grid;grid-template-columns:1fr 1fr;gap:12px;",

                                    // Pass 1 card
                                    div { style: "padding:12px 14px;background:var(--bg);border:1px solid var(--bd);border-radius:var(--r-md);",
                                        div { class: "row", style: "justify-content:space-between;margin-bottom:8px;",
                                            span { style: "font-weight:600;font-size:var(--fs-sm);",
                                                span { class: "badge info sq", style: "height:18px;vertical-align:-2px;margin-right:6px;", "Pass 1" }
                                                "Schema analysis"
                                            }
                                            span {
                                                class: "{p1_class}",
                                                onclick: move |_| {
                                                    let on = *p1_on.read();
                                                    if on {
                                                        p1_save.set(state.read().project.workers);
                                                        state.write().project.workers = 1;
                                                        p1_on.set(false);
                                                    } else {
                                                        let saved = *p1_save.read();
                                                        state.write().project.workers = saved;
                                                        p1_on.set(true);
                                                    }
                                                    crate::config::try_save(&state.read().project);
                                                },
                                                span { class: "sw" }
                                            }
                                        }
                                        p { style: "font-size:var(--fs-xs);color:var(--fg-3);margin:0 0 10px;line-height:1.5;",
                                            "Split the source scan across workers — faster on large files."
                                        }
                                        div { class: "field-row",
                                            label { style: "min-width:60px;", "Workers" }
                                            input {
                                                class: "input sm",
                                                style: "width:64px;",
                                                r#type: "number",
                                                min: "1",
                                                max: "{available_cores}",
                                                value: "{workers}",
                                                disabled: !*p1_on.read(),
                                                oninput: move |e| {
                                                    if let Ok(n) = e.value().parse::<usize>() {
                                                        if n >= 1 {
                                                            state.write().project.workers = n;
                                                            p1_save.set(n);
                                                            crate::config::try_save(&state.read().project);
                                                        }
                                                    }
                                                },
                                            }
                                            span { style: "font-size:var(--fs-xs);color:var(--fg-3);",
                                                "1–{available_cores} · ~250 MB/worker"
                                            }
                                        }
                                    }

                                    // Pass 2 card
                                    div { style: "padding:12px 14px;background:var(--bg);border:1px solid var(--bd);border-radius:var(--r-md);",
                                        div { class: "row", style: "justify-content:space-between;margin-bottom:8px;",
                                            span { style: "font-weight:600;font-size:var(--fs-sm);",
                                                span { class: "badge acc sq", style: "height:18px;vertical-align:-2px;margin-right:6px;", "Pass 2" }
                                                "Data import"
                                            }
                                            span {
                                                class: "{p2_class}",
                                                onclick: move |_| {
                                                    let on = *p2_on.read();
                                                    if on {
                                                        p2_save.set(state.read().project.pass2_parallel);
                                                        state.write().project.pass2_parallel = 1;
                                                        p2_on.set(false);
                                                    } else {
                                                        let saved = *p2_save.read();
                                                        state.write().project.pass2_parallel = saved;
                                                        p2_on.set(true);
                                                    }
                                                    crate::config::try_save(&state.read().project);
                                                },
                                                span { class: "sw" }
                                            }
                                        }
                                        p { style: "font-size:var(--fs-xs);color:var(--fg-3);margin:0 0 10px;line-height:1.5;",
                                            "Bulk-load tables in parallel. Match your DB connection pool size."
                                        }
                                        div { class: "field-row",
                                            label { style: "min-width:60px;", "Workers" }
                                            input {
                                                class: "input sm",
                                                style: "width:64px;",
                                                r#type: "number",
                                                min: "1",
                                                max: "{available_cores}",
                                                value: "{pass2_parallel}",
                                                disabled: !*p2_on.read(),
                                                oninput: move |e| {
                                                    if let Ok(n) = e.value().parse::<usize>() {
                                                        if n >= 1 {
                                                            state.write().project.pass2_parallel = n;
                                                            p2_save.set(n);
                                                            crate::config::try_save(&state.read().project);
                                                        }
                                                    }
                                                },
                                            }
                                            span { style: "font-size:var(--fs-xs);color:var(--fg-3);",
                                                "1–{available_cores} · 1 conn/worker"
                                            }
                                        }
                                    }
                                }

                                // Info tip
                                div { style: "font-size:var(--fs-xs);color:var(--fg-3);margin-top:12px;padding:8px 12px;background:var(--info-bg);border-left:2px solid var(--info);border-radius:0 var(--r-sm) var(--r-sm) 0;line-height:1.5;",
                                    b { style: "color:var(--fg-2);", "Tip" }
                                    " — pass 1 is CPU-bound, pass 2 is I/O-bound. They scale independently."
                                }

                                div { class: "divider" }

                                h4 { style: "font-size:var(--fs-sm);margin:0 0 8px;color:var(--fg);", "Table handling" }
                                div { class: "row gap-lg", style: "flex-wrap:wrap;",
                                    span {
                                        class: if drop_existing { "toggle on" } else { "toggle" },
                                        onclick: move |_| {
                                            let new_val = !state.read().project.drop_existing;
                                            state.write().project.drop_existing = new_val;
                                            crate::config::try_save(&state.read().project);
                                        },
                                        span { class: "sw" }
                                        span { style: if drop_existing { "color:var(--warning);" } else { "color:var(--fg-2);" },
                                            "Drop existing tables (CASCADE)"
                                        }
                                    }
                                }
                                if drop_existing {
                                    div { class: "alert warn compact mt-sm",
                                        span { "⚠" }
                                        div { b { "Destructive" } " — all existing data in target tables will be deleted." }
                                    }
                                }

                                div { class: "divider" }

                                // ── Strategy selection ────────────────────
                                h4 { style: "font-size:var(--fs-sm);margin:0 0 8px;color:var(--fg);", "Strategy selection" }
                                p { style: "font-size:var(--fs-xs);color:var(--fg-3);margin:0 0 10px;line-height:1.5;",
                                    "Uncheck a strategy to prevent it from running during schema analysis. "
                                    "Wide-table split is mandatory and always runs."
                                }
                                div { style: "display:flex;flex-direction:column;gap:6px;",
                                    // Sibling detection checkbox
                                    {
                                        let sibling_on = !state.read().project.disabled_strategies.contains(&StrategyName::Sibling);
                                        rsx! {
                                            label { style: "display:flex;align-items:center;gap:8px;cursor:pointer;font-size:var(--fs-sm);",
                                                input {
                                                    r#type: "checkbox",
                                                    checked: sibling_on,
                                                    onchange: move |e| {
                                                        if e.value() == "true" {
                                                            state.write().project.disabled_strategies.remove(&StrategyName::Sibling);
                                                        } else {
                                                            state.write().project.disabled_strategies.insert(StrategyName::Sibling);
                                                        }
                                                        crate::config::try_save(&state.read().project);
                                                    },
                                                }
                                                span { "Sibling collapse " }
                                                span { style: "color:var(--fg-3);font-size:var(--fs-xs);", "(sibling)" }
                                            }
                                        }
                                    }
                                    // Pivot inference checkbox
                                    {
                                        let pivot_on = !state.read().project.disabled_strategies.contains(&StrategyName::Pivot);
                                        rsx! {
                                            label { style: "display:flex;align-items:center;gap:8px;cursor:pointer;font-size:var(--fs-sm);",
                                                input {
                                                    r#type: "checkbox",
                                                    checked: pivot_on,
                                                    onchange: move |e| {
                                                        if e.value() == "true" {
                                                            state.write().project.disabled_strategies.remove(&StrategyName::Pivot);
                                                        } else {
                                                            state.write().project.disabled_strategies.insert(StrategyName::Pivot);
                                                        }
                                                        crate::config::try_save(&state.read().project);
                                                    },
                                                }
                                                span { "Pivot inference " }
                                                span { style: "color:var(--fg-3);font-size:var(--fs-xs);", "(pivot)" }
                                            }
                                        }
                                    }
                                    // Structured pivot checkbox
                                    {
                                        let sp_on = !state.read().project.disabled_strategies.contains(&StrategyName::StructuredPivot);
                                        rsx! {
                                            label { style: "display:flex;align-items:center;gap:8px;cursor:pointer;font-size:var(--fs-sm);",
                                                input {
                                                    r#type: "checkbox",
                                                    checked: sp_on,
                                                    onchange: move |e| {
                                                        if e.value() == "true" {
                                                            state.write().project.disabled_strategies.remove(&StrategyName::StructuredPivot);
                                                        } else {
                                                            state.write().project.disabled_strategies.insert(StrategyName::StructuredPivot);
                                                        }
                                                        crate::config::try_save(&state.read().project);
                                                    },
                                                }
                                                span { "Structured pivot " }
                                                span { style: "color:var(--fg-3);font-size:var(--fs-xs);", "(structured_pivot)" }
                                            }
                                        }
                                    }
                                }
                                p { style: "font-size:var(--fs-xs);color:var(--fg-4);margin:8px 0 0;",
                                    "Wide-table split is mandatory and always runs."
                                }

                                div { class: "divider" }

                                // ── Sample import ─────────────────────────
                                h4 { style: "font-size:var(--fs-sm);margin:0 0 8px;color:var(--fg);", "Sample import" }
                                p { style: "font-size:var(--fs-xs);color:var(--fg-3);margin:0 0 10px;line-height:1.5;",
                                    "Limit pass 2 to the first N root objects. Pass 1 always runs on the full file. "
                                    "Empty = full import."
                                }
                                div { class: "field-row",
                                    label { "Limit to" }
                                    input {
                                        class: "input sm",
                                        style: "width:96px;",
                                        r#type: "number",
                                        min: "0",
                                        placeholder: "unlimited",
                                        value: import_limit.map(|n| n.to_string()).unwrap_or_default(),
                                        oninput: move |e| {
                                            let v = e.value();
                                            if v.is_empty() {
                                                state.write().project.import_limit = None;
                                            } else if let Ok(n) = v.parse::<u64>() {
                                                state.write().project.import_limit = Some(n);
                                            }
                                            crate::config::try_save(&state.read().project);
                                        },
                                    }
                                    span { style: "font-size:var(--fs-xs);color:var(--fg-3);", "root objects  (0 = DDL only)" }
                                }
                                if import_limit.is_some() {
                                    div { class: "alert warn compact mt-sm",
                                        span { "⚠" }
                                        div { b { "Sample mode" } " — only the first " b { "{import_limit.unwrap()}" } " root objects will be imported." }
                                    }
                                }

                                div { class: "divider" }

                                h4 { style: "font-size:var(--fs-sm);margin:0 0 8px;color:var(--fg);", "Diagnostic logs" }
                                label { style: "display:flex;align-items:center;gap:8px;cursor:pointer;font-size:var(--fs-sm);",
                                    input {
                                        r#type: "checkbox",
                                        checked: verbose_logs,
                                        onchange: move |e| {
                                            state.write().project.verbose_logs = e.value() == "true";
                                            crate::config::try_save(&state.read().project);
                                        },
                                    }
                                    span { "Verbose logs " }
                                    span { style: "color:var(--fg-3);font-size:var(--fs-xs);",
                                        "(RAM tick / sec, DISPATCH every 10k rows)"
                                    }
                                }

                                div { class: "divider" }

                                h4 { style: "font-size:var(--fs-sm);margin:0 0 8px;color:var(--fg);", "Constraints" }
                                label { style: "display:flex;align-items:center;gap:8px;cursor:pointer;font-size:var(--fs-sm);",
                                    input {
                                        r#type: "checkbox",
                                        checked: skip_constraints,
                                        onchange: move |e| {
                                            state.write().project.skip_constraints = e.value() == "true";
                                            crate::config::try_save(&state.read().project);
                                        },
                                    }
                                    span { "Skip constraints " }
                                    span { style: "color:var(--fg-3);font-size:var(--fs-xs);",
                                        "(omit PRIMARY KEY + FOREIGN KEY phase — faster for dev/exploration runs)"
                                    }
                                }
                            }
                        }
                    } // stepper

                    // ── Bottom action bar ─────────────────────────────────
                    div { class: "row mt-lg", style: "justify-content:space-between;padding-top:8px;",
                        span { style: "font-size:var(--fs-xs);color:var(--fg-3);",
                            "{complete_count} / 3 sections complete"
                            if ready { " · ready to begin analysis" }
                        }
                        div { class: "row gap-md",
                            if snapshot_loaded {
                                button {
                                    class: "btn ghost",
                                    onclick: move |_| { state.write().screen = AppScreen::Strategy; },
                                    "👁 Visualize schema"
                                }
                            }
                            button {
                                class: "btn primary lg",
                                disabled: !ready || snapshot_loaded,
                                onclick: move |_| { state.write().screen = AppScreen::Analysis; },
                                "Start Analysis ›"
                            }
                        }
                    }

                } // max-width wrapper
            } // scrollable body
        }
    }
}

// ---------------------------------------------------------------------------
// Sub-component: PostgreSQL connection form (step-body of step 3)
// ---------------------------------------------------------------------------

#[component]
fn PgConnectionForm(mut state: Signal<AppState>) -> Element {
    let pg_ok      = state.read().project.pg_ok;
    let pg_testing = state.read().project.pg_testing;
    let pg_error   = state.read().project.pg_error.clone();
    let pg_host    = state.read().project.pg.host.clone();
    let pg_port    = { let p = state.read().project.pg.port; if p == 0 { String::new() } else { p.to_string() } };
    let pg_db      = state.read().project.pg.database.clone();
    let pg_user    = state.read().project.pg.username.clone();
    let pg_pass    = state.read().project.pg.password.clone();
    let pg_schema  = state.read().project.pg_schema.clone();
    let schema_invalid = !pg_schema.chars().all(|c| c.is_alphanumeric() || c == '_');

    rsx! {
        div { class: "step-body",
            // Host + Port
            div { style: "display:grid;grid-template-columns:1fr 100px;gap:10px;",
                div { class: "field",
                    label { "Host" }
                    input {
                        class: "input",
                        r#type: "text",
                        value: "{pg_host}",
                        placeholder: "localhost",
                        oninput: move |e| {
                            state.write().project.pg.host = e.value();
                            crate::config::try_save(&state.read().project);
                        },
                    }
                }
                div { class: "field",
                    label { "Port" }
                    input {
                        class: "input",
                        r#type: "number",
                        value: "{pg_port}",
                        placeholder: "5432",
                        oninput: move |e| {
                            let v = e.value();
                            if v.is_empty() {
                                state.write().project.pg.port = 0;
                            } else if let Ok(p) = v.parse::<u16>() {
                                state.write().project.pg.port = p;
                            }
                            crate::config::try_save(&state.read().project);
                        },
                    }
                }
            }
            // Database
            div { class: "field mt-sm",
                label { "Database" }
                input {
                    class: "input",
                    r#type: "text",
                    value: "{pg_db}",
                    placeholder: "my_database",
                    oninput: move |e| {
                        state.write().project.pg.database = e.value();
                        crate::config::try_save(&state.read().project);
                    },
                }
            }
            // User + Password
            div { style: "display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px;",
                div { class: "field",
                    label { "User" }
                    input {
                        class: "input",
                        r#type: "text",
                        value: "{pg_user}",
                        placeholder: "postgres",
                        oninput: move |e| {
                            state.write().project.pg.username = e.value();
                            crate::config::try_save(&state.read().project);
                        },
                    }
                }
                div { class: "field",
                    label { "Password" }
                    input {
                        class: "input",
                        r#type: "password",
                        value: "{pg_pass}",
                        placeholder: "••••••••",
                        oninput: move |e| {
                            state.write().project.pg.password = e.value();
                            crate::config::try_save(&state.read().project);
                        },
                    }
                }
            }
            // Schema
            div { class: "field mt-sm",
                label { "Schema" }
                input {
                    class: "input",
                    r#type: "text",
                    value: "{pg_schema}",
                    placeholder: "public",
                    oninput: move |e| {
                        let v = e.value().trim().to_string();
                        state.write().project.pg_schema = if v.is_empty() { "public".to_string() } else { v };
                        crate::config::try_save(&state.read().project);
                    },
                }
                if schema_invalid {
                    span { style: "font-size:var(--fs-xs);color:var(--danger);",
                        "Only letters, digits and underscores"
                    }
                }
            }
            // Test connection
            div { class: "row mt-md", style: "justify-content:space-between;",
                button {
                    class: "btn",
                    disabled: pg_testing,
                    onclick: move |_| async move {
                        let url = state.read().project.pg.to_url();
                        state.write().project.pg_testing = true;
                        state.write().project.pg_ok = None;
                        state.write().project.pg_error = None;
                        let result = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            tokio_postgres::connect(&url, tokio_postgres::NoTls),
                        ).await;
                        let (ok, err_msg) = match result {
                            Ok(Ok(_))  => (true, None),
                            Ok(Err(e)) => (false, Some(e.to_string())),
                            Err(_)     => (false, Some("Connection timed out (5s)".to_string())),
                        };
                        state.write().project.pg_testing = false;
                        state.write().project.pg_ok = Some(ok);
                        state.write().project.pg_error = err_msg;
                    },
                    if pg_testing { "Testing…" } else { "⬡ Test connection" }
                }
            }
            // Connection result alert
            if pg_ok == Some(true) {
                div { class: "alert success compact mt-sm",
                    span { "✓" }
                    div { "Connected to "
                        b { "{pg_host}:{state.read().project.pg.port} / {pg_db}" }
                        " — schema "
                        code { style: "font-family:'JetBrains Mono',monospace;", "{pg_schema}" }
                    }
                }
            } else if pg_ok == Some(false) {
                div { class: "alert danger compact mt-sm",
                    span { "✗" }
                    div {
                        "Connection failed"
                        if let Some(ref err) = pg_error {
                            span { style: "display:block;font-size:var(--fs-xs);opacity:0.8;margin-top:2px;", "{err}" }
                        }
                    }
                }
            }
        }
    }
}

