mod screens;
mod state;
mod theme;

use dioxus::prelude::*;

use state::{AppScreen, AppState};
use screens::{
    analysis::AnalysisScreen,
    import::ImportScreen,
    preview::PreviewScreen,
    setup::SetupScreen,
    strategy::StrategyScreen,
};


fn main() {
    // CSS + JS injected into the webkit2gtk webview head.
    //
    // Design approach:
    //   - CSS variables on :root for all design tokens
    //   - Semantic classes (.btn-primary, .btn-ghost, .input-field, …) for components
    //   - Webkit-specific overrides needed because webkit2gtk applies the GTK system
    //     theme to form controls, overriding inline `color:` with its own text colour.
    //     `-webkit-text-fill-color` takes precedence over `color` in webkit and must
    //     be set explicitly on every interactive element.
    //   - JS focus patch: webkit2gtk receives the native mousedown but doesn't route
    //     keyboard focus to the DOM target — force focus() after each mousedown.
    let head = r#"<style>
/* ── Design tokens ──────────────────────────────────────────────────────── */
:root, body {
    --primary:            #99CBFF;
    --primary-dark:       #007BC4;
    --secondary:          #4EDEA3;
    --tertiary:           #FFB95F;
    --error:              #FFB4AB;
    --bg-root:            #131313;
    --bg-workspace:       #1B1B1C;
    --bg-sidebar:         #2A2A2A;
    --bg-input:           #353535;
    --on-surface:         #E4E2E6;
    --on-surface-variant: #C5C6D0;
    --on-surface-dim:     #717680;
    --font-ui:   Inter, system-ui, sans-serif;
    --font-code: 'JetBrains Mono', 'Fira Code', monospace;
}

/* ── Webkit input override (GTK system theme fix) ───────────────────────── */
input:not([type="checkbox"]):not([type="radio"]):not([type="range"]),
textarea, select {
    -webkit-appearance: none;
    -webkit-text-fill-color: #E4E2E6 !important;
    background-color: #353535 !important;
    color: #E4E2E6 !important;
    font-size: 0.8125rem;
    min-height: 32px;
    caret-color: #E4E2E6;
}
input:not([type="checkbox"]):not([type="radio"])::placeholder,
textarea::placeholder {
    -webkit-text-fill-color: #717680 !important;
    opacity: 1;
}
input:-webkit-autofill {
    -webkit-box-shadow: 0 0 0 100px #353535 inset !important;
    -webkit-text-fill-color: #E4E2E6 !important;
}
input[type="checkbox"], input[type="radio"] {
    -webkit-appearance: auto !important;
    appearance: auto !important;
    background-color: transparent !important;
    min-height: unset !important;
    width: 16px;
    height: 16px;
    cursor: pointer;
}

/* ── Base button reset ───────────────────────────────────────────────────── */
button {
    -webkit-appearance: none;
    font-family: Inter, system-ui, sans-serif;
    font-size: 0.8125rem;
}

/* ── .btn-primary ────────────────────────────────────────────────────────── */
.btn-primary {
    background: linear-gradient(135deg, #99CBFF, #007BC4);
    color: #0D0D0D;
    -webkit-text-fill-color: #0D0D0D;
    border: none;
    border-radius: 2px;
    padding: 10px 20px;
    font-weight: 600;
    cursor: pointer;
}
.btn-primary:hover  { filter: brightness(1.08); }
.btn-primary:disabled,
.btn-primary[disabled] { opacity: 0.4; cursor: not-allowed; filter: none; }

/* ── .btn-ghost ──────────────────────────────────────────────────────────── */
.btn-ghost {
    background: transparent;
    color: #99CBFF;
    -webkit-text-fill-color: #99CBFF;
    border: 1px solid #40475266;
    border-radius: 2px;
    padding: 10px 20px;
    cursor: pointer;
}
.btn-ghost:hover { background: rgba(153, 203, 255, 0.08); }
.btn-ghost:disabled,
.btn-ghost[disabled] { opacity: 0.4; cursor: not-allowed; }

/* ── .btn-ghost--sm (compact variant) ───────────────────────────────────── */
.btn-ghost--sm {
    padding: 5px 10px;
    font-size: 0.75rem;
}

/* ── .input-field ────────────────────────────────────────────────────────── */
.input-field {
    background: #353535;
    color: #E4E2E6;
    -webkit-text-fill-color: #E4E2E6;
    -webkit-appearance: none;
    border: none;
    border-bottom: 1px solid #40475266;
    border-radius: 2px 2px 0 0;
    padding: 6px 10px;
    font-family: Inter, system-ui, sans-serif;
    font-size: 0.8125rem;
    width: 100%;
    box-sizing: border-box;
    min-height: 32px;
}

/* ── .progress-track / .progress-bar ────────────────────────────────────── */
.progress-track {
    background: #353535;
    height: 6px;
    width: 100%;
    overflow: hidden;
    border-radius: 3px;
}
.progress-bar {
    background: linear-gradient(90deg, #4EDEA3, #00C47A);
    height: 6px;
    border-radius: 0;
}

/* ── .log-panel ──────────────────────────────────────────────────────────── */
.log-panel {
    background: #111111;
    color: #C5C6D0;
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 0.8125rem;
    padding: 12px;
    overflow-y: auto;
}
</style>
<script>
document.addEventListener('DOMContentLoaded', function () {
    document.addEventListener('mousedown', function (e) {
        var el = e.target;
        if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
            setTimeout(function () { el.focus(); }, 0);
        }
    }, true);
});
</script>"#
        .to_string();

    dioxus::LaunchBuilder::new()
        .with_cfg(
            dioxus::desktop::Config::new()
                .with_custom_head(head)
                .with_window(
                    dioxus::desktop::WindowBuilder::new()
                        .with_title("json2sql")
                        .with_inner_size(dioxus::desktop::LogicalSize::new(1280.0_f64, 800.0_f64))
                        .with_resizable(true),
                ),
        )
        .launch(App);
}

#[component]
fn App() -> Element {
    // Global state — one Signal shared across all screens via props.
    let state: Signal<AppState> = use_signal(AppState::default);

    let screen = state.read().screen.clone();

    rsx! {
        div { style: "background:#131313;color:#E4E2E6;font-family:Inter,system-ui,sans-serif;height:100vh;overflow:hidden;",
            match screen {
                AppScreen::Setup    => rsx! { SetupScreen    { state } },
                AppScreen::Analysis => rsx! { AnalysisScreen { state } },
                AppScreen::Strategy => rsx! { StrategyScreen { state } },
                AppScreen::Preview  => rsx! { PreviewScreen  { state } },
                AppScreen::Import   => rsx! { ImportScreen   { state } },
            }
        }
    }
}
