mod config;
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
    // All CSS lives in theme::css() — theme.rs is the single source of truth.
    // The JS focus patch is webkit2gtk-specific behaviour, not a design concern,
    // so it stays here alongside the Dioxus launch configuration.
    // Layer 1: Google Fonts — Inter (UI) + JetBrains Mono (code/data).
    // Falls back gracefully to system-ui / monospace when offline.
    let fonts_link = r#"<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">"#;

    // Layer 2: design system (tokens + primitives + layouts from Claude Design handoff).
    // Layer 3: webkit2gtk overrides + legacy tokens (existing screens still use these).
    let head = format!(
        "{fonts_link}\n\
<style>{design}\n{legacy}</style>\n\
<script>\n\
document.addEventListener('DOMContentLoaded', function () {{\n\
    document.addEventListener('mousedown', function (e) {{\n\
        var el = e.target;\n\
        if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {{\n\
            setTimeout(function () {{ el.focus(); }}, 0);\n\
        }}\n\
    }}, true);\n\
}});\n\
</script>",
        fonts_link = fonts_link,
        design = theme::design_css(),
        legacy = theme::css(),
    );

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
    // Load persisted config from ~/.config/json2sql/last_project.toml on first mount.
    let state: Signal<AppState> = use_signal(|| {
        let mut s = AppState::default();
        if let Some(cfg) = crate::config::load() {
            cfg.apply_to(&mut s.project);
        }
        s
    });

    let screen = state.read().screen.clone();

    rsx! {
        div { style: "background:var(--bg-root);color:var(--on-surface);font-family:var(--font-ui);height:100vh;overflow:hidden;",
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
