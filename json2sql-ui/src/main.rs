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
    let head = format!(
        "<style>{}</style>\n\
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
        theme::css()
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
    let state: Signal<AppState> = use_signal(AppState::default);

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
