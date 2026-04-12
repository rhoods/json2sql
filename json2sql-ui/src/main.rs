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
    // On Linux/webkit2gtk, input elements need an explicit .focus() call after a
    // mousedown event — otherwise the webview receives the click but the DOM element
    // never gets keyboard focus and typing is silently dropped.
    let head = r#"<style>
/* Override webkit system theme for all inputs */
input, textarea, select {
    -webkit-appearance: none;
    -webkit-text-fill-color: #E4E2E6 !important;
    background-color: #353535 !important;
    color: #E4E2E6 !important;
    font-size: 0.8125rem;
    min-height: 32px;
    caret-color: #E4E2E6;
}
input::placeholder, textarea::placeholder {
    -webkit-text-fill-color: #717680 !important;
    opacity: 1;
}
/* Autofill background override */
input:-webkit-autofill {
    -webkit-box-shadow: 0 0 0 100px #353535 inset !important;
    -webkit-text-fill-color: #E4E2E6 !important;
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
        div { style: "{theme::STYLE_ROOT}",
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
