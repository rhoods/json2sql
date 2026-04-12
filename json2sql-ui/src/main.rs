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
    dioxus::LaunchBuilder::new()
        .with_cfg(dioxus::desktop::Config::new().with_window(
            dioxus::desktop::WindowBuilder::new()
                .with_title("json2sql")
                .with_inner_size(dioxus::desktop::LogicalSize::new(1280.0_f64, 800.0_f64))
                .with_resizable(true),
        ))
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
