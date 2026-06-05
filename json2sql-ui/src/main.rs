#![forbid(unsafe_code)]
#![deny(dead_code)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::nursery)]

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
    let fonts_link = r#"<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">"#;

    // JS patches injected at startup:
    // 1. Focus patch: webkit2gtk doesn't route keyboard focus on mousedown for form elements.
    // 2. Splitter drag: resize panes by dragging .splitter handles (JS-only, no Rust re-render).
    let head = format!(
        "{fonts_link}\n\
<style>{css}</style>\n\
<script>\n\
document.addEventListener('DOMContentLoaded', function () {{\n\
    var drag = null;\n\
    document.addEventListener('mousedown', function (e) {{\n\
        var el = e.target;\n\
        if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {{\n\
            setTimeout(function () {{ el.focus(); }}, 0);\n\
        }}\n\
        if (el.classList.contains('splitter')) {{\n\
            var next = el.nextElementSibling;\n\
            var prev = el.previousElementSibling;\n\
            var isLeft = next && next.classList.contains('fluid');\n\
            var pane = isLeft ? prev : next;\n\
            if (!pane) return;\n\
            drag = {{ splitter: el, pane: pane, isLeft: isLeft,\n\
                      startX: e.clientX, startW: pane.getBoundingClientRect().width }};\n\
            el.classList.add('dragging');\n\
            document.body.classList.add('dragging-col');\n\
            e.preventDefault();\n\
        }}\n\
    }}, true);\n\
    document.addEventListener('mousemove', function (e) {{\n\
        if (!drag) return;\n\
        var delta = e.clientX - drag.startX;\n\
        var newW = Math.max(160, drag.startW + (drag.isLeft ? delta : -delta));\n\
        drag.pane.style.flexBasis = newW + 'px';\n\
        drag.pane.style.minWidth = '0';\n\
    }});\n\
    document.addEventListener('mouseup', function () {{\n\
        if (!drag) return;\n\
        drag.splitter.classList.remove('dragging');\n\
        document.body.classList.remove('dragging-col');\n\
        drag = null;\n\
    }});\n\
}});\n\
</script>",
        fonts_link = fonts_link,
        css = theme::design_css(),
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
        div { style: "background:var(--bg);color:var(--fg);height:100vh;overflow:hidden;",
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
