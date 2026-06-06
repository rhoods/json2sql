
/// Full stylesheet embedded at compile time from `assets/styles.css`.
/// This is the single source of truth for all visual styles:
/// design tokens, component classes, layout primitives, and webkit2gtk overrides.
pub const fn design_css() -> &'static str {
    include_str!("../assets/styles.css")
}

#[cfg(test)]
mod design_css_tests {
    use super::*;

    #[test]
    fn design_css_is_non_empty() {
        assert!(!design_css().is_empty());
    }

    #[test]
    fn design_css_contains_root_tokens() {
        let css = design_css();
        assert!(css.contains("--bg:"), "missing --bg surface token");
        assert!(css.contains("--acc:"), "missing --acc accent token");
        assert!(css.contains("--fg:"), "missing --fg text token");
        assert!(css.contains("--bd:"), "missing --bd border token");
    }

    #[test]
    fn design_css_contains_strategy_badges() {
        let css = design_css();
        assert!(css.contains(".badge.default"), "missing default badge");
        assert!(css.contains(".badge.jsonb"), "missing jsonb badge");
        assert!(css.contains(".badge.normalize"), "missing normalize badge");
        assert!(css.contains(".badge.skip"), "missing skip badge");
        assert!(css.contains(".badge.flatten"), "missing flatten badge");
    }

    #[test]
    fn design_css_contains_strat_btn_component() {
        let css = design_css();
        assert!(css.contains(".strat-btn"), "missing strat-btn radio-style button");
        assert!(css.contains(".strat-list"), "missing strat-list container");
    }

    #[test]
    fn design_css_contains_split_layout() {
        let css = design_css();
        assert!(css.contains(".split-3"), "missing split-3 three-pane layout");
        assert!(css.contains(".splitter"), "missing splitter drag handle");
        assert!(css.contains(".pane.collapsed"), "missing collapsed pane state");
    }

    #[test]
    fn design_css_contains_stepper() {
        let css = design_css();
        assert!(css.contains(".step-card"), "missing step-card component");
        assert!(css.contains(".step-head"), "missing step-head");
        assert!(css.contains(".step-card.active"), "missing active step state");
        assert!(css.contains(".step-card.done"), "missing done step state");
    }

    #[test]
    fn design_css_contains_stat_tile() {
        let css = design_css();
        assert!(css.contains(".stat-tile"), "missing stat-tile component");
    }

    #[test]
    fn design_css_contains_webkit_input_override() {
        let css = design_css();
        assert!(
            css.contains("-webkit-text-fill-color"),
            "missing webkit input override (required for GTK/webkit2gtk dark theme)"
        );
    }
}
