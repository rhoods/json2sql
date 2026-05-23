// CSS class name constants generated at compile time from assets/styles.css.
//
// Usage in rsx!:
//   button { class: css::BTN, ... }
//   div    { class: css::SPLIT_3, ... }
//
// A typo → compile error. A renamed class in styles.css → all usages break loudly.
json2sql_css_macro::generate_css_consts!();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btn_class_exists() {
        assert_eq!(BTN, "btn");
    }

    #[test]
    fn split_3_class_exists() {
        assert_eq!(SPLIT_3, "split-3");
    }

    #[test]
    fn badge_class_exists() {
        assert_eq!(BADGE, "badge");
    }

    #[test]
    fn const_name_is_screaming_snake() {
        assert_eq!(STEP_CARD, "step-card");
        assert_eq!(LOG, "log");
    }

    #[test]
    fn utility_classes_used_in_screens_exist() {
        assert_eq!(GROW, "grow");
        assert_eq!(FG_2, "fg-2");
        assert_eq!(FG_3, "fg-3");
        assert_eq!(FG_4, "fg-4");
        assert_eq!(FS_XS, "fs-xs");
        assert_eq!(FS_SM, "fs-sm");
        assert_eq!(W_100, "w-100");
    }
}
