/// Extracts simple CSS class names from a CSS string.
///
/// "Simple" means: a selector that is exactly `.classname` with nothing else —
/// no pseudo-classes (`:hover`), no compound selectors (`.a.b`), no descendant
/// selectors (`.a .b`).
///
/// Strategy: split the stylesheet into selector/block pairs by scanning for `{`
/// and `}`, then validate each selector with a strict regex.
///
/// Returns deduplicated class names in order of first appearance.
pub fn extract_simple_classes(css: &str) -> Vec<String> {
    let css = strip_comments(css);
    let simple_class = regex::Regex::new(r"^\s*\.([a-zA-Z][a-zA-Z0-9_-]*)\s*$").unwrap();

    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();

    for selector in selectors_from(&css) {
        if let Some(cap) = simple_class.captures(&selector) {
            let class = cap[1].to_string();
            if seen.insert(class.clone()) {
                result.push(class);
            }
        }
    }

    result
}

/// Yields the selector string for each `{ ... }` block found in `css`.
/// Handles one level of nesting (e.g. `@media { .foo { } }`) by skipping
/// at-rule blocks without yielding their outer selector.
fn selectors_from(css: &str) -> Vec<String> {
    let mut selectors = Vec::new();
    let mut depth: u32 = 0;
    let mut selector_start = 0;
    let chars: Vec<char> = css.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            '{' => {
                if depth == 0 {
                    let selector: String = chars[selector_start..i].iter().collect();
                    // Skip at-rules (@media, @keyframes, etc.)
                    if !selector.trim_start().starts_with('@') {
                        selectors.push(selector);
                    }
                }
                depth += 1;
            }
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    selector_start = i + 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    selectors
}

/// Converts a CSS class name (kebab-case) to a Rust SCREAMING_SNAKE_CASE const name.
pub fn class_to_const_name(class: &str) -> String {
    class.replace('-', "_").to_uppercase()
}

fn strip_comments(css: &str) -> String {
    let mut result = String::with_capacity(css.len());
    let mut rest = css;
    while let Some(start) = rest.find("/*") {
        result.push_str(&rest[..start]);
        rest = match rest[start..].find("*/") {
            Some(end) => &rest[start + end + 2..],
            None => break,
        };
    }
    result.push_str(rest);
    result
}
