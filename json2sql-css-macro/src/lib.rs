use proc_macro::TokenStream;
use quote::quote;

mod parser;

/// Parses `assets/styles.css` at compile time and generates a `css` module
/// containing one `pub const` per simple CSS class found.
///
/// Usage:
/// ```ignore
/// json2sql_css_macro::generate_css_consts!();
/// // Generates: pub mod css { pub const BTN_PRIMARY: &str = "btn-primary"; ... }
/// ```
#[proc_macro]
pub fn generate_css_consts(input: TokenStream) -> TokenStream {
    let _ = input;

    let css_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../json2sql-ui/assets/styles.css");
    let css = match std::fs::read_to_string(css_path) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("Cannot read styles.css: {e}");
            return quote! { compile_error!(#msg); }.into();
        }
    };

    let classes = parser::extract_simple_classes(&css);

    let consts: Vec<_> = classes
        .iter()
        .map(|class| {
            let const_name_str = parser::class_to_const_name(class);
            let const_ident = syn::Ident::new(&const_name_str, proc_macro2::Span::call_site());
            let value = class.as_str();
            quote! { pub const #const_ident: &str = #value; }
        })
        .collect();

    // Constants are emitted directly — the caller controls the module structure
    // by placing the macro call inside a `mod` block (e.g. `mod css`).
    quote! { #(#consts)* }.into()
}

#[cfg(test)]
mod tests {
    use super::parser;

    #[test]
    fn extracts_simple_class() {
        let css = ".btn-primary { color: red; }";
        let classes = parser::extract_simple_classes(css);
        assert_eq!(classes, vec!["btn-primary"]);
    }

    #[test]
    fn extracts_multiple_classes() {
        let css = ".foo { } .bar { } .baz { }";
        let classes = parser::extract_simple_classes(css);
        assert_eq!(classes, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn ignores_pseudo_classes() {
        let css = ".btn-primary:hover { color: blue; }";
        let classes = parser::extract_simple_classes(css);
        assert!(classes.is_empty(), "pseudo-classes must not be extracted");
    }

    #[test]
    fn ignores_compound_selectors() {
        // descendant: ".foo .bar { }" → neither foo nor bar as standalone
        let css = ".foo .bar { color: red; }";
        let classes = parser::extract_simple_classes(css);
        assert!(classes.is_empty(), "compound selectors must not be extracted");
    }

    #[test]
    fn ignores_multi_class_selectors() {
        // ".badge.jsonb { }" — two classes chained, not a simple selector
        let css = ".badge.jsonb { color: purple; }";
        let classes = parser::extract_simple_classes(css);
        assert!(classes.is_empty(), "multi-class selectors must not be extracted");
    }

    #[test]
    fn ignores_comments() {
        let css = "/* .not-a-class { } */ .real { }";
        let classes = parser::extract_simple_classes(css);
        assert_eq!(classes, vec!["real"]);
    }

    #[test]
    fn deduplicates_classes() {
        let css = ".foo { } .foo:hover { } .foo { }";
        let classes = parser::extract_simple_classes(css);
        assert_eq!(classes, vec!["foo"]);
    }

    #[test]
    fn kebab_to_screaming_snake() {
        assert_eq!(parser::class_to_const_name("btn-primary"), "BTN_PRIMARY");
        assert_eq!(parser::class_to_const_name("log-panel"), "LOG_PANEL");
        assert_eq!(parser::class_to_const_name("split-3"), "SPLIT_3");
        assert_eq!(parser::class_to_const_name("surface"), "SURFACE");
    }
}
