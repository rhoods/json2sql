#![allow(dead_code)] // tokens définis en avance pour les écrans à venir
// ---------------------------------------------------------------------------
// Design system tokens — "The Architectural Logic" (see docs/DESIGN.md)
//
// This file is the single source of truth for all design values.
// The CSS injected by main.rs is generated from these constants via
// `css_variables()` so that changing a value here updates both inline
// Rust styles (`{theme::ON_SURFACE}`) and the CSS variable (`--on-surface`).
//
// Usage in rsx!:  style: "background: {BG_ROOT}; color: {ON_SURFACE}",
// ---------------------------------------------------------------------------

// Surface hierarchy (no-line rule — depth via background shifts, not borders)
pub const BG_ROOT: &str = "#131313";
pub const BG_WORKSPACE: &str = "#1B1B1C";
pub const BG_SIDEBAR: &str = "#2A2A2A";
pub const BG_INPUT: &str = "#353535";
pub const BG_EDITOR: &str = "#111111"; // surface_container_lowest

// Text
pub const ON_SURFACE: &str = "#E4E2E6";
pub const ON_SURFACE_VARIANT: &str = "#C5C6D0";
pub const ON_SURFACE_DIM: &str = "#717680";
/// Text color on top of the primary gradient (dark, for contrast).
pub const ON_PRIMARY: &str = "#0D0D0D";

// Accent semantic tokens
pub const PRIMARY: &str = "#99CBFF";        // action / intent
pub const PRIMARY_DARK: &str = "#007BC4";   // gradient end
pub const SECONDARY: &str = "#4EDEA3";      // success / connected
pub const SECONDARY_DARK: &str = "#00C47A"; // progress bar gradient end
pub const TERTIARY: &str = "#FFB95F";       // warning / truncation
pub const ERROR: &str = "#FFB4AB";          // error / failure

// Borders — semi-transparent; used as-is in CSS, not as base + opacity.
pub const OUTLINE_VARIANT: &str = "#40475266"; // #404752 at ~40% opacity

// Ghost button hover background: primary at 8% opacity (pre-computed — CSS
// cannot do rgba(var(--primary-rgb), 0.08) without a separate rgb channel var).
pub const PRIMARY_ALPHA_08: &str = "rgba(153, 203, 255, 0.08)";

// Strategy badge colors
pub const BADGE_DEFAULT: &str = "#4A90D9";   // blue
pub const BADGE_JSONB: &str = "#9B59B6";     // purple
pub const BADGE_FLATTEN: &str = "#27AE60";   // green
pub const BADGE_NORMALIZE: &str = "#E67E22"; // orange
pub const BADGE_SKIP: &str = "#E74C3C";      // red

// ---------------------------------------------------------------------------
// Typography helpers (used as CSS font-family values)
// ---------------------------------------------------------------------------
pub const FONT_UI: &str = "Inter, system-ui, sans-serif";
pub const FONT_CODE: &str = "'JetBrains Mono', 'Fira Code', monospace";

// ---------------------------------------------------------------------------
// CSS — complete stylesheet injected into the webkit2gtk webview head.
//
// `css()` is the single source of truth for all visual styles. It is called
// once by main.rs; changing a Rust constant above automatically updates both
// the CSS variable and every {theme::TOKEN} inline-style reference.
//
// Structure:
//   1. :root — CSS custom properties, generated from the Rust constants.
//   2. Webkit overrides — GTK system theme conflicts on form elements.
//   3. Component classes — .btn-primary, .btn-ghost, .input-field, etc.
//      All colors reference var(--token); no hardcoded hex in this section.
// ---------------------------------------------------------------------------
pub fn css() -> String {
    format!(
        r#"/* ── Design tokens ────────────────────────────────────────────────── */
:root, body {{
    --primary:            {PRIMARY};
    --primary-dark:       {PRIMARY_DARK};
    --secondary:          {SECONDARY};
    --secondary-dark:     {SECONDARY_DARK};
    --tertiary:           {TERTIARY};
    --error:              {ERROR};
    --bg-root:            {BG_ROOT};
    --bg-workspace:       {BG_WORKSPACE};
    --bg-sidebar:         {BG_SIDEBAR};
    --bg-input:           {BG_INPUT};
    --bg-editor:          {BG_EDITOR};
    --on-surface:         {ON_SURFACE};
    --on-surface-variant: {ON_SURFACE_VARIANT};
    --on-surface-dim:     {ON_SURFACE_DIM};
    --on-primary:         {ON_PRIMARY};
    --outline-variant:    {OUTLINE_VARIANT};
    --primary-alpha-08:   {PRIMARY_ALPHA_08};
    --font-ui:   {FONT_UI};
    --font-code: {FONT_CODE};
}}

/* ── Webkit input override (GTK system theme fix) ───────────────────── */
input:not([type="checkbox"]):not([type="radio"]):not([type="range"]),
textarea, select {{
    -webkit-appearance: none;
    -webkit-text-fill-color: var(--on-surface) !important;
    background-color: var(--bg-input) !important;
    color: var(--on-surface) !important;
    font-size: 0.8125rem;
    min-height: 32px;
    caret-color: var(--on-surface);
}}
input:not([type="checkbox"]):not([type="radio"])::placeholder,
textarea::placeholder {{
    -webkit-text-fill-color: var(--on-surface-dim) !important;
    opacity: 1;
}}
input:-webkit-autofill {{
    -webkit-box-shadow: 0 0 0 100px var(--bg-input) inset !important;
    -webkit-text-fill-color: var(--on-surface) !important;
}}
input[type="checkbox"], input[type="radio"] {{
    -webkit-appearance: auto !important;
    appearance: auto !important;
    background-color: transparent !important;
    min-height: unset !important;
    width: 16px;
    height: 16px;
    cursor: pointer;
}}

/* ── Base button reset ──────────────────────────────────────────────── */
button {{
    -webkit-appearance: none;
    font-family: Inter, system-ui, sans-serif;
    font-size: 0.8125rem;
}}

/* ── .btn-primary ───────────────────────────────────────────────────── */
.btn-primary {{
    background: linear-gradient(135deg, var(--primary), var(--primary-dark));
    color: var(--on-primary);
    -webkit-text-fill-color: var(--on-primary);
    border: none;
    border-radius: 2px;
    padding: 10px 20px;
    font-weight: 600;
    cursor: pointer;
}}
.btn-primary:hover  {{ filter: brightness(1.08); }}
.btn-primary:disabled,
.btn-primary[disabled] {{ opacity: 0.4; cursor: not-allowed; filter: none; }}

/* ── .btn-ghost ─────────────────────────────────────────────────────── */
.btn-ghost {{
    background: transparent;
    color: var(--primary);
    -webkit-text-fill-color: var(--primary);
    border: 1px solid var(--outline-variant);
    border-radius: 2px;
    padding: 10px 20px;
    cursor: pointer;
}}
.btn-ghost:hover {{ background: var(--primary-alpha-08); }}
.btn-ghost:disabled,
.btn-ghost[disabled] {{ opacity: 0.4; cursor: not-allowed; }}

/* ── .btn-ghost--sm (compact variant) ──────────────────────────────── */
.btn-ghost--sm {{
    padding: 5px 10px;
    font-size: 0.75rem;
}}

/* ── .input-field ───────────────────────────────────────────────────── */
.input-field {{
    background: var(--bg-input);
    color: var(--on-surface);
    -webkit-text-fill-color: var(--on-surface);
    -webkit-appearance: none;
    border: none;
    border-bottom: 1px solid var(--outline-variant);
    border-radius: 2px 2px 0 0;
    padding: 6px 10px;
    font-family: Inter, system-ui, sans-serif;
    font-size: 0.8125rem;
    width: 100%;
    box-sizing: border-box;
    min-height: 32px;
}}

/* ── .progress-track / .progress-bar ───────────────────────────────── */
.progress-track {{
    background: var(--bg-input);
    height: 6px;
    width: 100%;
    overflow: hidden;
    border-radius: 3px;
}}
.progress-bar {{
    background: linear-gradient(90deg, var(--secondary), var(--secondary-dark));
    height: 6px;
    border-radius: 0;
}}

/* ── .log-panel ─────────────────────────────────────────────────────── */
.log-panel {{
    background: var(--bg-editor);
    color: var(--on-surface-variant);
    font-family: 'JetBrains Mono', 'Fira Code', monospace;
    font-size: 0.8125rem;
    padding: 12px;
    overflow-y: auto;
}}"#
    )
}
