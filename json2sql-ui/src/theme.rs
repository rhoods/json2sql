#![allow(dead_code)] // tokens définis en avance pour les écrans à venir
// ---------------------------------------------------------------------------
// Design system tokens — "The Architectural Logic" (see docs/DESIGN.md)
// ---------------------------------------------------------------------------
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

// Accent semantic tokens
pub const PRIMARY: &str = "#99CBFF";       // action / intent
pub const PRIMARY_DARK: &str = "#007BC4";  // gradient end
pub const SECONDARY: &str = "#4EDEA3";     // success / connected
pub const TERTIARY: &str = "#FFB95F";      // warning / truncation
pub const ERROR: &str = "#FFB4AB";         // error / failure

// Ghost border fallback (use sparingly)
pub const OUTLINE_VARIANT: &str = "#404752"; // at 40% opacity in practice

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
// Note: component styles (.btn-primary, .btn-ghost, .input-field, etc.) live
// in the CSS injected via with_custom_head() in main.rs — not here.
// This file contains only design tokens (raw values, no CSS fragments).
// ---------------------------------------------------------------------------
