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
// Common inline style fragments
// ---------------------------------------------------------------------------

/// Full-window root container.
pub const STYLE_ROOT: &str =
    "background:#131313;color:#E4E2E6;\
     font-family:Inter,system-ui,sans-serif;\
     height:100vh;overflow:hidden;";

/// Primary gradient CTA button (135° from PRIMARY to PRIMARY_DARK).
pub const STYLE_BTN_PRIMARY: &str =
    "background:linear-gradient(135deg,#99CBFF,#007BC4);\
     color:#0D0D0D;border:none;border-radius:2px;\
     padding:8px 20px;font-weight:600;cursor:pointer;";

/// Ghost secondary button.
pub const STYLE_BTN_GHOST: &str =
    "background:transparent;color:#99CBFF;\
     border:1px solid #40475266;border-radius:2px;\
     padding:8px 20px;cursor:pointer;";

/// Text input field.
pub const STYLE_INPUT: &str =
    "background:#353535;color:#E4E2E6;\
     border:none;border-bottom:1px solid #40475266;\
     border-radius:2px 2px 0 0;padding:6px 10px;\
     font-family:Inter,system-ui,sans-serif;width:100%;box-sizing:border-box;";

/// Progress bar track.
pub const STYLE_PROGRESS_TRACK: &str =
    "background:#353535;height:6px;width:100%;";

/// Progress bar indicator (gradient, no rounded corners — "Brutalist").
pub const STYLE_PROGRESS_BAR: &str =
    "background:linear-gradient(90deg,#4EDEA3,#00C47A);height:6px;border-radius:0;";

/// Monospace log panel.
pub const STYLE_LOG_PANEL: &str =
    "background:#111111;color:#C5C6D0;\
     font-family:'JetBrains Mono','Fira Code',monospace;\
     font-size:0.8125rem;padding:12px;overflow-y:auto;";
