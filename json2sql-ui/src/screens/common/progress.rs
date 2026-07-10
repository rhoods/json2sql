//! Barre de progression partagée — calcul de pourcentage, classe CSS d'animation,
//! composant Dioxus utilisé par les écrans Analysis et Import.
//!
//! Fonctions :
//! - fn `progress_pct` — calcule un pourcentage de progression (`done`/`total`, borné à 100)
//! - fn `progress_bar_class` — classe CSS de la barre de progression (animation indéterminée avant démarrage)
//! - fn `ProgressBar` — composant : barre de progression labellisée (pourcentage, phase, légende)
#![allow(clippy::derive_partial_eq_without_eq)]
use dioxus::prelude::*;

/// Compute a progress percentage from `done` units out of `total`.
/// Returns 0 if total is 0, clamps to 100 if done >= total.
pub fn progress_pct(done: u64, total: u64) -> u32 {
    if total == 0 { return 0; }
    ((done * 100 / total).min(100)) as u32
}

/// Returns the CSS class for a progress bar track.
/// `indeterminate` (scanning animation) only when waiting to start (pct == 0 and not done).
/// Once progress begins, the bar fills deterministically via `width:{pct}%`.
pub const fn progress_bar_class(done: bool, pct: u32) -> &'static str {
    if !done && pct == 0 { "prog thick indeterminate" } else { "prog thick" }
}

/// A labeled progress bar used in `AnalysisScreen` and `ImportScreen`.
///
/// - `pct`   : 0–100
/// - `done`  : if true, bar is solid (no animation)
/// - `label` : caption line below the bar (bytes/rows/ETA)
/// - `phase` : short phase name shown as a prefix badge (e.g. "Streaming")
#[allow(clippy::derive_partial_eq_without_eq)]
#[component]
pub fn ProgressBar(pct: u32, done: bool, label: String, phase: String) -> Element {
    let cls = progress_bar_class(done, pct);
    // rsx! macro expansion triggers a disallowed_methods false positive (no unwrap() below) —
    // scoped to this block only, same pattern as main.rs:App().
    #[allow(clippy::disallowed_methods)]
    {
        rsx! {
            div {
                div { style: "display:flex;align-items:center;gap:8px;margin-bottom:4px;",
                    span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;min-width:32px;",
                        "{pct}%"
                    }
                    span { style: "font-size:var(--fs-xs);color:var(--fg-2);font-weight:600;", "{phase}" }
                }
                div { class: "{cls}",
                    i { style: "width:{pct}%;", "" }
                }
                span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;",
                    "{label}"
                }
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    #[test]
    fn progress_pct_zero_when_total_is_zero() {
        assert_eq!(progress_pct(0, 0), 0);
    }

    #[test]
    fn progress_pct_zero_at_start() {
        assert_eq!(progress_pct(0, 1000), 0);
    }

    #[test]
    fn progress_pct_half() {
        assert_eq!(progress_pct(500, 1000), 50);
    }

    #[test]
    fn progress_pct_full() {
        assert_eq!(progress_pct(1000, 1000), 100);
    }

    #[test]
    fn progress_pct_capped_at_100_when_done_exceeds_total() {
        assert_eq!(progress_pct(1500, 1000), 100);
    }

    #[test]
    fn bar_class_indeterminate_only_when_not_started() {
        // pct=0, not done → waiting to start → indeterminate animation
        assert_eq!(progress_bar_class(false, 0), "prog thick indeterminate");
    }

    #[test]
    fn bar_class_deterministic_once_progress_begins() {
        // pct>0, not done → filling progressively, no scanning animation
        assert_eq!(progress_bar_class(false, 1), "prog thick");
        assert_eq!(progress_bar_class(false, 50), "prog thick");
        assert_eq!(progress_bar_class(false, 99), "prog thick");
    }

    #[test]
    fn bar_class_solid_when_done() {
        assert_eq!(progress_bar_class(true, 100), "prog thick");
        // Edge: done=true but pct still 0 (e.g. empty phase) → no animation
        assert_eq!(progress_bar_class(true, 0), "prog thick");
    }
}
