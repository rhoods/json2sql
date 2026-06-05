//! Surveillance mémoire système — RSS et pression RAM.
//!
//! Utilitaires prévus pour déclencher un flush anticipé des sinks quand la RAM est saturée
//! (risque d'OOM sur les très grands fichiers). Non encore câblés dans les runners de prod —
//! les fonctions sont conservées prêtes à l'emploi.
#![allow(dead_code)]

use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

/// Returns the current process's resident set size in bytes.
/// Returns `None` if the information is unavailable (unsupported OS,
/// restricted container, or other system error).
#[must_use]
pub fn rss_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_memory(),
    );
    sys.process(pid).map(|p| p.memory())
}

/// Returns total installed RAM in bytes.
/// Returns `None` if the value is zero or unavailable.
#[must_use]
pub fn total_memory_bytes() -> Option<u64> {
    let mut sys = System::new();
    sys.refresh_memory();
    let total = sys.total_memory();
    if total == 0 { None } else { Some(total) }
}

/// Returns `true` when the process RSS exceeds `threshold_pct` percent of
/// total system RAM. Returns `false` if either value is unavailable (safe
/// fallback: no drain triggered by RAM pressure).
#[must_use]
pub fn ram_pressure_exceeded(threshold_pct: u8) -> bool {
    let Some(rss) = rss_bytes() else { return false };
    let Some(total) = total_memory_bytes() else { return false };
    rss >= total * threshold_pct as u64 / 100
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_bytes_is_nonzero() {
        let rss = rss_bytes();
        assert!(rss.is_some(), "rss_bytes must return Some on a running process");
        assert!(rss.unwrap() > 0, "RSS must be positive");
    }

    #[test]
    fn total_memory_bytes_is_nonzero() {
        let total = total_memory_bytes();
        assert!(total.is_some(), "total_memory_bytes must return Some");
        assert!(total.unwrap() > 0);
    }

    #[test]
    fn rss_does_not_exceed_total() {
        if let (Some(rss), Some(total)) = (rss_bytes(), total_memory_bytes()) {
            assert!(rss <= total, "RSS ({rss}) must not exceed total RAM ({total})");
        }
    }

    #[test]
    fn ram_pressure_exceeded_at_zero_pct_is_true() {
        // 0% threshold → always exceeded (any RSS > 0)
        assert!(ram_pressure_exceeded(0));
    }

    #[test]
    fn ram_pressure_exceeded_at_100_pct_is_false() {
        // 100% threshold → only exceeded if we're using all RAM
        assert!(!ram_pressure_exceeded(100));
    }
}
