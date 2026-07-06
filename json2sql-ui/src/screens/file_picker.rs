//! Sélecteurs de fichiers natifs (rfd) — dialogues cross-platform pour ouvrir/enregistrer
//! un fichier ou choisir un dossier, avec une garde anti-réentrance (un seul dialogue à la fois).
//!
//! Fonctions :
//! - enum `PickResult` — résultat d'un appel au picker de fichier/dossier
//! - fn `option_to_pick_result` — convertit un `Option<PathBuf>` rfd en `PickResult`
//! - fn `parse_ext` — découpe un motif glob (`"*.json *.jsonl"`) en liste d'extensions rfd
//! - struct `PickerGuard` — garde RAII : un seul dialogue actif à la fois, libère le verrou au drop
//! - fn `PickerGuard::drop` — remet `PICKER_ACTIVE` à `false`
//! - fn `pick_file` — ouvre le dialogue natif de sélection de fichier
//! - fn `pick_folder` — ouvre le dialogue natif de sélection de dossier
//! - fn `pick_save_file` — ouvre le dialogue natif de sauvegarde de fichier

/// Result of a file picker invocation.
pub enum PickResult {
    Selected(std::path::PathBuf),
    Cancelled,
    /// Never returned by rfd (compiled-in library); kept for exhaustive matches.
    #[allow(dead_code)]
    NotAvailable,
}

fn option_to_pick_result(opt: Option<std::path::PathBuf>) -> PickResult {
    opt.map_or(PickResult::Cancelled, PickResult::Selected)
}

/// Parse a glob pattern string like `"*.json *.jsonl"` into rfd extension list `["json", "jsonl"]`.
fn parse_ext(pattern: &str) -> Vec<String> {
    pattern
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_start_matches("*.").to_string())
        .collect()
}

// One dialog at a time — the OS only supports a single native file dialog per process.
static PICKER_ACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// RAII guard: resets PICKER_ACTIVE to false when dropped, even on panic or future cancellation.
struct PickerGuard;
impl Drop for PickerGuard {
    fn drop(&mut self) {
        PICKER_ACTIVE.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub async fn pick_file(filters: &[(&str, &str)]) -> PickResult {
    if PICKER_ACTIVE.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return PickResult::Cancelled;
    }
    let _guard = PickerGuard;
    let mut dialog = rfd::AsyncFileDialog::new().set_title("Select file");
    for (name, pattern) in filters {
        let exts = parse_ext(pattern);
        dialog = dialog.add_filter(*name, &exts);
    }
    let handle = dialog.pick_file().await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub async fn pick_folder() -> PickResult {
    if PICKER_ACTIVE.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return PickResult::Cancelled;
    }
    let _guard = PickerGuard;
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Select folder")
        .pick_folder()
        .await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub async fn pick_save_file(default_name: &str) -> PickResult {
    if PICKER_ACTIVE.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return PickResult::Cancelled;
    }
    let _guard = PickerGuard;
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Save schema as")
        .set_file_name(default_name)
        .add_filter("JSON", &["json"])
        .save_file()
        .await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn none_maps_to_cancelled() {
        assert!(matches!(option_to_pick_result(None), PickResult::Cancelled));
    }

    #[test]
    fn some_path_maps_to_selected() {
        let path = PathBuf::from("/tmp/test.json");
        assert!(matches!(
            option_to_pick_result(Some(path.clone())),
            PickResult::Selected(p) if p == path
        ));
    }

    #[test]
    fn parse_ext_single() {
        assert_eq!(parse_ext("*.json"), vec!["json"]);
    }

    #[test]
    fn parse_ext_multiple() {
        assert_eq!(parse_ext("*.json *.jsonl *.ndjson"), vec!["json", "jsonl", "ndjson"]);
    }

    #[test]
    fn parse_ext_no_star_prefix() {
        assert_eq!(parse_ext("json"), vec!["json"]);
    }

    #[test]
    fn parse_ext_empty_string() {
        let result = parse_ext("");
        assert!(result.is_empty());
    }

    #[test]
    fn picker_guard_resets_active_flag_on_drop() {
        use std::sync::atomic::Ordering;
        // Ensure clean state
        PICKER_ACTIVE.store(false, Ordering::SeqCst);
        // Simulate picker acquiring the flag
        PICKER_ACTIVE.store(true, Ordering::SeqCst);
        assert!(PICKER_ACTIVE.load(Ordering::SeqCst));
        // Drop guard must reset the flag
        drop(PickerGuard);
        assert!(!PICKER_ACTIVE.load(Ordering::SeqCst), "PickerGuard drop must reset PICKER_ACTIVE to false");
    }

    #[test]
    fn picker_active_swap_blocks_reentry() {
        use std::sync::atomic::Ordering;
        PICKER_ACTIVE.store(false, Ordering::SeqCst);
        // First acquire succeeds (returns false = was not active)
        let was_active = PICKER_ACTIVE.swap(true, Ordering::AcqRel);
        assert!(!was_active, "first acquire should succeed");
        // Second acquire fails (returns true = was already active)
        let was_active2 = PICKER_ACTIVE.swap(true, Ordering::AcqRel);
        assert!(was_active2, "second acquire must see flag already set");
        // Cleanup
        PICKER_ACTIVE.store(false, Ordering::SeqCst);
    }
}
