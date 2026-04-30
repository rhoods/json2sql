pub mod setup;
pub mod analysis;
pub mod strategy;
pub mod preview;
pub mod import;

use json2sql::schema::table_schema::WideStrategy;
use crate::theme;

// ---------------------------------------------------------------------------
// Shared zenity helpers (used by multiple screens)
// ---------------------------------------------------------------------------

/// Result of a zenity picker invocation.
pub enum PickResult {
    Selected(std::path::PathBuf),
    Cancelled,
    NotAvailable,
}

/// Run zenity with the given args.
pub async fn run_zenity(args: Vec<String>) -> PickResult {
    let output = tokio::task::spawn_blocking(move || {
        // Force X11 backend so zenity uses XWayland instead of connecting to the
        // Wayland compositor directly. Without this, zenity's Wayland connection
        // teardown disrupts the parent process's compositor session on exit.
        std::process::Command::new("zenity")
            .env("GDK_BACKEND", "x11")
            .args(&args)
            .output()
    })
    .await;

    let output = match output {
        Ok(Ok(o)) => o,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => return PickResult::NotAvailable,
        _ => return PickResult::Cancelled,
    };

    if output.status.success() {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            PickResult::Cancelled
        } else {
            PickResult::Selected(std::path::PathBuf::from(path))
        }
    } else {
        PickResult::Cancelled
    }
}

pub async fn pick_file_zenity(filters: &[(&str, &str)]) -> PickResult {
    let mut args = vec!["--file-selection".to_string(), "--title=Select file".to_string()];
    for (_, glob) in filters {
        args.push(format!("--file-filter={}", glob));
    }
    run_zenity(args).await
}

pub async fn pick_folder_zenity() -> PickResult {
    run_zenity(vec![
        "--file-selection".to_string(),
        "--directory".to_string(),
        "--title=Select folder".to_string(),
    ])
    .await
}

pub async fn pick_save_file_zenity(default_name: &str) -> PickResult {
    run_zenity(vec![
        "--file-selection".to_string(),
        "--save".to_string(),
        "--confirm-overwrite".to_string(),
        format!("--title=Save schema as"),
        format!("--filename={}", default_name),
        "--file-filter=*.json".to_string(),
    ])
    .await
}

pub fn strategy_label(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns                     => "DEFAULT",
        WideStrategy::Pivot                       => "PIVOT",
        WideStrategy::Jsonb                       => "JSONB SÉP.",
        WideStrategy::JsonbFlatten                => "JSONB INLINE",
        WideStrategy::StructuredPivot(_)          => "STRUCT PIVOT",
        WideStrategy::KeyedPivot(_)               => "KEYED PIVOT",
        WideStrategy::MultiKeyedPivot(_)          => "MULTI PIVOT",
        WideStrategy::AutoSplit { .. }            => "AUTO SPLIT",
        WideStrategy::Ignore                      => "SKIP",
        WideStrategy::NormalizeDynamicKeys { .. } => "NORMALIZE",
        WideStrategy::Flatten { .. }              => "FLATTEN",
    }
}

pub fn strategy_color(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns                     => theme::BADGE_DEFAULT,
        WideStrategy::Pivot                       => theme::BADGE_NORMALIZE,
        WideStrategy::Jsonb                       => theme::BADGE_JSONB,
        WideStrategy::JsonbFlatten                => theme::BADGE_JSONB_INLINE,
        WideStrategy::StructuredPivot(_)          => theme::BADGE_FLATTEN,
        WideStrategy::KeyedPivot(_)               => theme::BADGE_FLATTEN,
        WideStrategy::MultiKeyedPivot(_)          => theme::BADGE_FLATTEN,
        WideStrategy::AutoSplit { .. }            => theme::BADGE_NORMALIZE,
        WideStrategy::Ignore                      => theme::BADGE_SKIP,
        WideStrategy::NormalizeDynamicKeys { .. } => theme::BADGE_NORMALIZE,
        WideStrategy::Flatten { .. }              => theme::BADGE_FLATTEN,
    }
}
