//! Utilitaires partagés entre écrans : hook de timer, construction du view-model de la liste
//! de tables (badges/arbre/filtre), sélecteurs de fichiers natifs, labels de stratégie,
//! composant barre de progression, application des overrides utilisateur.
//!
//! Chaque domaine vit dans son propre fichier sous `common/`, réexporté ici pour que
//! `crate::screens::X` continue de résoudre sans changement côté écrans appelants :
//!
//! - Hook : voir `common/timer.rs`
//! - View-model liste de tables : voir `common/table_rows.rs`
//! - Sélecteurs de fichiers natifs : voir `common/file_picker.rs`
//! - Stratégies : voir `common/strategy_badges.rs`
//! - Progress : voir `common/progress.rs`
//! - Overrides : voir `common/overrides.rs`
pub mod analysis;
pub mod import;
pub mod preview;
pub mod resume;
pub mod setup;
pub mod strategy;
pub mod table_list;

mod common;
pub use common::{
    build_effective_schemas, build_table_rows, display_schemas, group_preview, pick_file,
    pick_folder, pick_save_file, progress_pct, skip_cascade_names, splittable_tables,
    strategy_badge, strategy_label, use_elapsed_timer, user_override_badge, GroupPreview,
    PickResult, PreviewGroup, ProgressBar, TableRowViewModel, TableRowsCtx,
};
