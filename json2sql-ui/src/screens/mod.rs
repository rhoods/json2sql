//! Utilitaires partagés entre écrans : hook de timer, construction du view-model de la liste
//! de tables (badges/arbre/filtre), sélecteurs de fichiers natifs, labels de stratégie,
//! composant barre de progression, application des overrides utilisateur.
//!
//! Chaque domaine vit dans son propre fichier, réexporté à plat ici pour que
//! `crate::screens::X` continue de résoudre sans changement côté écrans appelants :
//!
//! - Hook : voir `timer.rs`
//! - View-model liste de tables : voir `table_rows.rs`
//! - Sélecteurs de fichiers natifs : voir `file_picker.rs`
//! - Stratégies : voir `strategy_badges.rs`
//! - Progress : voir `progress.rs`
//! - Overrides : voir `overrides.rs`
pub mod setup;
pub mod analysis;
pub mod strategy;
pub mod preview;
pub mod import;
pub mod resume;
pub mod table_list;

mod common;
pub use common::{
    use_elapsed_timer,
    PickResult, pick_file, pick_folder, pick_save_file,
    strategy_label, strategy_badge, user_override_badge,
    progress_pct, ProgressBar,
    build_effective_schemas,
    TableRowViewModel, TableRowsCtx, build_table_rows,
};
