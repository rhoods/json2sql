//! Utilitaires partagés entre écrans : hook de timer, construction du view-model de la liste
//! de tables (badges/arbre/filtre), sélecteurs de fichiers natifs, labels de stratégie,
//! composant barre de progression, application des overrides utilisateur.
//!
//! Chaque domaine vit dans son propre fichier, réexporté ici pour que
//! `crate::screens::X` continue de résoudre sans changement côté écrans appelants :
//!
//! - Hook : voir `timer.rs`
//! - View-model liste de tables : voir `table_rows.rs`
//! - Sélecteurs de fichiers natifs : voir `file_picker.rs`
//! - Stratégies : voir `strategy_badges.rs`
//! - Progress : voir `progress.rs`
//! - Overrides : voir `overrides.rs`

mod timer;
pub use timer::use_elapsed_timer;

mod file_picker;
pub use file_picker::{PickResult, pick_file, pick_folder, pick_save_file};

mod strategy_badges;
pub use strategy_badges::{strategy_label, strategy_badge, user_override_badge};

mod progress;
pub use progress::{progress_pct, ProgressBar};

mod overrides;
pub use overrides::build_effective_schemas;

mod table_rows;
pub use table_rows::{TableRowViewModel, TableRowsCtx, build_table_rows};
