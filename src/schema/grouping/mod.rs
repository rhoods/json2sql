//! Thematic split of wide tables (#78): proposing 1:1 groups of columns.
//!
//! Sous-modules :
//! - [`names`] — normalisation des noms de colonnes en radicaux (T1a)
//! - [`lexical`] — regroupement lexical des radicaux : trigrammes TF-IDF, liaison moyenne (T1b)

pub mod lexical;
pub mod names;
