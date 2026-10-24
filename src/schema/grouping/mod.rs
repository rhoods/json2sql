//! Thematic split of wide tables (#78): proposing 1:1 groups of columns.
//!
//! Sous-modules :
//! - [`names`] — normalisation des noms de colonnes en radicaux (T1a)
//! - [`lexical`] — regroupement lexical des radicaux : trigrammes TF-IDF, liaison moyenne (T1b)
//! - [`presence`] — co-présence des colonnes dans un échantillon de lignes : φ ajusté par richesse de fiche (T1c)
//! - [`sample`] — échantillon à pas régulier des lignes d'une table, relu dans le fichier d'entrée (T1c)

pub mod lexical;
pub mod names;
pub mod presence;
pub mod sample;
