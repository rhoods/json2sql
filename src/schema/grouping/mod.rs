//! Thematic split of wide tables (#78): proposing 1:1 groups of columns.
//!
//! Sous-modules :
//! - [`names`] — normalisation des noms de colonnes en radicaux (T1a)
//! - [`lexical`] — regroupement lexical des radicaux : trigrammes TF-IDF, liaison moyenne (T1b)
//! - [`numeric`] — signaux numériques : corrélation de rang et formes précises (année, epoch, degrés) (T1d)
//! - [`presence`] — co-présence des colonnes dans un échantillon de lignes : φ ajusté par richesse de fiche (T1c)
//! - [`sample`] — échantillon à pas régulier des lignes d'une table, relu dans le fichier d'entrée (T1c)
//! - [`shared`] — valeurs partagées entre colonnes texte : contenance pondérée par la rareté (T1d)
//! - [`values`] — valeurs échantillonnées des colonnes (vocabulaire, nombres), pour les signaux de données (T1d)

pub mod lexical;
pub mod names;
pub mod numeric;
pub mod presence;
pub mod sample;
pub mod shared;
pub mod values;
