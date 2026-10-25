//! Thematic split of wide tables (#78): proposing 1:1 groups of columns.
//!
//! Sous-modules :
//! - [`auto`] — groupes confirmés par les noms, appliqués comme une stratégie (`--group-columns`, bascule de l'IHM) (#91)
//! - [`names`] — normalisation des noms de colonnes en radicaux (T1a)
//! - [`embedding`] — embeddings statiques `Model2Vec` des noms de colonnes : lecteur safetensors et `WordPiece` (T2)
//! - [`lexical`] — regroupement lexical des radicaux : trigrammes TF-IDF, liaison moyenne (T1b)
//! - [`numeric`] — signaux numériques : corrélation de rang et formes précises (année, epoch, degrés) (T1d)
//! - [`placement`] — pour chaque colonne laissée dans la table, les deux groupes les plus proches de son nom en embedding (T2)
//! - [`presence`] — co-présence des colonnes dans un échantillon de lignes : φ ajusté par richesse de fiche (T1c)
//! - [`proposal`] — assemblage d'une proposition : règle des deux signaux, noms de groupes, familles signalées (T1e)
//! - [`render`] — sorties d'une proposition : TOML `column_groups` et rapport Markdown (T1e)
//! - [`sample`] — échantillon à pas régulier des lignes d'une table, relu dans le fichier d'entrée (T1c)
//! - [`shared`] — valeurs partagées entre colonnes texte : contenance pondérée par la rareté (T1d)
//! - [`suggest`] — l'exécution de `--suggest-groups` : choix d'une table, échantillon, proposition, sorties (T1e)
//! - [`values`] — valeurs échantillonnées des colonnes (vocabulaire, nombres), pour les signaux de données (T1d)

pub mod auto;
pub mod embedding;
pub mod lexical;
pub mod names;
pub mod numeric;
pub mod placement;
pub mod presence;
pub mod proposal;
pub mod render;
pub mod sample;
pub mod shared;
pub mod suggest;
pub mod values;
