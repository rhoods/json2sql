//! Détection des tables sœurs (*siblings*) et fusion (*collapse*) en tables canoniques.
//!
//! **Sibling** : tables issues du même array JSON (`items_0`, `items_1`…), structurellement
//! similaires et destinées à être fusionnées. La similarité est mesurée par l'indice de
//! Jaccard sur les colonnes.
//!
//! **Collapse** : opération qui fusionne un groupe de siblings en une table canonique.
//! Selon la nature des clés, le collapse produit soit un *pivot* (clé numérique ou slug),
//! soit un *keyed pivot* (clé sémantique comme un code langue).
//!
//! Sous-modules :
//! - [`scoring`]   — Jaccard, clustering glouton (fonctions pures sur `&[TableSchema]`)
//! - [`detection`] — BFS cascade, `finalize_cascading`
//! - [`merge`]     — API publique pour la fusion manuelle depuis l'IHM

pub mod scoring;
pub mod detection;
pub mod merge;

pub use detection::finalize_cascading;
