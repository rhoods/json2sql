# UX — Améliorations panneau de sélection des tables
> À faire **après** le refacto SOLID de l'IHM.

---

## Contexte
Écrans Strategy (3) et Preview (4). L'objectif final est de permettre à l'utilisateur
de sélectionner des tables sœurs facilement pour y appliquer une stratégie de fusion.

---

## T1 — `TableSelectionPanel` : nouveau composant (remplace inline strategy.rs)

- Indentation correcte : `depth * 14px` padding-left + connecteurs `└─` / `├─` selon `is_last_child`
- Suppression du toggle "multi select" : mode multi implicite dès que `selected_indices.len() > 1`
- **Shift+click** : sélection de plage entre la dernière sélection et le click
- **"Select children"** : bouton au survol d'une ligne parente → sélectionne tous les enfants directs (`parent_table == row.name`)
- **"Select all visible"** : bouton dans le header → sélectionne toutes les lignes non filtrées
- Lasso souris : **nice-to-have**, à reconsidérer après les autres (complexité élevée dans `<table>` HTML)

## T2 — Score Jaccard affiché dans le panneau droit

- Exposer `pairwise_jaccard_min` en `pub` dans `json2sql/src/schema/cascading.rs`
  (actuellement `pub(crate)`, inaccessible depuis le crate UI)
- Quand `selection_count >= 2` : calculer et afficher le score Jaccard entre les tables sélectionnées
- Afficher un avertissement si les tables n'ont pas le même `parent_table`
- Sert de signal visuel avant d'appliquer une fusion manuelle

## T3 — Fusion manuelle de tables sœurs (feature backend + UI)

- **Prérequis T2**
- `WideStrategy::MultiKeyedPivot(Vec<SiblingGroup>)` nécessite des champs auto-calculés
  (`pivot_table`, `sibling_schema`, `absorbed_names`) — il faudra un chemin de construction manuel
- Design à faire : comment l'utilisateur déclenche la fusion et comment le système calcule les `SiblingGroup`
- Note dans `strategy_configurator.rs` ligne 139 : *"non disponible via l'IHM"* → à retirer quand implémenté
