# UX — Améliorations panneau de sélection des tables
> À faire **après** le refacto SOLID de l'IHM.

---

## Contexte
Écrans Strategy (3) et Preview (4). L'objectif final est de permettre à l'utilisateur
de sélectionner des tables sœurs facilement pour y appliquer une stratégie de fusion.

---

## ~~T1~~ ✅ livré 2026-05-25

- Multi-select implicite (`selected_indices.len() > 1`)
- **Ctrl+clic / ⌘+clic** : toggle dans la sélection (min 1 table)
- **Shift+clic** : plage sur lignes visibles uniquement (ancre locale `Signal<usize>`)
- **"⊕ children"** : bouton au survol d'une table parente → enfants directs visibles
- **"⊕ all"** : bouton dans la barre de filtre → toutes lignes visibles
- `TableListPanel` refactorisé en composant partagé
- Lasso souris : **nice-to-have**, repoussé (complexité élevée dans `<table>` HTML)

## ~~T2+T3~~ ✅ livré 2026-05-25

Jaccard score coloré (barre rouge/orange/vert + ratio common/union) + "Merge as siblings"
dans le panneau droit Strategy multi-select. Backend : `build_keyed_pivot_from_siblings`
dans `cascading.rs`. State : `apply_sibling_merge` + `compute_jaccard_display`.
