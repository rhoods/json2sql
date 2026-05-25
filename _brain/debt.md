# Dette Technique — json2sql

## À adresser avant release
<!-- Ex: "Authentification non implémentée — accès libre aux routes" -->

## Améliorations futures
- IHM Leptos bancale — à consolider (visualisation du schéma)
- Log des flush périodiques (`flush tablename (N rows)`)

## Backlog sibling detection — options non implémentées (2026-05-25)

### Option A — Assouplir le child-compat gate
**Cas pertinent** : des frères ont un Jaccard propre élevé (≥ 0.9) mais leur child-compat
échoue parce que certains n'ont des sous-enfants que dans un sous-ensemble de produits.
Exemple concret : `images_selected_af_generation` (6 cols) vs `images_selected_nutrition_aa_generation`
(1 col `angle` seulement) bloque la fusion de leurs parents malgré un Jaccard parent = 100%.
Fix : bypass le child-compat quand Jaccard propre ≥ seuil_haut (ex. 0.9).
Code : `src/schema/cascading.rs`, fonction `run_sibling_wave`, après le check Jaccard (ligne ~496).

### Option C — Clustering glouton dans `process_co_sibling_group`
**Cas pertinent** : des co-frères (enfants d'un groupe de frères absorbé) ont des schémas
hétérogènes entre eux. Exemple concret : après absorption de `{front, nutrition, packaging}`
dans `images_selected`, les co-frères `generation` ont 2 schémas distincts (1 col vs 6 cols).
Actuellement ils sont re-parentés individuellement → N tables orphelines.
Fix : appliquer le clustering glouton (même algo que T2 de la session 2026-05-25) dans
`process_co_sibling_group` quand Jaccard < min_jaccard, pour créer N pivots-clusters
au lieu de N tables individuelles.
Code : `src/schema/cascading.rs`, fonction `process_co_sibling_group`, branche `else` (ligne ~851).
