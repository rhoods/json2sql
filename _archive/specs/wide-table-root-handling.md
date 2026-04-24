# Spec — Wide Table Root Handling & AutoSplit Strategy

> Issu de la session brainstorming 2026-03-20-3.
> Problème découvert lors du test OpenFoodFacts (66 Go, 4,4M produits).

---

## Problème

Le dry-run sur OpenFoodFacts révèle que la table racine `products` accumule **189 881 clés uniques** — non pas parce que chaque produit est réellement large, mais à cause de trois sources de bruit :

1. **Clés OCR avec timestamp** (source dominante) :
   `ingredients_text_en_ocr_1550522033`, `ingredients_text_en_ocr_1545732141`...
   → Chaque scan OCR crée une clé unique. Sur 4,4M produits = des dizaines de milliers de clés singleton.

2. **Variantes linguistiques** : `generic_name_fr`, `generic_name_en`...

3. **Champs one-off contributeurs** : 76% des clés n'apparaissent qu'une seule fois.

**Conséquence du design actuel :**
- 189k colonnes > seuil 50 → `StructuredPivot` appliqué à `products`
- `is_wide()` = true → toutes les tables enfants (`products_nutriments`, etc.) supprimées
- Schema final : **1 table, 4 colonnes** — structure relationnelle détruite

---

## Cause Racine

`StructuredPivot` et `Pivot` sont conçus pour les **tables enfants** (ils requièrent un `j2s_parent_id`). Aucune garde n'empêche leur application aux tables racines. Une table racine en StructuredPivot n'a pas de `parent_id` à fournir à ses enfants → lien relationnel brisé.

---

## Solution : AutoSplit + Anchor UUID

### Principe

Au lieu d'une stratégie monolithique par table, les tables racines larges sont **splittées automatiquement** en deux tables complémentaires basées sur la fréquence des clés :

```
products          → colonnes stables (fréquence > stable_threshold)
                    1 ligne par produit, porte l'anchor UUID
products_wide     → colonnes medium/rares (Pivot EAV)
                    N lignes par produit, j2s_parent_id = anchor

Enfants existants préservés :
products_nutriments → StructuredPivot, j2s_parent_id = anchor ✅
```

### Anchor UUID

Un UUID stable est généré **par enregistrement source** (par produit). Ce UUID sert de `j2s_parent_id` pour :
- Les lignes de `products_wide`
- Toutes les tables enfants habituelles (`products_nutriments`, `products_ingredients`, etc.)

### Seuils de fréquence

Deux seuils remplacent le `--wide-column-threshold` absolu :

| Seuil | Défaut | Signification |
|---|---|---|
| `stable_threshold` | 0.10 | Clé présente dans > 10% des lignes → colonne stable dans `{table}` |
| `rare_threshold` | 0.001 | Clé présente dans < 0.1% des lignes → ignorée ou JSONB catch-all |

Les clés entre les deux seuils → `{table}_wide` (Pivot EAV).

Signal de détection amélioré :
```
ratio_stable = stable_keys / total_keys
Si ratio_stable < 0.01 → table wide détectée
```
OpenFoodFacts : 282 stables / 189k totales = **0.15%** → wide évident.

---

## Arbre de Décision — finalize()

```
POUR CHAQUE TABLE T
│
├─ [1] User override (config/IHM) → appliquer, STOP
│
├─ [2] total_keys <= wide_threshold → Columns, STOP
│
└─ [3] total_keys > wide_threshold
    │
    ├─ [3a] ratio_stable > 0.5 → Columns + warning, STOP
    │
    ├─ [3b] suffix_detected AND NOT is_root → StructuredPivot, STOP
    │
    ├─ [3c] is_root AND has_children
    │   └─ → AutoSplit {stable_threshold, rare_threshold}
    │        ├─ stable_keys  → table "{name}" (Columns)
    │        ├─ medium_keys  → table "{name}_wide" (Pivot)
    │        └─ rare_keys    → Ignore ou JSONB catch-all
    │
    ├─ [3d] NOT is_root AND NOT has_children
    │   └─ → homogène → Pivot | hétérogène → Jsonb
    │
    └─ [3e] fallback → Jsonb
```

---

## Nouveaux variants WideStrategy

```rust
pub enum WideStrategy {
    Columns,
    Pivot,
    Jsonb,
    StructuredPivot(SuffixSchema),

    /// Nouveau : split automatique en {table} + {table}_wide
    AutoSplit {
        stable_threshold: f64,
        rare_threshold: f64,
    },

    /// Nouveau : clé complètement écartée du schema et des données
    Ignore,
}
```

---

## Implémentation — Étapes

### 🔴 Priorité 1 — Garde immédiate (3 lignes)

Dans `finalize()`, avant d'appliquer une WideStrategy à une table racine avec enfants :

```rust
if schema.is_root() && schema_has_children && matches!(strategy, WideStrategy::StructuredPivot(_) | WideStrategy::Pivot) {
    // Fallback vers AutoSplit ou Jsonb
    strategy = WideStrategy::AutoSplit { stable_threshold: 0.10, rare_threshold: 0.001 };
}
```

### 🔴 Priorité 2 — Ratio stable/total

Dans `finalize()`, calculer `ratio_stable` à partir de `TypeTracker.total_count` et `registry.total_rows`. Utiliser ce ratio comme critère principal de détection wide.

### 🟡 Priorité 3 — WideStrategy::AutoSplit

1. Ajouter le variant `AutoSplit` à `WideStrategy`
2. Dans `finalize()` : implémenter le split → créer `{name}_wide` comme nouvelle `TableSchema` avec les colonnes medium, liée à `{name}` via `j2s_parent_id`
3. Dans `pass2/runner.rs` : ajouter `insert_autosplit_object()` — génère l'anchor UUID, écrit la ligne stable dans `{name}`, écrit les lignes pivot dans `{name}_wide`, puis propage l'anchor UUID aux enfants

### 🟡 Priorité 4 — CLI

```
--stable-threshold FLOAT   # défaut: 0.10
--rare-threshold FLOAT     # défaut: 0.001
```

### 🟢 Priorité 5 — WideStrategy::Ignore

Filtrer les clés sous `rare_threshold` **avant** `finalize()`, pendant la construction des `TableEntry`. Ces clés ne créent ni colonne, ni table enfant, ni JSONB.

---

## Vision Long Terme — IHM

Une interface web locale (full Rust : Axum + Leptos ou Egui, compilé en WASM) permettant :

- Visualisation de l'arborescence des tables après Pass 1
- Colonnes groupées par pattern détecté avec count + exemples de valeurs
- Vue dépliable en lazy scroll par groupe
- Approbation/rejet des suggestions de stratégie par table et par groupe
- Export TOML auto-généré pour reproductibilité (CI/CD)
- Réutilisation automatique de la config si le schéma est similaire (hash-match)

Workflow cible :
```
json2sql -i data.jsonl --inspect
→ Pass 1
→ "Schema inspector at http://localhost:7823"
→ Validation IHM
→ "Launch Pass 2" depuis l'IHM
→ Export .j2s_config.toml
```
