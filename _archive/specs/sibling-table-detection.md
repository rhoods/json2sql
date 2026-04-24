# Spec — Sibling Table Detection & KeyedPivot Strategy

> Issue de la session brainstorming 2026-03-20-4.
> Problème découvert lors du test OpenFoodFacts (66 Go, 4,4M produits) : 15 037 tables.

---

## Problème

Certains objets JSON utilisent des **valeurs comme clés** : IDs numériques, codes langue, slugs d'énumération. Chaque clé crée un `TableEntry` distinct dans Pass 1, ce qui génère des centaines ou milliers de tables "sœurs" de même schéma.

Exemples observés sur OpenFoodFacts :

```
products_images_1, _2, _3, _4...            (→ des milliers sur 4,4M produits)
products_images_front_fr, _front_en, _de...
products_ingredients_analysis_en_palm_oil, _en_vegan_status_unknown...
products_nova_groups_markers_2, _3, _4
products_packagings_materials_en_glass, _en_metal, _en_unknown...
```

---

## Analogie avec le problème des colonnes (session 3)

| Niveau colonnes | Niveau tables |
|---|---|
| `ingredients_text_en_ocr_1550522033` → nouvelle colonne | `products_images_1` → nouvelle table |
| Même structure, clé différente = bruit | Même schéma interne, clé différente = bruit |
| Fix : Pattern Folding → StructuredPivot | Fix : SiblingGroup → KeyedPivot |
| Résultat : 1 ligne par base, suffixe = colonne | Résultat : 1 ligne par instance, key = colonne |

**Cause racine (Five Whys) :**
> Le système sait plier des **colonnes** similaires (`StructuredPivot`) mais pas des **tables** similaires. Le concept de *schema sibling detection* n'existe pas au niveau table. C'est une asymétrie de design, pas un bug.

---

## Solution : SiblingGroup + KeyedPivot

### Principe

Après `finalize()`, une passe supplémentaire `finalize_siblings()` regroupe les tables enfants qui partagent :
- Le même parent
- Un préfixe commun dans le nom
- Un schéma de colonnes similaire (Jaccard ≥ seuil)

Ces tables "sœurs" sont fusionnées en **une seule table** avec une colonne `key` portant la clé originale.

```
Avant :                          Après :
products_images_1    (13 cols)
products_images_2    (13 cols)   products_images   (14 cols : key + union)
products_images_3    (13 cols)
products_images_fr   (15 cols)
```

### Key Shape Classifier

La forme des clés détermine le nom et le type de la colonne `key` :

| KeyShape | Exemples | Colonne générée |
|---|---|---|
| `Numeric` | `"1"`, `"42"`, `"100"` | `image_id INTEGER` |
| `IsoLang` | `"fr"`, `"en"`, `"de"`, `"ar"` | `lang_code TEXT` |
| `Slug` | `"en_glass"`, `"palm_oil"`, `"maybe_vegan"` | `key TEXT` |
| `Semantic` | `"selected"`, `"uploaded"`, `"front"` | → pas de fusion |

### Résultat attendu sur OpenFoodFacts

```
15 037 tables → ~200 tables
```
Les tables sœurs fusionnées restent des tables relationnelles normales avec `j2s_parent_id`.

---

## Arbre de décision — `finalize_siblings()`

```
POUR CHAQUE PARENT P ayant N tables enfants de même préfixe
│
├─ [1] User override (TOML/IHM) ?
│   └─ OUI → appliquer override, STOP
│
├─ [2] sibling_count < sibling_threshold (défaut: 3) ?
│   └─ OUI → garder séparées (peut être intentionnel), STOP
│
├─ [3] jaccard_avg < 0.3 ?
│   └─ OUI → schémas trop différents → garder séparées, STOP
│
├─ [4] key_shape == Numeric ?
│   └─ → KeyedPivot, colonne `{parent}_id INTEGER`, STOP
│
├─ [5] key_shape == IsoLang ?
│   └─ → KeyedPivot, colonne `lang_code TEXT`, STOP
│
├─ [6] key_shape == Slug ET jaccard_avg >= 0.7 ?
│   └─ → KeyedPivot, colonne `key TEXT`, STOP
│
├─ [7] key_shape == Slug ET jaccard_avg entre 0.3–0.7 ?
│   └─ → KeyedPivot avec colonnes NULL-able pour champs absents, STOP
│
└─ [8] key_shape == Semantic ?
    └─ → garder séparées (nutriscore_2021 vs nutriscore_2023 = distincts)
```

---

## Nouveaux types Rust

```rust
pub enum WideStrategy {
    Columns,
    Pivot,
    Jsonb,
    StructuredPivot(SuffixSchema),
    AutoSplit { stable_threshold: f64, rare_threshold: f64 },
    Ignore,
    KeyedPivot(SiblingSchema),   // ← NOUVEAU
}

pub struct SiblingSchema {
    pub key_col:    KeyColumn,         // nom + type de la colonne clé
    pub key_shape:  KeyShape,
    pub union_cols: Vec<ColumnSchema>, // union des colonnes de toutes les sœurs
}

pub enum KeyColumn {
    Numeric(String),  // ex: "image_id"
    Text(String),     // ex: "lang_code" ou "key"
}

pub enum KeyShape {
    Numeric,    // "1", "42", "100"
    IsoLang,    // "fr", "en", "de", "ar"
    Slug,       // "en_glass", "palm_oil", "maybe_vegan"
    Semantic,   // "selected", "uploaded" → pas de fusion
}
```

---

## Implémentation — Étapes

### 🔴 Priorité 1 — `finalize_siblings()` dans `registry.rs`

```rust
fn finalize_siblings(&mut self, sibling_threshold: usize) {
    // 1. Grouper les TableEntry par parent_path
    // 2. Pour chaque groupe de même parent :
    //    a. Détecter les sous-groupes de même préfixe (sans la clé finale)
    //    b. Pour chaque sous-groupe de taille >= sibling_threshold :
    //       - Calculer jaccard entre toutes les paires (colonnes)
    //       - Classifier key_shape depuis les clés observées
    //       - Appliquer l'arbre de décision
    //       - Si KeyedPivot : créer 1 TableSchema fusionné, supprimer les N individuels
}
```

Appelée dans `build_schema()` après `finalize()` existant.

### 🔴 Priorité 2 — `KeyedPivot` dans `WideStrategy` + log

```
Sibling tables detected: products_images (12 tables → 1)
  Keys: "1","2","3","front_fr","front_en","ingredients_fr"...
  Shape: NUMERIC+ISO_LANG → KeyedPivot (col: key TEXT)
  Jaccard avg: 0.82
  Override: [products_images] siblings = "keep"

Sibling tables detected: products_nova_groups_markers (3 → 1)
  Keys: "2","3","4" → NUMERIC → KeyedPivot (col: group_id INTEGER)
```

### 🟡 Priorité 3 — `insert_keyed_pivot_object()` dans `pass2/runner.rs`

Variante de `insert_structured_pivot_object()` :
- Au lieu de lire des suffixes dans une même clé → lire les champs d'un objet enfant
- La clé parente (ex: `"1"`, `"fr"`) devient la valeur de la colonne `key`
- Les champs internes (`sizes`, `uploaded_t`...) → colonnes typées

### 🟡 Priorité 4 — CLI + TOML

```
--sibling-threshold N     # défaut: 3
--sibling-jaccard F       # défaut: 0.7
```

```toml
# Opt-out de la fusion automatique :
[products_images]
siblings = "keep"

# Forcer la fusion même sous le seuil :
[products_nutriscore]
siblings = "merge"
```

### 🟢 Priorité 5 — IHM

Affichage des groupes sœurs détectés :
```
📁 products_images  (12 tables → 1 si fusion)
   Clés : "1", "2", "3", "front_fr", "front_en"... (+7)
   Jaccard : 0.82 | Shape : NUMERIC+ISO_LANG
   ✅ Suggestion : KeyedPivot (col: key TEXT)
   [Fusionner] [Garder séparées] [Détails]
```

---

## Vision Long Terme

`SiblingGroup` et `SuffixSchema` forment une paire symétrique :
- `SuffixSchema` → colonnes instances d'un pattern → `StructuredPivot`
- `SiblingGroup` → tables instances d'un pattern → `KeyedPivot`

L'IHM peut les présenter avec la même UI : un groupe détecté, un nombre d'éléments fusionnés, un choix fusion/keep, un export TOML.
