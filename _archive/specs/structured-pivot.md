# Spec : Stratégie `structured_pivot`

## Problème

Certains objets JSON encodent une table relationnelle sous forme d'un dictionnaire plat où chaque entité est représentée par un groupe de clés partageant un préfixe commun et un ensemble de suffixes fixes.

Exemple OpenFoodFacts `nutriments` :
```json
{
  "calcium": 0.12,
  "calcium_100g": 0.12,
  "calcium_serving": 0.03,
  "calcium_unit": "g",
  "calcium_label": "Calcium",
  "calcium_value": 0.12,
  "iron": 0.002,
  "iron_100g": 0.002,
  "iron_unit": "mg",
  ...
}
```

La forme relationnelle naturelle est :
```sql
nutriments(j2s_id, j2s_parent_id, name TEXT, value FLOAT,
           per_100g FLOAT, per_serving FLOAT, unit VARCHAR, label TEXT, norm_value FLOAT)
```
Une ligne par nutriment, les suffixes deviennent des colonnes.

Le pivot actuel (`key TEXT, value <type>`) ne capture pas cette structure —
il crée une ligne par clé brute, perdant le regroupement sémantique.

---

## Nouvelle stratégie : `structured_pivot`

Quatrième valeur possible pour `WideStrategy` :

| Stratégie | Table SQL produite |
|---|---|
| `columns` | une colonne par clé JSON |
| `pivot` | `(key TEXT, value <type>)` — une ligne par clé |
| `jsonb` | `(data JSONB)` — objet entier sérialisé |
| `structured_pivot` | `(name TEXT, col1, col2, ...)` — une ligne par entité, suffixes comme colonnes |

---

## Algorithme de détection automatique des suffixes

### Objectif

Étant donné un ensemble de clés `K`, trouver la décomposition `key = base + separator + suffix`
qui maximise la cohérence du schéma, sans connaissance a priori des suffixes.

### Phase 1 : Extraction des suffixes candidats

Pour chaque clé, extraire tous les "segments finaux" possibles (splits sur `_`) :

```
"calcium_100g"      → candidats suffixes : ["_100g", "_g"]  (depuis la droite)
"calcium_100g_unit" → candidats suffixes : ["_unit", "_100g_unit"]
"calcium"           → pas de suffixe (base pure)
```

Compter la fréquence de chaque suffixe candidat (nombre de clés distinctes qui se terminent par ce suffixe).

### Phase 2 : Score de couverture par décomposition

Pour chaque suffixe candidat `s` fréquent (>= seuil de couverture) :

1. Identifier toutes les clés qui se terminent par `s` → extraire leur `base` = `key[:-len(s)]`
2. Calculer le "taux de couverture" : pour chaque base identifiée, quel % des suffixes candidats est présent ?
3. Score global = couverture moyenne × nombre de bases couvertes

Retenir le jeu de suffixes `S` qui maximise ce score.

### Phase 3 : Signal de type (discriminant secondaire)

Les colonnes du même suffixe ont des types homogènes entre bases :
- `_100g` → toujours DOUBLE PRECISION
- `_unit` → toujours VARCHAR
- `_label` → toujours TEXT

Utiliser la cohérence de type par suffixe comme signal de validation :
un suffixe dont les colonnes ont des types incohérents est pénalisé dans le score.

### Phase 4 : Clés "base pure" (sans suffixe)

Les clés qui correspondent exactement à une base identifiée (ex: `calcium` quand `calcium_100g` existe)
sont mappées sur la colonne `value` dans la table structurée.

Les clés qui ne correspondent à aucune base/suffixe connue restent dans une colonne `_other JSONB` ou sont ignorées avec warning.

---

## Paramètres configurables

```toml
[nutriments]
strategy = "structured_pivot"

# Seuil : un suffixe est retenu s'il couvre >= X% des bases (défaut : 0.3 = 30%)
# suffix_coverage_threshold = 0.3

# Override manuel des suffixes (désactive l'auto-détection)
# suffix_columns = ["_100g", "_serving", "_unit", "_label", "_value"]

# Séparateur (défaut : "_")
# separator = "_"
```

### CLI

```bash
# Détection et rapport des suffixes sans import
json2sql --input data.jsonl --dry-run --discover-suffixes

# Résultat écrit dans le schema-config pour validation
json2sql --input data.jsonl --dry-run --discover-suffixes --schema-config schema.toml
```

---

## Table SQL produite

Pour `nutriments` avec suffixes auto-détectés `{(none), _100g, _serving, _unit, _label, _value}` :

```sql
CREATE TABLE ingredient_nutriments (
  j2s_id        UUID NOT NULL,
  j2s_parent_id UUID NOT NULL,
  name          TEXT NOT NULL,       -- base : "calcium", "iron", "vitamin_c"
  value         DOUBLE PRECISION,    -- clé base sans suffixe (peut être NULL)
  per_100g      DOUBLE PRECISION,    -- suffixe _100g
  per_serving   DOUBLE PRECISION,    -- suffixe _serving
  unit          VARCHAR(16),         -- suffixe _unit
  label         TEXT,                -- suffixe _label
  norm_value    DOUBLE PRECISION,    -- suffixe _value
  CONSTRAINT pk_ingredient_nutriments PRIMARY KEY (j2s_id)
);
ALTER TABLE ingredient_nutriments
  ADD CONSTRAINT fk_ingredient_nutriments_parent
  FOREIGN KEY (j2s_parent_id) REFERENCES ingredient (j2s_id);
```

---

## Impact sur l'architecture existante

### Nouveaux composants

| Composant | Rôle |
|---|---|
| `schema/suffix_detector.rs` | Algorithme de détection des suffixes (phases 1-4) |
| `SuffixSchema` struct | Représente le jeu de suffixes détecté : `{separator, suffixes: Vec<(suffix_str, col_name, PgType)>}` |

### Composants modifiés

| Composant | Changement |
|---|---|
| `WideStrategy` | Nouvelle variante `StructuredPivot(SuffixSchema)` |
| `TableSchema` | Supporte le nouveau DDL |
| `schema/registry.rs` | Appel à `suffix_detector` dans `finalize()` |
| `schema/config.rs` | Lecture de `suffix_columns` + `suffix_coverage_threshold` |
| `pass2/runner.rs` | Nouveau `insert_structured_pivot_object()` |
| `db/ddl.rs` | DDL pour StructuredPivot |

### Nouvelle fonction d'insertion (passe 2)

Pour chaque objet wide :
1. Grouper les clés par base détectée
2. Pour chaque groupe `(base, {suffix: value})` : construire une ligne `(j2s_id, j2s_parent_id, name=base, per_100g=..., unit=..., ...)`
3. Les colonnes absentes pour un groupe → NULL

---

## Mode `--discover-suffixes`

Nouveau flag qui, combiné à `--dry-run`, écrit dans le TOML de schema-config les suffixes détectés :

```toml
# Généré automatiquement par --discover-suffixes
# Vérifier et ajuster avant l'import

[nutriments]
strategy = "structured_pivot"
suffix_coverage_threshold = 0.3
# Suffixes détectés (couverture entre parenthèses) :
suffix_columns = [
  "_100g",      # couverture 94%
  "_unit",      # couverture 87%
  "_label",     # couverture 72%
  "_value",     # couverture 68%
  "_serving",   # couverture 41%
]
# Suffixes ignorés (couverture trop faible) :
# "_modifier"   # couverture 3%
# "_debug"      # couverture 1%
```

L'utilisateur peut ajuster le seuil ou la liste avant de relancer sans `--dry-run`.

---

## Cas limites

| Cas | Comportement |
|---|---|
| Base sans aucun suffixe connu | Ligne avec `name=base, value=v`, autres colonnes NULL |
| Clé orpheline (ni base ni suffixe reconnu) | Warning + colonne `_unmatched JSONB` optionnelle |
| Deux bases produisant le même nom sanitisé | Résolu par hash suffix (voir spec collision-hash-suffix) |
| Suffixe lui-même contenant `_` | Le score de couverture favorise le split le plus "stable" |
| Aucun groupe détecté (coverage trop faible) | Fallback sur `pivot` classique avec warning |

---

## Priorité et effort

- **Effort :** Élevé (~300-500 lignes, nouveau module `suffix_detector`)
- **Impact :** Fort — transforme des tables de milliers de colonnes en tables relationnelles propres
- **Dépendance :** Après implémentation de `collision-hash-suffix` (les bases détectées peuvent avoir des noms en collision)
- **Risque :** L'algorithme de détection peut produire de mauvais splits sur des données atypiques → le mode `--discover-suffixes` est le filet de sécurité
