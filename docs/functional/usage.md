# json2sql — Utilisation

## Installation

```bash
cargo build --release
# Binaire : ./target/release/json2sql
```

Variable d'environnement : `DATABASE_URL` est lue automatiquement si `--db-url` est absent.

---

## Exemples rapides

```bash
# Import basique
json2sql --input data.json --db-url postgres://user:pass@localhost/mydb

# Depuis stdin
cat data.jsonl | json2sql --db-url $DATABASE_URL --table my_root

# Afficher le DDL sans toucher la base
json2sql --input data.json --dry-run --schema staging

# Rapport de types par colonne (validation avant import)
json2sql --input data.json --dry-run --schema-report
json2sql --input data.json --dry-run --schema-report-output stats.txt

# Stocker les tableaux scalaires en colonnes PostgreSQL natives
json2sql --input data.json --db-url $DATABASE_URL --array-as-pg-array

# Import complet avec options
json2sql \
  --input data.jsonl \
  --db-url $DATABASE_URL \
  --schema staging \
  --table my_root \
  --drop-existing \
  --transaction \
  --anomaly-output anomalies.json \
  --anomaly-format json \
  --max-anomaly-rate 0.01 \
  --text-threshold 512
```

---

## Référence des paramètres CLI

### Paramètres principaux

| Paramètre | Défaut | Description |
|---|---|---|
| `--input` | stdin | Fichier JSON/NDJSON à importer |
| `--db-url` | `DATABASE_URL` | URL de connexion PostgreSQL |
| `--schema` | `public` | Schéma PostgreSQL cible |
| `--table` | dérivé du nom de fichier | Nom de la table racine |
| `--dry-run` | false | Affiche le DDL sans se connecter |
| `--drop-existing` | false | Supprime les tables existantes avant création |
| `--schema-config` | aucun | Fichier TOML de surcharges manuelles |

### Paramètres d'inférence

| Paramètre | Défaut | Description |
|---|---|---|
| `--text-threshold` | 256 | Longueur max avant TEXT (sinon VARCHAR) |
| `--wide-column-threshold` | 1000 | Seuil de détection des tables larges |
| `--stable-threshold` | 0.10 | Fréquence min pour colonne stable (AutoSplit) |
| `--rare-threshold` | 0.001 | Fréquence max avant suppression (AutoSplit) |
| `--sibling-threshold` | 3 | Nb min de tables sœurs pour fusion |
| `--sibling-jaccard` | 0.5 | Similarité min des colonnes sœurs |
| `--array-as-pg-array` | false | Tableaux scalaires → colonne `TEXT[]` |
| `--depth-limit` | aucun | Avertissement si imbrication dépasse N niveaux |

### Paramètres de chargement

| Paramètre | Défaut | Description |
|---|---|---|
| `--batch-size` | 100 000 | Flush vers PostgreSQL toutes les N lignes par table |
| `--parallel` | 1 | Connexions PostgreSQL parallèles |
| `--transaction` | false | Enveloppe tout dans une transaction |

### Paramètres d'anomalies

| Paramètre | Défaut | Description |
|---|---|---|
| `--anomaly-dir` | aucun | Dossier pour les fichiers NDJSON d'anomalies par table |
| `--anomaly-output` | stdout | Fichier du rapport de synthèse des anomalies |
| `--anomaly-format` | json | Format du rapport (`json` ou `csv`) |
| `--max-anomaly-rate` | aucun | Taux max acceptable (0.0–1.0) ; abort si dépassé |

### Paramètres de rapport de schéma

| Paramètre | Défaut | Description |
|---|---|---|
| `--schema-report` | false | Affiche le rapport de colonnes sur stderr |
| `--schema-report-output` | stderr | Fichier pour le rapport de colonnes |

---

## Config TOML (`--schema-config`)

Le fichier TOML permet de forcer des stratégies de stockage ou des types de colonnes que l'inférence automatique ne peut pas deviner.

### Format général

```toml
[nom_table]
strategy = "jsonb"          # jsonb | pivot | structured_pivot | columns | ignore

[nom_table_nutrients]
strategy = "structured_pivot"
suffix_columns = ["_100g", "_unit", "_serving"]

[nom_table.columns]
ma_colonne = "INTEGER"      # forcer un type : INTEGER | BIGINT | TEXT | VARCHAR(N) | etc.
```

### Stratégies disponibles

| Stratégie | Résultat |
|---|---|
| `columns` | Une colonne par champ JSON (défaut) |
| `jsonb` | L'objet entier en une colonne JSONB |
| `pivot` | EAV générique `(parent_id, key, value)` |
| `structured_pivot` | EAV structuré `(parent_id, base, val_suffixe1, ...)` |
| `ignore` | La clé est supprimée (ni schéma, ni données) |

### Exemple réel — OpenFoodFacts

```bash
json2sql \
  --input openfoodfacts-products.jsonl \
  --db-url $DATABASE_URL \
  --wide-column-threshold 50 \
  --stable-threshold 0.10 \
  --rare-threshold 0.001 \
  --schema-config openfoodfacts.toml \
  --drop-existing \
  --parallel 4
```

```toml
# openfoodfacts.toml

# images: ~2600 tables enfants → stocker tout en JSONB
[openfoodfacts_products_images]
strategy = "jsonb"

# nutriments: pattern calcium_100g / calcium_unit → StructuredPivot auto-détecté
# (pas besoin d'override, sauf pour forcer des suffixes explicites)

# ecoscore_extended_data: dépasse la limite 1600 colonnes de PostgreSQL
[openfoodfacts_products_ecoscore_extended_data]
strategy = "jsonb"
```

Résultat : 125 colonnes stables dans la table principale, 184 clés médium dans `_wide` (EAV), 189 572 clés rares supprimées.

---

## Conseils pratiques

**Fichiers temporaires volumineux** : la Pass 2 écrit les COPY dans des fichiers temporaires. Sur des datasets de plusieurs Go, s'assurer que `TMPDIR` pointe vers un disque avec suffisamment d'espace :

```bash
TMPDIR=/path/to/large/disk json2sql --input big.jsonl --db-url $DATABASE_URL
```

**Exploration du schéma avant import** : utiliser `--dry-run --schema-report` pour valider le schéma inféré sans connexion à la base :

```bash
json2sql --input data.json --dry-run --schema-report-output schema_report.txt
```

**Taux d'anomalies** : un `--max-anomaly-rate 0.01` (1 %) est une valeur raisonnable pour détecter des données corrompues sans bloquer les petites incohérences attendues.
