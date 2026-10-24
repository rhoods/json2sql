# json2sql — Utilisation

## Installation

```bash
cargo build --release
# Binaire : ./target/release/json2sql
```

Variable d'environnement : `DATABASE_URL` est lue automatiquement si `--db-url` est absent.

## Formats d'entrée

Le format est détecté automatiquement :

| Contenu | Interprétation |
|---|---|
| `[ {...}, {...} ]` | Tableau JSON : un objet = une ligne de la table racine |
| `{...}` puis `{...}` … | NDJSON : **un objet par ligne** (les objets indentés sur plusieurs lignes ne sont pas supportés) |
| `{"produits": [...], "magasins": [...]}` | Conteneur : chaque clé devient une table racine. **Toutes** les valeurs doivent être des tableaux |
| `{...}` seul (même indenté) | Un seul enregistrement |

Un objet unique qui mélange tableaux et autres valeurs (ex. `{"meta": {...}, "foods": [...]}`) est **refusé** car ambigu : conteneur ou enregistrement ? Pour l'importer comme un enregistrement, l'entourer de crochets : `[ {...} ]`.

---

## Sous-commandes

json2sql propose deux modes :

| Commande | Usage |
|---|---|
| *(défaut)* | Import complet : inférence de schéma + chargement dans PostgreSQL |
| `inspect` | Exploration rapide : inférence brute sur un échantillon, sans base de données |

---

## Sous-commande `inspect`

Inspecte les N premiers objets d'un fichier JSON/NDJSON et affiche le schéma inféré brut — sans stratégies de fusion, sans garde-fous de colonnes, sans connexion à une base de données.

**Utilité :** comprendre la structure d'un fichier trop volumineux pour être ouvert, ou diagnostiquer pourquoi json2sql ne détecte pas une architecture particulière.

```bash
# Inspecter les 500 premiers objets (défaut)
json2sql inspect mon_fichier.json

# Limiter à 100 objets pour un aperçu immédiat
json2sql inspect mon_fichier.json --limit 100

# Écrire le schéma dans un fichier (stderr garde les messages de progression)
json2sql inspect mon_fichier.json --limit 200 --output schema.txt

# Extraire l'échantillon JSON dans un fichier NDJSON
json2sql inspect mon_fichier.json --limit 200 --sample-output extrait.ndjson

# Les deux en même temps
json2sql inspect mon_fichier.json --limit 200 --output schema.txt --sample-output extrait.ndjson

# Spécifier un nom de table racine
json2sql inspect mon_fichier.json --limit 200 --table commandes

# Ajuster le seuil TEXT/VARCHAR
json2sql inspect mon_fichier.json --limit 500 --text-threshold 512
```

### Paramètres de `inspect`

| Paramètre | Défaut | Description |
|---|---|---|
| `FILE` | *(requis)* | Fichier JSON ou NDJSON à inspecter |
| `--limit` | 500 | Nombre maximum d'objets racines à scanner |
| `--table` | dérivé du nom de fichier | Nom de la table racine |
| `--text-threshold` | 256 | Longueur max avant TEXT (sinon VARCHAR) |
| `--output` | stdout | Écrit le schéma inféré dans ce fichier |
| `--sample-output` | aucun | Écrit les objets scannés en NDJSON dans ce fichier |

### Exemple de sortie

```
Inspecting 'products.jsonl' (limit: 200 objects)...

Scanned 200 object(s) → 4 table(s) detected

┌─ products (5 columns)
│  id                             INTEGER
│  name                           VARCHAR(64)
│  price                          DOUBLE PRECISION
│  available                      BOOLEAN
│  category                       VARCHAR(32)

┌─ products_tags (1 columns)
│  parent: products
│  value                          VARCHAR(16)

...

⚠ 1 column(s) with mixed types detected (re-run with --schema-report for details)
```

### Workflow typique

```bash
# 1. Explorer la structure générale
json2sql inspect fichier.json --limit 50

# 2. Élargir et extraire un échantillon réutilisable
json2sql inspect fichier.json --limit 2000 --sample-output sample.ndjson

# 3. Tester l'import sur l'échantillon avant de lancer sur le fichier complet
json2sql --input sample.ndjson --db-url $DATABASE_URL --dry-run

# 4. Lancer l'import complet
json2sql --input fichier.json --db-url $DATABASE_URL
```

---

## Exemples rapides (mode import)

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
| `--schema-output` | aucun | Sauvegarde le schéma de la Pass 1 (JSON) |
| `--schema-input` | aucun | Charge un schéma sauvegardé et saute la Pass 1 |
| `--suggest-groups` | aucun | Au lieu d'importer, propose un découpage de toutes les tables larges en tables 1:1 thématiques : écrit le TOML `column_groups` (une section par table qui a un groupe) dans le fichier donné et affiche le rapport (Markdown) sur la sortie standard. Une table est éligible si sa stratégie est `Columns` ou `AutoSplit` et qu'elle a au moins 20 colonnes de données ; les colonnes sans groupe restent dans la table d'origine, qui garde le lien avec les tables enfants issues des objets. Aucune base n'est nécessaire ; incompatible avec `--dry-run`. `--limit N` borne les lignes racine relues (`0` éteint les signaux de données). Les tableaux de scalaires (`*_tags`…) ne sont analysés qu'avec `--array-as-pg-array`, sinon ils deviennent des tables de jonction hors de la table analysée (le rapport en donne le nombre par table). Le TOML n'est pas encore appliqué par `--schema-config` (#78, T3) |
| `--embedding-model` | aucun | Avec `--suggest-groups` : dossier d'un modèle d'embeddings statiques (Model2Vec à tokenizer WordPiece, par exemple `potion-base-8M` : `model.safetensors` et `tokenizer.json`). Pour chaque colonne laissée dans une table, le rapport donne les deux groupes dont le centre est le plus proche de son nom, avec un score (cosinus). C'est une indication : rien n'est déplacé ni fusionné sur ce signal. Le modèle est fourni par l'utilisateur, json2sql ne le télécharge jamais ; un tokenizer d'un autre type (le modèle multilingue est de type Unigram) est refusé avec un message clair |

### Paramètres d'inférence

| Paramètre | Défaut | Description |
|---|---|---|
| `--text-threshold` | 256 | Longueur max avant TEXT (sinon VARCHAR) |
| `--wide-column-threshold` | 15 | Seuil de détection des tables larges |
| `--stable-threshold` | 0.10 | Fréquence min pour colonne stable (AutoSplit) |
| `--rare-threshold` | 0.001 | Fréquence sous laquelle une clé n'est **pas importée** (AutoSplit). `0` = tout conserver |
| `--sibling-threshold` | 2 | Nb min de tables sœurs pour fusion |
| `--sibling-jaccard` | 0.8 | Similarité min des colonnes sœurs |
| `--array-as-pg-array` | false | Tableaux scalaires → colonne `TEXT[]`. Un champ tantôt scalaire, tantôt tableau devient une seule colonne tableau ; ses valeurs scalaires sont stockées comme tableaux à un élément (`"bio"` → `{bio}`) |
| `--depth-limit` | aucun | Avertissement si imbrication dépasse N niveaux |
| `--workers` | 1 | Threads de la Pass 1 (inférence parallèle) |
| `--disable-strategy` | aucun | Désactive une stratégie d'inférence optionnelle. Répétable. Valeurs : `sibling`, `pivot`, `structured_pivot`. La stratégie `split` (wide-table) est obligatoire et ne peut pas être désactivée. |

### Paramètres de chargement

| Paramètre | Défaut | Description |
|---|---|---|
| `--parallel` | 1 | Workers de la Pass 2 (parse + formatage COPY), connexions COPY du flusher et connexions pour l'ajout des contraintes |
| `--no-constraints` | false | N'ajoute pas les PK/FK en fin d'import |
| `--tune-wal` | false | Relève `max_wal_size` à 16 GB pendant la Pass 2 (moins de checkpoints), puis remet l'ancienne valeur. Réglage global au serveur : il faut un rôle superuser, sinon il est ignoré avec un warning |
| `--batch-size` | 100 000 | ⚠️ Sans effet depuis le pipeline diskless (accepté pour compatibilité) |
| `--transaction` | false | ⚠️ Sans effet actuellement (accepté mais non câblé) |
| `--limit` | aucun | Arrête le dispatch Pass 2 après N objets racines. Pass 1 tourne toujours sur le fichier complet. `0` = créer les tables sans lignes. |

**Serveur d'analyse local : `wal_level = minimal`** (#75, #77). Les tables enfants sont chargées en `COPY FREEZE` dans la transaction qui les vide. Avec `wal_level = minimal`, la table racine l'est aussi, et PostgreSQL n'écrit aucune de ces tables dans le WAL. Sur OpenFoodFacts 70 GB, l'import passe de 64 min (`replica`) à 39 min, et le WAL de 64,6 à 0,2 GiB. json2sql affiche un avertissement quand le serveur n'est pas réglé ainsi, mais ne change jamais ce réglage, qui demande un redémarrage :

```sql
ALTER SYSTEM SET wal_level = 'minimal';
ALTER SYSTEM SET max_wal_senders = 0;
-- puis redémarrer PostgreSQL
```

`max_wal_senders = 0`, `archive_mode = off` et, à partir de PG 17, `summarize_wal = off` sont obligatoires avec `minimal`, sinon PostgreSQL refuse de démarrer. À réserver à une base locale d'analyse : la réplication, la restauration à un instant donné (PITR) et `pg_basebackup` ne fonctionnent plus. Pour revenir en arrière : `ALTER SYSTEM RESET wal_level; ALTER SYSTEM RESET max_wal_senders;`, puis redémarrer.

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
# openfoodfacts.toml (à créer, non fourni dans le dépôt)

# images: ~2600 tables enfants → stocker tout en JSONB
[openfoodfacts_products_images]
strategy = "jsonb"

# nutriments: pattern calcium_100g / calcium_unit → StructuredPivot auto-détecté
# (pas besoin d'override, sauf pour forcer des suffixes explicites)

# ecoscore_extended_data: dépasse la limite 1600 colonnes de PostgreSQL
[openfoodfacts_products_ecoscore_extended_data]
strategy = "jsonb"
```

Résultat : 125 colonnes stables dans la table principale, 184 clés médium dans `_wide` (EAV), 189 572 clés rares **non importées** (listées dans un `WARNING` sur stderr ; `--rare-threshold 0` pour tout garder).

---

## Conseils pratiques

**Disque et mémoire** : la Pass 2 n'écrit aucun fichier temporaire, la RAM est bornée (pause des workers au-delà de 512 MiB en attente). Seule exception : une entrée lue sur stdin est d'abord copiée dans `$TMPDIR` (le fichier doit être lu deux fois) — prévoir l'espace correspondant ou passer `--input`.

**Explorer un fichier inconnu** : utiliser `inspect` pour comprendre la structure d'un fichier trop grand à ouvrir, avant tout import :

```bash
json2sql inspect data.json --limit 200
```

**Exploration du schéma avant import** : utiliser `--dry-run --schema-report` pour valider le schéma inféré complet sans connexion à la base :

```bash
json2sql --input data.json --dry-run --schema-report-output schema_report.txt
```

**Taux d'anomalies** : un `--max-anomaly-rate 0.01` (1 %) est une valeur raisonnable pour détecter des données corrompues sans bloquer les petites incohérences attendues.
