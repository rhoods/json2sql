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

Scanned 200 object(s) → 3 table(s) detected

┌─ products (6 columns)
│  id                             INTEGER
│  name                           VARCHAR(64)
│  price                          DOUBLE PRECISION
│  available                      BOOLEAN
│  category                       VARCHAR(32)
│  tags                           VARCHAR(16)[]

...

⚠ 1 column(s) with mixed types detected (re-run with --schema-report for details)
```

Un tableau de scalaires apparaît comme une colonne tableau de sa table (`tags VARCHAR(16)[]`), pas comme une table.

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

## Tableaux de scalaires

Un tableau de scalaires n'est jamais une table : c'est une colonne tableau de la table qui le contient (`VARCHAR(n)[]`, `INTEGER[]`, `DOUBLE PRECISION[]`…). Le type des éléments suit les règles d'une colonne ordinaire.

| JSON | Cellule |
|---|---|
| `"tags": ["rust", "sql"]` | `{rust,sql}` |
| `"tags": []` | `{}` (tableau vide) |
| `tags` absent, ou `"tags": null` | `NULL` |
| `"tags": "bio"` (champ tantôt scalaire, tantôt tableau) | `{bio}` : une seule colonne tableau, la valeur scalaire y est un tableau à un élément ; le type des éléments est élargi si besoin (entier + texte → texte) |
| `"tags": [["a"], ["b"]]` (tableaux imbriqués) | colonne `TEXT[]` dont chaque élément est le JSON du sous-tableau, en texte |

Requêtes, sur l'import de [`examples/products.jsonl`](../../examples/products.jsonl) :

```sql
-- les produits qui ont un tag donné
SELECT name FROM products WHERE 'chocolate' = ANY(tags);

-- contenance et intersection : le littéral doit avoir le type de la colonne (VARCHAR[]),
-- pas TEXT[] comme le ferait ARRAY['biscuit'] seul
SELECT name FROM products WHERE tags @> '{biscuit}';
SELECT name FROM products WHERE tags && ARRAY['spread', 'biscuit']::varchar[];

-- une ligne par élément, avec sa position
SELECT p.name, t.tag, t.pos
FROM products p, unnest(p.tags) WITH ORDINALITY AS t(tag, pos)
ORDER BY p.name, t.pos;

-- nombre d'éléments et premier élément
SELECT name, cardinality(tags) AS n, tags[1] AS premier FROM products;
```

**Ordre.** L'ordre d'un tableau de scalaires est l'index dans la colonne. `unnest(...) WITH ORDINALITY` redonne la position que `j2s_order` donnait aux anciennes tables de jonction, avec un décalage : elle commence à **1**, alors que `j2s_order` (toujours présent pour les tableaux d'objets) commence à 0.

**Anomalies.** Un élément qui ne peut pas être stocké (chaîne plus longue que le `VARCHAR(n)` de la colonne, valeur qui n'a pas le type des éléments, octet nul dans un texte…) devient `NULL` dans le tableau, et l'import continue. La cellule compte pour **une seule** anomalie, même si plusieurs éléments sont fautifs : le rapport donne la colonne (le tableau) et le premier élément fautif.

> **Changement.** L'option `--array-as-pg-array` et la case « Arrays as PG arrays » de l'IHM n'existent plus : ce comportement est le seul. Un script qui passe encore l'option échoue (`error: unexpected argument '--array-as-pg-array' found`, code de sortie 2). Un snapshot de schéma ancien qui contient des tables de jonction est refusé avec un message qui demande de relancer la Pass 1 ; un snapshot fait avec l'option est accepté tel quel.

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
| `--suggest-groups` | aucun | Au lieu d'importer, propose un découpage de toutes les tables larges en tables 1:1 thématiques : écrit le TOML `column_groups` (une section par table qui a un groupe) dans le fichier donné et affiche le rapport (Markdown) sur la sortie standard. Une table est éligible si sa stratégie est `Columns` ou `AutoSplit` et qu'elle a au moins 20 colonnes de données ; les colonnes sans groupe restent dans la table d'origine, qui garde le lien avec les tables enfants issues des objets. Aucune base n'est nécessaire ; incompatible avec `--dry-run`. `--limit N` borne les lignes racine relues (`0` éteint les signaux de données). Un groupe dont la table existerait déjà (un objet imbriqué `nutrition` donne déjà la table `{table}_nutrition`) est proposé avec le suffixe `_cols` (`nutrition_cols`, puis `nutrition_cols_2` si ce nom est pris aussi), pour que le TOML s'applique tel quel. Les colonnes d'un groupe sont listées par ordre de nom. Les tableaux de scalaires (`*_tags`…) sont des colonnes de la table analysée, comme les autres. Le TOML se réutilise tel quel avec `--schema-config` (clé `column_groups`, voir plus bas). Incompatible avec `--group-columns` |
| `--group-columns` | désactivé | Stratégie « groupes de colonnes » (#91) : découpe chaque table large (`Columns` ou `AutoSplit`, au moins 20 colonnes de données) en tables 1:1, une par famille de colonnes que ses noms confirment, sans relire le fichier ni proposer de TOML. S'applique aussi à un snapshot chargé par `--schema-input`. Une table dont la section de `--schema-config` a une clé `column_groups` garde ceux du TOML (`column_groups = {}` la laisse entière). Voir « Découper une table en groupes de colonnes » plus bas |
| `--embedding-model` | aucun | Avec `--suggest-groups` : dossier d'un modèle d'embeddings statiques (Model2Vec à tokenizer WordPiece, par exemple `potion-base-8M` : `model.safetensors` et `tokenizer.json`). Pour chaque colonne laissée dans une table, le rapport donne les deux groupes dont le centre est le plus proche de son nom, avec un score (cosinus). C'est une indication : rien n'est déplacé ni fusionné sur ce signal. Le modèle est fourni par l'utilisateur, json2sql ne le télécharge jamais ; un tokenizer d'un autre type (le modèle multilingue est de type Unigram) est refusé avec un message clair |

### Paramètres d'inférence

| Paramètre | Défaut | Description |
|---|---|---|
| `--text-threshold` | 256 | Longueur max avant TEXT (sinon VARCHAR) |
| `--wide-column-threshold` | 15 | Seuil de détection des tables larges, compté en colonnes **scalaires** : les colonnes tableau ne comptent pas |
| `--stable-threshold` | 0.10 | Fréquence min pour colonne stable (AutoSplit) |
| `--rare-threshold` | 0.001 | Fréquence sous laquelle une clé n'est **pas importée** (AutoSplit). `0` = tout conserver |
| `--sibling-threshold` | 2 | Nb min de tables sœurs pour fusion |
| `--sibling-jaccard` | 0.8 | Similarité min des colonnes sœurs |
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
| `--max-anomaly-rate` | aucun | Taux max acceptable (0.0–1.0) ; abort si dépassé. Le taux se calcule sur toutes les lignes écrites, toutes tables confondues : les éléments d'un tableau de scalaires ne sont plus des lignes (ils l'étaient dans les anciennes tables de jonction), donc à données égales le taux est plus élevé qu'avant |

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

Ces types sont scalaires : une surcharge de type sur une colonne tableau est ignorée avec un avertissement, la colonne reste un tableau.

### Découper une table en groupes de colonnes (`column_groups`)

La clé `column_groups` d'une table range des colonnes dans des tables filles 1:1, une par groupe. C'est le format que produit `--suggest-groups` ; les noms de colonnes sont ceux de PostgreSQL.

```toml
[openfoodfacts.column_groups]
nutrition = ["nutrition_data", "nutrition_data_per", "nutrition_grades"]
packaging = ["packaging_materials_tags", "packaging_recycling_tags", "packaging_shapes_tags"]
```

- La table du groupe s'appelle `{table}_{groupe}` (53 octets au plus, un hash si c'est plus long). Elle reprend l'identifiant de la table d'origine (`j2s_id`) et une clé étrangère vers elle ; la table d'origine garde les colonnes qui ne sont dans aucun groupe, ainsi que ses autres tables enfants.
- Les groupes sont appliqués après les surcharges de types, de stratégie et `flatten`, dans l'ordre alphabétique de leurs noms.
- Un groupe est ignoré, avec un avertissement, si une colonne est inconnue, générée par json2sql (`j2s_*`), déjà dans un autre groupe, si le groupe a moins de 3 colonnes ou si le nom de table qu'il aurait existe déjà. Une table dont la stratégie n'est ni `columns` ni `AutoSplit`, ou dont toutes les colonnes partiraient, n'est pas découpée.
- À l'import, la table d'un groupe reçoit une ligne par objet de la table d'origine qui a au moins une valeur dans les colonnes du groupe. Une valeur est tout sauf `null`, `""`, `[]` et `{}` (la règle de `--suggest-groups`). La ligne reprend le `j2s_id` de la ligne d'origine : `LEFT JOIN {table}_{groupe} USING (j2s_id)` redonne la table de départ. Quand toutes les colonnes d'un groupe sont vides, il n'y a pas de ligne et la jointure rend ces colonnes à `NULL`, y compris celles qui contenaient `""` ou `[]`. `--dry-run` affiche le DDL.
- Une valeur de groupe qui ne rentre pas dans le type de sa colonne est une anomalie de la table du groupe (fichier d'anomalies au nom de cette table) ; chaque ligne de groupe écrite compte dans le taux de `--max-anomaly-rate`, au même titre que les lignes des autres tables.
- Réappliquer la même config à un schéma déjà découpé (`--schema-input` d'un snapshot) ne change rien. Un snapshot qui contient des groupes est écrit en version 3 : un json2sql plus ancien le refuse.

#### Groupes automatiques (`--group-columns`, #91)

Au lieu d'écrire les groupes, on peut laisser json2sql les trouver dans les noms des colonnes : c'est une stratégie, désactivée par défaut, comme les autres.

- **CLI** : `--group-columns`. Chaque table large (stratégie effective `Columns` ou `AutoSplit`, au moins 20 colonnes de données) reçoit les groupes que ses noms confirment : une famille de colonnes qui partagent leur premier mot (`allergens`, `allergens_from_user`, `allergens_tags`…), d'au moins 3 colonnes. Une fusion de plusieurs familles (`nutrition_*` et `nutriscore_*`) n'est appliquée que si les données la confirment (voir plus bas) ; sinon `--suggest-groups` la montre « à confirmer ».
- **Ordre** : la stratégie passe après la Pass 1 (ou le chargement du snapshot), le TOML et les choix Pivot/Jsonb/Skip, puis le DDL et la Pass 2 écrivent les tables de groupe comme celles d'un TOML. `--dry-run` les montre.
- **Le TOML l'emporte** : une table dont la section a une clé `column_groups` garde ceux du TOML, même si tous sont rejetés ; `column_groups = {}` la garde entière. Une table déjà découpée, ou une table de groupe, ne l'est jamais une seconde fois.
- **Snapshot** : le snapshot écrit par `--schema-output` ne contient jamais les groupes automatiques ; ils sont recalculés à chaque chargement avec `--group-columns` (quelques secondes sur OFF), et toujours de la même façon, quel que soit le nombre de workers.
- **Noms** : `{table}_{mot}`. Si cette table existe déjà (un objet ou un tableau imbriqué du même nom, comme `ingredients`), le groupe devient `{mot}_cols`, puis `{mot}_cols_2`…
- **IHM** : la case « Column groups » de Setup (Strategy selection), décochée par défaut, coche après la Pass 1 toutes les tables découpables ; l'écran Strategy garde une case par table, avec l'aperçu des tables de groupe ou la raison de leur absence. Les tables cochées sont enregistrées dans le snapshot (`group_columns`), et la CLI qui charge ce snapshot les découpe aussi, même sans `--group-columns`.
- Les avertissements de la stratégie commencent par `group-columns:` (par exemple une table dont toutes les colonnes partiraient n'est pas découpée).
- **Signaux de données** : chaque Pass 1 garde un échantillon des lignes, au plus 20 000 et 256 Mo, une ligne tous les `taille / 20 000` octets, le même quel que soit le nombre de workers. Il en tire, pour chaque table large, la présence et les valeurs de ses colonnes, et les range dans le snapshot (champ optionnel `samples`, lu sans erreur par une version antérieure qui l'ignore ; environ 10 Mo de plus sur le snapshot d'OFF). Avec ces signaux, la stratégie applique les fusions que les données confirment, comme `--suggest-groups`, sans relire le fichier. Une table dont les colonnes ont changé depuis la Pass 1 (un `flatten` du TOML) ou un snapshot plus ancien se contente des noms.
- **Candidats à confirmer** : une colonne laissée dans la table que les données rapprochent nettement d'un groupe (le groupe est rempli avec elle bien plus souvent que la richesse des fiches ne l'explique : lift moyen d'au moins 2, avec 30 % d'avance sur le groupe suivant, sur au moins 1 % des lignes) est signalée, jamais déplacée : dans le rapport `--suggest-groups`, en commentaire sous son groupe dans le TOML (`# might join …, to confirm`), et dans l'aperçu de l'écran Strategy (« might join, to confirm »). Sur OFF complet il y en a 13 (`manufacturing_places`, vitamines et minéraux vers `additives`…), certains discutables : c'est une indication.

### Stratégies disponibles

Exemples de schémas SQL avec et sans chaque stratégie : [strategies.md](strategies.md).

| Stratégie | Résultat |
|---|---|
| `columns` | Une colonne par champ JSON (défaut) |
| `jsonb` | L'objet entier en une colonne JSONB |
| `pivot` | EAV générique `(parent_id, key, value)` |
| `structured_pivot` | Les clés à suffixes dans une table `{table}_pivot` : `(parent_id, name, value, suffixe1, ...)` ; les autres colonnes restent |
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
