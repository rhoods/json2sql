# Stratégies : exemples de schémas SQL

Chaque section montre une stratégie sur un petit fichier JSON : la commande, puis le schéma **sans** la stratégie et
**avec**. Les fichiers sont dans [`examples/strategies/`](../../examples/strategies/) ou dans `tests/fixtures/` : chaque
exemple se refait avec la commande indiquée.

Le SQL est celui de `--dry-run`, allégé pour la lecture : sans le préfixe `"public".`, sans guillemets, sans les clés
primaires (`j2s_id`) ni les clés étrangères (`j2s_<parent>_id` → `<parent>.j2s_id`), que json2sql crée toujours.

Rappel des colonnes générées :

- `j2s_id` : identifiant de la ligne (UUID, clé primaire) ;
- `j2s_<parent>_id` : la ligne parente (clé étrangère) ;
- `j2s_order` : la position de l'élément dans un tableau d'objets.

Ce que json2sql décide seul, d'après la **structure** du JSON (#200) :

- le **split** des tables larges et creuses (voir [plus bas](#le-split--les-tables-larges-et-creuses)), toujours
  appliqué, réglé par `--wide-column-threshold` et `--stable-threshold` ;
- la fusion **`sibling`** des objets de même forme, qui se désactive avec `--disable-strategy sibling` ou dans
  Setup > Strategy selection.

Ce que json2sql **propose** d'après les **noms** des clés, sans l'appliquer : la grille de suffixes et les groupes de
colonnes. `--suggest` les écrit dans un TOML ; l'utilisateur garde ce qui lui convient et relance avec
`--schema-config`. Le reste se demande table par table : dans le TOML (`strategy = …`), dans l'écran Strategy de
l'IHM, ou avec `--group-columns` pour les groupes de colonnes. JSONB n'est jamais choisi seul.

---

## Par défaut : une table par objet (`columns`)

```json
{"code": "0001", "name": "Muesli", "brand": {"name": "Alpen", "country": "ch"},
 "reviews": [{"stars": 4, "text": "bon"}, {"stars": 5, "text": "top"}], "tags": ["bio", "vegan"]}
```

```bash
json2sql --input examples/strategies/products.jsonl --dry-run
```

Un objet imbriqué devient une table enfant, un tableau d'objets une table enfant ordonnée, un tableau de scalaires une
colonne tableau PostgreSQL.

```sql
CREATE TABLE products (
    j2s_id UUID NOT NULL,
    code VARCHAR(5),
    name VARCHAR(8),
    tags VARCHAR(6)[]          -- ["bio", "vegan"]
);
CREATE TABLE products_brand (  -- l'objet "brand"
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    country VARCHAR(3),
    name VARCHAR(9)
);
CREATE TABLE products_reviews (  -- le tableau "reviews"
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    j2s_order BIGINT NOT NULL,
    stars INTEGER,
    text VARCHAR(6)
);
```

---

## `pivot` : des clés dynamiques en lignes clé/valeur

Pour un objet dont les clés sont des données (une langue, un pays, une date…) et les valeurs du même type.

```json
{"id": 1, "scores": {"fr": 7, "de": 5, "en": 6}}
{"id": 2, "scores": {"fr": 4, "it": 8}}
```

```bash
json2sql --input examples/strategies/scores.jsonl --dry-run                                            # sans
json2sql --input examples/strategies/scores.jsonl --schema-config examples/strategies/pivot.toml --dry-run  # avec
```

```toml
# pivot.toml
[scores_scores]
strategy = "pivot"
```

Sans : une colonne par clé rencontrée, une nouvelle langue ajoute une colonne.

```sql
CREATE TABLE scores_scores (
    j2s_id UUID NOT NULL,
    j2s_scores_id UUID NOT NULL,
    de INTEGER,
    en INTEGER,
    fr INTEGER,
    it INTEGER,
    es INTEGER,
    pt INTEGER
);
```

Avec : une ligne par clé.

```sql
CREATE TABLE scores_scores (
    j2s_id UUID NOT NULL,
    j2s_scores_id UUID NOT NULL,
    key TEXT,       -- 'fr', 'de'…
    value INTEGER   -- 7, 5…
);
```

Sans TOML, le split fait presque la même chose sur une table large et creuse aux valeurs d'un seul type : la table
garde son nom, vide, et les lignes clé/valeur vont dans un compagnon `scores_scores_pivot` (voir
[le split](#le-split--les-tables-larges-et-creuses)). `strategy = "pivot"` les met dans la table elle-même, et la table
absorbe ses tables enfants.

---

## `jsonb` : l'objet entier dans une colonne JSONB

Pour un objet trop irrégulier pour des colonnes, que l'on interrogera avec les opérateurs JSON de PostgreSQL.
Seulement sur demande : json2sql ne choisit plus JSONB seul (#200), une table large aux types mélangés passe par le
split.

```bash
json2sql --input examples/strategies/products.jsonl --schema-config examples/strategies/jsonb.toml --dry-run
```

```toml
# jsonb.toml
[products_brand]
strategy = "jsonb"
```

Sans : voir `products_brand` dans l'exemple par défaut. Avec :

```sql
CREATE TABLE products_brand (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    data JSONB      -- {"name": "Alpen", "country": "ch"}
);
```

PostgreSQL refuse l'octet nul dans une colonne JSONB. Un objet qui en contient un, dans une chaîne ou dans une clé,
n'y est pas écrit : sa colonne `data` vaut `NULL`, et l'objet est rapporté comme anomalie
(`string_contains_null_byte`). L'import continue. Sur la table racine, c'est tout l'enregistrement qui manque ; le
rapport d'anomalies n'en garde que les 200 premiers caractères.

---

## Grille de suffixes (`suffix_columns`) : des clés à suffixes en lignes typées

Pour des clés qui combinent un nom et un suffixe répété (`calories`, `calories_100g`, `calories_serving`, `fat`,
`fat_100g`…). Ces clés forment une grille : des noms (`calories`, `fat`, `salt`) croisés avec des suffixes (`_100g`,
`_serving`).

```json
{"name": "Widget", "nutrients": {"calories": 100, "calories_100g": 200, "calories_serving": 150,
                                 "fat": 5, "fat_100g": 10, "fat_serving": 8,
                                 "salt": 1, "salt_100g": 2, "salt_serving": 3}}
```

La grille n'est **jamais appliquée seule** (#200) : les noms seuls peuvent former une grille dans n'importe quel
domaine. Elle s'applique quand le TOML la demande :

```bash
json2sql --input tests/fixtures/structured_pivot.jsonl --table products --dry-run                                # sans
json2sql --input tests/fixtures/structured_pivot.jsonl --table products --schema-config examples/strategies/grille.toml --dry-run  # avec
```

```toml
# examples/strategies/grille.toml
[products_nutrients]
suffix_columns = ["_100g", "_serving"]
```

Sans : une colonne par clé dans `products_nutrients` (l'objet a moins de colonnes que le seuil des tables larges ; plus
large et creux, il passerait par le split).

Avec : la grille part dans une table `products_nutrients_pivot`, avec une ligne par nom et une colonne par suffixe.
`products_nutrients` garde son nom et tout ce qui n'est pas dans la grille.

```sql
CREATE TABLE products_nutrients (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL
    -- ici, les autres clés de l'objet : textes, tableaux… ; ses objets restent des tables filles
);

CREATE TABLE products_nutrients_pivot (
    j2s_id UUID NOT NULL,
    j2s_products_nutrients_id UUID NOT NULL,
    name TEXT,         -- 'calories', 'fat', 'salt'
    value INTEGER,     -- la clé sans suffixe : "calories": 100
    _100g INTEGER,     -- "calories_100g": 200
    serving INTEGER    -- "calories_serving": 150
);
```

- La colonne d'un suffixe porte le nom du suffixe. Un suffixe qui commence par un chiffre garde son `_` (`_100g`),
  car un nom de colonne ne peut pas commencer par un chiffre. `_name` devient `norm_name`, pour ne pas se confondre
  avec la colonne `name`.
- `value` n'existe que si l'objet a des clés sans suffixe (`calories` seul). Si la grille a aussi un suffixe `_value`,
  c'est lui qui remplit `value` (`fat_value`), et la clé sans suffixe (`fat`) remplit `bare_value` (#199).
- Une clé va au suffixe le plus long qui la termine : `fat_prepared_100g` va dans `prepared_100g`, pas dans `_100g`.
- La grille du TOML passe avant le split : une table large et creuse qui a une grille garde ses colonnes de suffixes.

**Les parties en début de clé (`prefix_columns`, #199).** Pour `min_temperature`, `max_temperature`, `min_wind`… les
colonnes sont au début des clés :

```toml
[meteo]
prefix_columns = ["min_", "max_", "avg_"]
```

donne `meteo_pivot (name, min, max, avg)`, une ligne par mesure (`name = 'temperature'`). Un `_` est ajouté à la fin
d'un préfixe qui n'en a pas (`min` devient `min_`). Avec `suffix_columns` dans la même section, `suffix_columns`
s'applique et `prefix_columns` est ignoré, avec un avertissement.

**La proposer.** `--suggest FICHIER` cherche une grille dans chaque table d'au moins 20 colonnes de données et l'écrit
**en commentaire** sous la section de la table : `# suffix_columns = ["_100g", "_serving"]`. Enlever le `#` l'applique.
L'ancienne forme `strategy = "structured_pivot"` avec `suffix_columns` est toujours lue. Une grille n'est proposée que
si :

- chaque suffixe revient avec au moins trois noms : deux clés comme `en:celery_seed` et `en:mustard_seed` ne suffisent
  pas à créer un suffixe `_seed` ;
- les suffixes portent les mêmes noms (#199) : la plupart des noms d'un suffixe doivent déjà être dans la grille.
  `user_id`, `created_at`, `user_name` ne donnent plus `_at` avec `_id` et `_name`, car `_at` date d'autres noms ;
- les suffixes sont remplis ensemble (#199), quand l'échantillon de lignes est lisible. Pour chaque suffixe, sur les
  noms qu'il partage avec le plus rempli, on compte les objets où les deux clés sont remplies, sur ceux de la clé la
  plus rare. À moins de la moitié, ce sont des **alternatives** (`air_temp_c` / `air_temp_f`, `title_fr` / `title_en`,
  `revenue_2021` / `revenue_2022`, une seule par objet) : une colonne par suffixe serait presque toujours vide. Elles
  sont écartées de la grille et nommées dans le rapport ; si aucun suffixe ne reste avec le plus rempli, rien n'est
  proposé, et ces clés restent dans la table clé/valeur du split, une ligne par clé ;
- elle couvre au moins 30 % des clés de l'objet.

Une grille qui a **plus de suffixes que de noms** est à l'envers (#199) : `min_temperature`… donne 3 noms (`min`, `max`,
`avg`) et 12 suffixes (les mesures). Elle est proposée en `# prefix_columns = ["avg_", "max_", "min_"]`, une ligne par
mesure. La règle est la même quel que soit l'ordre des clés : `fr_title` donne `suffix_columns` (une ligne par
langue), `title_fr` donne `prefix_columns`, et les deux mènent à `(name = 'fr', title, description…)`.

Limites : le rapport dit « from the names alone » quand l'échantillon n'est pas lisible (un tableau sur le chemin de la
table, ou `--limit 0`). Les alternatives ne sont alors pas repérées : avec 3 langues et 5 champs, une langue par
objet, `title_fr`… serait proposé en `suffix_columns` avec une colonne par langue, surtout vide. Les clés en camelCase
(`tempMin`) et le séparateur `-` (`energy-kcal`) ne sont pas découpés.

> **Changement (#199).** Sur une table qui a `suffix_columns` avec `_value`, la colonne `norm_value` s'appelle
> maintenant `value`, et l'ancienne `value` (la clé sans suffixe) s'appelle `bare_value` : les requêtes SQL sont à
> reprendre, et un réimport dans une base créée avant garde l'ancien schéma sans `--drop-existing`.

---

## `sibling` : des objets de même forme fusionnés en une table

Pour un objet dont chaque clé porte un objet de même structure (`front`, `back`, `side`…).

```json
{"id": 1, "images": {"front": {"url": "https://img.example/1/front.jpg", "width": 800, "height": 600},
                     "back":  {"url": "https://img.example/1/back.jpg",  "width": 800, "height": 600}, …}}
```

```bash
json2sql --input examples/strategies/images.jsonl --disable-strategy sibling --dry-run  # sans
json2sql --input examples/strategies/images.jsonl --dry-run                             # avec (détecté seul)
```

Sans : une table par clé.

```sql
CREATE TABLE images_images (
    j2s_id UUID NOT NULL,
    j2s_images_id UUID NOT NULL
);
CREATE TABLE images_images_back (
    j2s_id UUID NOT NULL,
    j2s_images_images_id UUID NOT NULL,
    height INTEGER,
    url VARCHAR(36),
    width INTEGER
);
CREATE TABLE images_images_front ( … mêmes colonnes … );
CREATE TABLE images_images_side ( … mêmes colonnes … );
CREATE TABLE images_images_top ( … mêmes colonnes … );
```

Avec : une seule table, la clé devient une colonne.

```sql
CREATE TABLE images_images (
    j2s_id UUID NOT NULL,
    j2s_images_id UUID NOT NULL,
    key TEXT,          -- 'front', 'back', 'side', 'top'
    height INTEGER,
    url TEXT,
    width INTEGER
);
```

---

## Le split : les tables larges et creuses

json2sql coupe seul une table **large et creuse** : beaucoup de clés, chacune présente dans peu de lignes. C'est la même
règle pour la table racine et pour une table enfant, qu'elle ait des sous-objets ou non :

```
1. Table large ?   plus de --wide-column-threshold colonnes de valeurs simples (15 par défaut)
   └─ non → une colonne par clé
2. Table stable ?  au moins 10 lignes, et plus de la moitié des colonnes présentes dans au moins
                   --stable-threshold des lignes (10 % par défaut)
   └─ oui → une colonne par clé
3. Sinon, le split :
   - table enfant aux valeurs d'un seul type (toutes des nombres, toutes du texte…) : les clés sont des
     données (devises, langues…), toutes partent en lignes clé/valeur dans un compagnon {table}_pivot ;
   - table racine, ou types mélangés : les clés présentes dans au moins --stable-threshold des lignes
     restent des colonnes, les autres partent dans un compagnon {table}_wide.
```

Aucune clé n'est jetée : même une clé vue une seule fois va dans le compagnon (`--rare-threshold` n'existe plus, #200).
Les colonnes tableau PostgreSQL et les objets enfants restent sur la table. Une table qui a une stratégie dans le TOML
ou dans l'IHM (`strategy = "columns"` compris) n'est pas coupée.

**Types mélangés : les clés fréquentes en colonnes.**

```json
{"id": 1, "name": "Widget", "details": {"color": "red"}, "tag_a": "x1", "tag_b": "y1"}
{"id": 4, "name": "Thingamajig", "details": {"color": "purple"}}
{"id": 5, "name": "Whatsit", "details": {"color": "orange"}, "rare_key": "rare_value"}
```

```bash
json2sql --input tests/fixtures/auto_split.jsonl --table products --dry-run    # sans
json2sql --input tests/fixtures/auto_split.jsonl --table products --dry-run \
  --wide-column-threshold 3 --stable-threshold 0.80                             # avec
```

Sans :

```sql
CREATE TABLE products (
    j2s_id UUID NOT NULL,
    id INTEGER,
    name VARCHAR(14),
    tag_a VARCHAR(3),
    tag_b VARCHAR(3),
    rare_key VARCHAR(12)
);
```

Avec : les clés présentes dans au moins 80 % des lignes restent des colonnes ; toutes les autres vont dans
`products_wide`, même `rare_key`, vue dans une seule ligne.

```sql
CREATE TABLE products (
    j2s_id UUID NOT NULL,
    id INTEGER,
    name VARCHAR(14)
);
CREATE TABLE products_wide (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    key TEXT,          -- 'tag_a', 'tag_b', 'rare_key'
    value TEXT
);
```

`products_details` (l'objet `details`) est la même dans les deux cas.

Le type de `value` est celui qui accepte toutes les valeurs du compagnon ; des types mélangés donnent `TEXT`, où
`false` devient `'false'`.

**Un seul type : toutes les clés en clé/valeur.** Avec le fichier de l'exemple `pivot` (3 lignes : sous 10 lignes, une
table n'est jamais jugée stable, d'où le split malgré « 100% stable » dans le journal) :

```bash
json2sql --input examples/strategies/scores.jsonl --dry-run --wide-column-threshold 3
```

```sql
CREATE TABLE scores_scores (
    j2s_id UUID NOT NULL,
    j2s_scores_id UUID NOT NULL
);
CREATE TABLE scores_scores_pivot (
    j2s_id UUID NOT NULL,
    j2s_scores_scores_id UUID NOT NULL,
    key TEXT,          -- 'fr', 'de'…
    value INTEGER
);
```

**`--stable-threshold` vaut pour toutes les tables.** Avant #200, il ne réglait que la racine ; il règle maintenant
aussi le split des tables enfants, et un réglage personnalisé change leur schéma.

---

## `flatten` : un objet enfant remonté en colonnes préfixées

```bash
json2sql --input examples/strategies/products.jsonl --schema-config examples/strategies/flatten.toml --dry-run
```

```toml
# flatten.toml
[products_brand]
strategy = "flatten"
prefix = "brand_"
```

Seul un objet simple s'aplatit : une table en `Columns`, sans tables filles. Un tableau d'objets, une table pivot ou
JSONB, ou un objet qui contient d'autres objets ou tableaux d'objets restent des tables, avec un avertissement
(`cannot be flattened`) : les aplatir perdrait des données.

Sans : voir `products` et `products_brand` dans l'exemple par défaut. Avec, la table `products_brand` disparaît :

```sql
CREATE TABLE products (
    j2s_id UUID NOT NULL,
    code VARCHAR(5),
    name VARCHAR(8),
    tags VARCHAR(6)[],
    brand_country VARCHAR(3),
    brand_name VARCHAR(9)
);
```

---

## Des clés dynamiques qui portent des objets : la fusion `sibling`

Pour un objet dont les clés sont des identifiants et les valeurs des objets (`img_123`, `img_456`…), la fusion
`sibling` est automatique : une ligne par identifiant, qui devient la colonne `key`.

```json
{"id": 1, "name": "Widget", "images": {"img_123": {"url": "http://a.com", "width": 800},
                                       "img_456": {"url": "http://b.com", "width": 600}}}
```

```bash
json2sql --input tests/fixtures/normalize_dynamic_keys.jsonl --table products --dry-run
```

```sql
CREATE TABLE products_images (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    key TEXT,          -- 'img_123', 'img_456'…
    url VARCHAR(15),
    width INTEGER
);
```

L'ancienne stratégie TOML `normalize_dynamic_keys` n'existe plus (#200) : elle faisait la même chose, en perdant les
sous-objets des objets et les champs propres de la table. Un TOML qui la demande reçoit un avertissement et garde le
schéma de la fusion automatique ; pour régler celle-ci, `--sibling-threshold` et `--sibling-jaccard`.

---

## Groupes de colonnes (`--group-columns`) : une table large découpée par thème

Pour une table dense et large (au moins 20 colonnes de données) dont les colonnes forment des familles par leur nom.
Désactivé par défaut (#91).

```json
{"code": "code0", "name": "name0", "brand": "brand0", …,
 "nutrition_fat": "fat0", "nutrition_sugars": "sugars0", …,
 "packaging_material": "material0", …, "origin_country": "country0", …}
```

```bash
json2sql --input examples/strategies/foods.jsonl --dry-run                  # sans
json2sql --input examples/strategies/foods.jsonl --group-columns --dry-run  # avec
```

Sans : 20 colonnes dans une table.

```sql
CREATE TABLE foods (
    j2s_id UUID NOT NULL,
    barcode VARCHAR(10),
    brand VARCHAR(8),
    code VARCHAR(6),
    creator VARCHAR(10),
    lang VARCHAR(6),
    name VARCHAR(6),
    nutrition_fat VARCHAR(5),
    nutrition_fiber VARCHAR(8),
    nutrition_salt VARCHAR(6),
    nutrition_sugars VARCHAR(9),
    origin_city VARCHAR(6),
    origin_country VARCHAR(10),
    origin_farm VARCHAR(6),
    origin_region VARCHAR(9),
    packaging_material VARCHAR(11),
    packaging_recycling VARCHAR(12),
    packaging_shape VARCHAR(8),
    packaging_weight VARCHAR(9),
    quantity VARCHAR(11),
    stores VARCHAR(9)
);
```

Avec : une table 1:1 par famille. Une ligne de groupe reprend le `j2s_id` de sa ligne, donc
`foods LEFT JOIN foods_nutrition USING (j2s_id)` redonne les colonnes de départ.

```sql
CREATE TABLE foods (
    j2s_id UUID NOT NULL,
    barcode VARCHAR(10),
    brand VARCHAR(8),
    code VARCHAR(6),
    creator VARCHAR(10),
    lang VARCHAR(6),
    name VARCHAR(6),
    quantity VARCHAR(11),
    stores VARCHAR(9)
);
CREATE TABLE foods_nutrition (
    j2s_id UUID NOT NULL,          -- le j2s_id de la ligne de foods
    j2s_foods_id UUID NOT NULL,
    nutrition_fat VARCHAR(5),
    nutrition_fiber VARCHAR(8),
    nutrition_salt VARCHAR(6),
    nutrition_sugars VARCHAR(9)
);
CREATE TABLE foods_origin (
    j2s_id UUID NOT NULL,
    j2s_foods_id UUID NOT NULL,
    origin_city VARCHAR(6),
    origin_country VARCHAR(10),
    origin_farm VARCHAR(6),
    origin_region VARCHAR(9)
);
CREATE TABLE foods_packaging (
    j2s_id UUID NOT NULL,
    j2s_foods_id UUID NOT NULL,
    packaging_material VARCHAR(11),
    packaging_recycling VARCHAR(12),
    packaging_shape VARCHAR(8),
    packaging_weight VARCHAR(9)
);
```

Les données de la Pass 1 (un échantillon des lignes, gardé dans le snapshot) peuvent aussi confirmer la fusion de deux familles, par exemple `nutrition_*` et `nutriscore_*` quand leurs colonnes partagent les mêmes valeurs : les deux familles forment alors une seule table de groupe, `{table}_nutriscore_nutrition`. Une colonne isolée que les données rapprochent d'un groupe est seulement signalée (« might join, to confirm »), jamais déplacée.

Les mêmes groupes s'écrivent à la main dans le TOML (`[foods.column_groups]`, voir
[usage.md](usage.md#découper-une-table-en-groupes-de-colonnes-column_groups)), et `--suggest` les propose
avec un rapport. Les groupes du TOML passent avant le split ; ceux de `--group-columns` après lui, sur les colonnes qui
restent.

---

## `skip` (IHM)

Dans l'écran Strategy, `Skip` retire une table de l'import, avec ses tables enfants : elle n'apparaît ni dans le DDL ni
dans les données.
