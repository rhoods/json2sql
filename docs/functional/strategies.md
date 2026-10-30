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

Les stratégies `sibling`, `pivot` et `structured_pivot` sont détectées automatiquement (désactivables avec
`--disable-strategy`, ou dans Setup > Strategy selection). `AutoSplit` dépend des seuils `--wide-column-threshold`,
`--stable-threshold` et `--rare-threshold`. Les autres se demandent : dans le TOML `--schema-config` (`strategy = …`),
dans l'écran Strategy de l'IHM, ou avec `--group-columns` pour les groupes de colonnes.

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

json2sql choisit seul le pivot quand un objet a beaucoup de clés peu remplies (au-delà de `--wide-column-threshold`).

---

## `jsonb` : l'objet entier dans une colonne JSONB

Pour un objet trop irrégulier pour des colonnes, que l'on interrogera avec les opérateurs JSON de PostgreSQL.

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

---

## `structured_pivot` : des clés à suffixes en lignes typées

Pour des clés qui combinent un nom et un suffixe répété (`calories`, `calories_100g`, `calories_serving`, `fat`,
`fat_100g`…). Ces clés forment une grille : des noms (`calories`, `fat`, `salt`) croisés avec des suffixes (`_100g`,
`_serving`).

```json
{"name": "Widget", "nutrients": {"calories": 100, "calories_100g": 200, "calories_serving": 150,
                                 "fat": 5, "fat_100g": 10, "fat_serving": 8,
                                 "salt": 1, "salt_100g": 2, "salt_serving": 3}}
```

```bash
json2sql --input tests/fixtures/structured_pivot.jsonl --table products --dry-run                             # sans
json2sql --input tests/fixtures/structured_pivot.jsonl --table products --wide-column-threshold 4 --dry-run  # avec
```

Sans (l'objet a moins de colonnes que le seuil des tables larges) : une colonne par clé dans `products_nutrients`.

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
  car un nom de colonne ne peut pas commencer par un chiffre. `_value` devient `norm_value` et `_name` devient
  `norm_name`, pour ne pas se confondre avec les colonnes `value` et `name`.
- `value` n'existe que si l'objet a des clés sans suffixe (`calories` seul).
- Une clé va au suffixe le plus long qui la termine : `fat_prepared_100g` va dans `prepared_100g`, pas dans `_100g`.
- La grille n'est retenue que si chaque suffixe revient avec au moins trois noms, et si elle couvre au moins 30 % des
  clés de l'objet. Deux clés comme `en:celery_seed` et `en:mustard_seed` ne suffisent pas à créer un suffixe `_seed`.

Les suffixes peuvent aussi être donnés dans le TOML : `strategy = "structured_pivot"` et
`suffix_columns = ["_100g", "_serving"]`. Le découpage est le même : seules les clés de la grille partent dans la
table `_pivot`.

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

## `AutoSplit` : les clés fréquentes en colonnes, les autres en clé/valeur

Pour une table où quelques clés sont toujours là et beaucoup d'autres n'apparaissent que parfois.

```json
{"id": 1, "name": "Widget", "details": {"color": "red"}, "tag_a": "x1", "tag_b": "y1"}
{"id": 4, "name": "Thingamajig", "details": {"color": "purple"}}
{"id": 5, "name": "Whatsit", "details": {"color": "orange"}, "rare_key": "rare_value"}
```

```bash
json2sql --input tests/fixtures/auto_split.jsonl --table products --dry-run    # sans
json2sql --input tests/fixtures/auto_split.jsonl --table products --dry-run \
  --wide-column-threshold 3 --stable-threshold 0.80 --rare-threshold 0.30     # avec
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

Avec : les clés présentes dans au moins 80 % des lignes restent des colonnes ; celles entre 30 et 80 % vont dans
`products_wide` ; celles sous 30 % (`rare_key`) sont écartées.

```sql
CREATE TABLE products (
    j2s_id UUID NOT NULL,
    id INTEGER,
    name VARCHAR(14)
);
CREATE TABLE products_wide (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    key TEXT,          -- 'tag_a', 'tag_b'
    value VARCHAR(3)
);
```

`products_details` (l'objet `details`) est la même dans les deux cas.

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

## `normalize_dynamic_keys` : des clés dynamiques qui portent des objets

Pour un objet dont les clés sont des identifiants et les valeurs des objets (`img_123`, `img_456`…).

```json
{"id": 1, "name": "Widget", "images": {"img_123": {"url": "http://a.com", "width": 800},
                                       "img_456": {"url": "http://b.com", "width": 600}}}
```

```bash
json2sql --input tests/fixtures/normalize_dynamic_keys.jsonl --table products --sibling-threshold 10 --dry-run   # sans
json2sql --input tests/fixtures/normalize_dynamic_keys.jsonl --table products --sibling-threshold 10 \
  --schema-config examples/strategies/normalize.toml --dry-run                                                 # avec
```

```toml
# normalize.toml
[products_images]
strategy = "normalize_dynamic_keys"
id_column = "image_id"
```

Sans : une table par identifiant (`--sibling-threshold 10` empêche ici la fusion automatique `sibling`).

```sql
CREATE TABLE products_images (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL
);
CREATE TABLE products_images_img_123 (
    j2s_id UUID NOT NULL,
    j2s_products_images_id UUID NOT NULL,
    url VARCHAR(15),
    width INTEGER
);
CREATE TABLE products_images_img_456 ( … mêmes colonnes … );
-- … une table par identifiant : img_001, img_002, img_789
```

Avec : une ligne par identifiant, qui devient une colonne.

```sql
CREATE TABLE products_images (
    j2s_id UUID NOT NULL,
    j2s_products_id UUID NOT NULL,
    image_id TEXT,     -- 'img_123', 'img_456'…
    url VARCHAR(15),
    width INTEGER
);
```

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
[usage.md](usage.md#découper-une-table-en-groupes-de-colonnes-column_groups)), et `--suggest-groups` les propose
avec un rapport.

---

## `skip` (IHM)

Dans l'écran Strategy, `Skip` retire une table de l'import, avec ses tables enfants : elle n'apparaît ni dans le DDL ni
dans les données.
