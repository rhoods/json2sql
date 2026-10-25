# json2sql

Outil Rust qui importe des fichiers JSON/NDJSON volumineux dans PostgreSQL **sans écrire de schéma à la main** : il infère tables, colonnes et types, normalise les objets imbriqués en tables liées par clés étrangères, puis charge les données via `COPY FROM STDIN`.

## Avant / après

Entrée ([`examples/products.jsonl`](examples/products.jsonl), 3 lignes) :

```json
{"code":"3017620422003","name":"Nutella","quantity_g":400,"price":3.99,"tags":["spread","chocolate"],"nutriments":{"energy_kcal":539,"sugars":56.3},"stores":[{"name":"Carrefour","city":"Paris"}]}
{"code":"7622210449283","name":"Prince","quantity_g":300,"price":2,"tags":["biscuit"],"nutriments":{"energy_kcal":467,"sugars":32},"stores":[]}
{"code":"3274080005003","name":"Cristaline","quantity_g":1500,"price":"N/A","tags":[],"nutriments":{"energy_kcal":0,"sugars":0},"stores":[{"name":"Leclerc","city":"Lyon"},{"name":"Auchan","city":"Lille"}]}
```

`json2sql --input examples/products.jsonl --dry-run` produit (extrait) :

```sql
CREATE TABLE IF NOT EXISTS "public"."products" (
    "j2s_id" UUID NOT NULL,
    "code" VARCHAR(16),
    "name" VARCHAR(12),
    "price" VARCHAR(30),        -- un seul "N/A" suffit : le type le plus large l'emporte
    "quantity_g" INTEGER,
    "tags" VARCHAR(11)[],       -- tableau de scalaires : une colonne tableau, pas de table de jonction
    CONSTRAINT "pk_products" PRIMARY KEY (j2s_id)
);

CREATE TABLE IF NOT EXISTS "public"."products_nutriments" (   -- objet imbriqué
    "j2s_id" UUID NOT NULL,
    "j2s_products_id" UUID NOT NULL,                          -- FK vers products
    "energy_kcal" INTEGER,
    "sugars" DOUBLE PRECISION,                                -- entiers + flottants
    CONSTRAINT "pk_products_nutriments" PRIMARY KEY (j2s_id)
);

CREATE TABLE IF NOT EXISTS "public"."products_stores" (       -- tableau d'objets
    "j2s_id" UUID NOT NULL,
    "j2s_products_id" UUID NOT NULL,
    "j2s_order" BIGINT NOT NULL,                              -- ordre dans le tableau
    "city" VARCHAR(6),
    "name" VARCHAR(11),
    CONSTRAINT "pk_products_stores" PRIMARY KEY (j2s_id)
);

-- + les FOREIGN KEY, ajoutées après le chargement
```

## Fonctionnement

- **Pass 1** : scan complet du fichier pour inférer le schéma. Le type retenu est le plus large observé (`INTEGER` → `BIGINT` → `DOUBLE PRECISION` → texte), pas le plus fréquent.
- **Pass 2** : pipeline *diskless*. N workers parsent le JSON (`simd_json`) et forment les lignes COPY en mémoire ; un flusher les envoie à PostgreSQL au fil de l'eau. Aucun fichier temporaire, RAM bornée par contre-pression.
- **Tables larges** : détection automatique de structures à clés dynamiques (pivot EAV, fusion de tables sœurs, JSONB, *AutoSplit*), surchargeable par un fichier TOML.
- **Anomalies** : une valeur non convertible vers le type inféré est insérée en `NULL` et tracée dans un rapport JSON/CSV ; `--max-anomaly-rate` fait échouer l'import au-delà d'un seuil.

Sur l'export OpenFoodFacts (base ouverte de produits alimentaires), la table racine expose 189 881 clés distinctes : *AutoSplit* en garde 125 en colonnes, 184 dans une table EAV `_wide` et écarte les 189 572 restantes (voir ci-dessous).

> ⚠️ **Clés rares non importées.** Sur une table très large, *AutoSplit* écarte les clés présentes dans moins de `--rare-threshold` des lignes (défaut 0,1 %). Elles sont listées dans un `WARNING` sur stderr. Pour tout conserver : `--rare-threshold 0`.

## Installation

```bash
cargo build --release
# Binaire : ./target/release/json2sql
```

**Plateformes** : la CLI est testée en CI sous Linux, macOS et Windows. L'IHM (`json2sql-ui` + `json2sql-worker`) communique par socket Unix et fonctionne **uniquement sous Linux et macOS**.

## Utilisation rapide

```bash
# Explorer la structure d'un fichier inconnu (sans base de données)
json2sql inspect data.json --limit 200

# Import basique
json2sql --input data.json --db-url postgres://user:pass@localhost/mydb

# Depuis stdin (bufferisé dans $TMPDIR, le fichier étant lu deux fois)
cat data.jsonl | json2sql --db-url $DATABASE_URL --table my_root

# Afficher le DDL sans toucher la base
json2sql --input data.json --dry-run

# Valider le schéma inféré avant import
json2sql --input data.json --dry-run --schema-report

# Import avec surcharges manuelles et rapport d'anomalies
json2sql \
  --input data.jsonl \
  --db-url $DATABASE_URL \
  --schema-config overrides.toml \
  --drop-existing \
  --parallel 4 \
  --anomaly-output anomalies.json \
  --max-anomaly-rate 0.01
```

`DATABASE_URL` est lue automatiquement si `--db-url` est absent. Connexion sans TLS pour l'instant.

## Documentation

| Document | Contenu |
|---|---|
| [docs/index.md](docs/index.md) | Index complet — point d'entrée |
| [docs/functional/usage.md](docs/functional/usage.md) | Référence complète des flags CLI et config TOML |
| [docs/technical/architecture.md](docs/technical/architecture.md) | Pipeline et modules |
| [docs/technical/schema-inference.md](docs/technical/schema-inference.md) | Règles d'inférence de types et stratégies de tables larges |

## Licence

[MIT](LICENSE)
