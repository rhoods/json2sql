# json2sql — Vue d'ensemble fonctionnelle

Outil Rust pour importer des fichiers JSON volumineux (plusieurs Go) dans une base de données PostgreSQL relationnelle, en :

- inférant le schéma (types, nullabilité) à partir d'un scan complet du fichier
- normalisant les objets imbriqués et les tableaux d'objets en tables filles liées par clés étrangères (un tableau de scalaires devient une colonne tableau)
- chargeant les données via `COPY FROM STDIN` — la méthode la plus rapide pour PostgreSQL
- produisant un rapport des anomalies de type (valeurs non convertibles vers le type inféré)

---

## Choix de conception

### Deux passes sur le fichier

| Approche | Avantage | Inconvénient |
|---|---|---|
| **1 passe + ALTER TABLE** (rejeté) | Lecture unique | ALTER TABLE sur des millions de lignes existantes = réécriture coûteuse |
| **2 passes** (retenu) | Schéma définitif avant insertion, COPY optimal | Fichier lu deux fois |

La passe 1 ne fait aucune écriture en base — uniquement du suivi de types en mémoire (`TypeTracker` par colonne). Pour 500 tables × 50 colonnes, cela représente ~500 Ko de RAM.

### Normalisation complète

Les objets imbriqués et les tableaux d'objets deviennent des tables filles — pas de colonne JSONB par défaut. Chaque niveau d'imbrication crée une table préfixée :

```
users                    → table racine
users_address            → objet imbriqué { "address": {...} }
users_orders             → tableau d'objets [ {"amount": ...} ]
users_orders_items       → imbrication profonde
```

Un tableau de scalaires ne crée pas de table : c'est une colonne tableau de sa table parente (`tags VARCHAR(n)[]`, `INTEGER[]`, etc.). Dans `users`, `"tags": ["rust", "sql"]` donne `{rust,sql}`, `[]` donne `{}`, un tableau absent ou `null` donne `NULL`, et l'ordre est l'index du tableau. Un champ tantôt scalaire, tantôt tableau (`"bio"` puis `["bio", "vegan"]`) devient une seule colonne tableau : chaque valeur scalaire y est stockée comme un tableau à un élément (`{bio}`), et le type des éléments est élargi si besoin (entier + texte → texte). Exemples de requêtes : [usage.md](usage.md#tableaux-de-scalaires).

### Tables larges : la structure décide, les noms proposent

Certains objets ont des clés qui sont des données (devises, langues, identifiants) : une colonne par clé donnerait des
centaines de colonnes presque vides. json2sql les met en forme selon un principe (#200) :

- **la structure décide seule** : une table large et creuse (plus de `--wide-column-threshold` colonnes, peu présentes
  ligne à ligne) passe par le **split** : ses clés fréquentes restent des colonnes, les autres vont dans une table
  clé/valeur ; si ses valeurs sont toutes du même type, toutes ses clés y vont. Des objets frères de même forme sont
  fusionnés en une table, avec une colonne `key` ;
- **les noms proposent** : la grille de suffixes (`sugars`, `sugars_100g`, `sugars_unit`…) et les groupes de colonnes
  (`packaging_*`, `origin_*`…) ne s'appliquent pas d'eux-mêmes. `--suggest` les écrit dans un TOML, que l'utilisateur
  valide et passe avec `--schema-config` ;
- **aucune clé n'est jetée** : une erreur de détection donne au pire un schéma moins lisible, qu'on corrige dans le TOML
  ou dans l'IHM.

JSONB n'est jamais choisi seul. Exemples : [strategies.md](strategies.md).

### Clés générées (préfixe `j2s_`)

Toutes les tables reçoivent des colonnes synthétiques réservées :

| Colonne | Type | Rôle |
|---|---|---|
| `j2s_id` | UUID v7 | Clé primaire (time-ordered) |
| `j2s_<parent>_id` | UUID v7 | Clé étrangère vers la table parente |
| `j2s_order` | BIGINT | Préserve l'ordre d'apparition dans les tableaux d'objets (l'ordre d'un tableau de scalaires est l'index dans sa colonne) |

UUID v7 est choisi pour ses propriétés time-ordered (index B-tree efficace, insert-order préservé).

### Inférence de types

Le `TypeTracker` suit un histogramme de types pour chaque colonne. Le type **le plus large** observé gagne (pas le plus fréquent). Exception : des chaînes qui font moins de 0,1 % des valeurs non nulles ne décident pas du type ; elles deviennent `NULL` + anomalie en Pass 2 (détail : `docs/technical/schema-inference.md` § 1).

```
int + float       → DOUBLE PRECISION
int + bigint      → BIGINT
string courte     → VARCHAR(max_len × 1.2)   si max_len ≤ seuil (défaut 256)
string longue     → TEXT
ISO date          → DATE
ISO timestamp     → TIMESTAMP           (heure avec décalage ±HH:MM écrite en UTC)
UUID (36 chars)   → UUID
bool              → BOOLEAN
```

Quelques chaînes suffisent à faire passer la colonne en texte : `1000 INTEGER + 3 STRING` → `VARCHAR`, sans anomalie (3 chaînes sur 1 003 valeurs, au-dessus de 0,1 %). Sous ce seuil, elles ne comptent pas : `10000 INTEGER + 1 STRING` reste `INTEGER`. Les anomalies concernent les valeurs qui ne rentrent pas dans le type retenu (ex. : booléen dans une colonne `INTEGER`, chaîne rare dans une colonne numérique, chaîne plus longue que le `VARCHAR(N)` inféré) : elles sont insérées en `NULL` et enregistrées dans le rapport. Détails : [schema-inference.md](../technical/schema-inference.md).

### Nommage des tables

- Noms normalisés : minuscules, caractères non-alphanumériques → `_`, underscores consécutifs fusionnés
- Un nom de table tient en 53 octets, pour que les noms qui en dérivent (`pk_…`, `fk_…_parent`, `j2s_…_id`) restent sous la limite de 63 octets de PostgreSQL. Un nom trop long perd d'abord le début de son chemin (`openfoodfacts_products_ecoscore_extended_data_impact_likeliest_recipe` → `ecoscore_extended_data_impact_likeliest_recipe`) ; un hash 7 hex n'arrive qu'en dernier recours, ou quand deux chemins donneraient le même nom
- Un nom de colonne tient en 63 octets : au-delà, ses derniers caractères sont remplacés par `_` + un hash 7 hex du nom d'origine
- `NOT NULL` si aucun null observé en passe 1

### Performance d'insertion

- Pipeline **diskless** : N workers parsent le JSON en parallèle et accumulent les lignes au format COPY en mémoire ; un flusher les envoie à PostgreSQL via `COPY FROM STDIN` au fil de l'eau. Aucun fichier temporaire (sauf si l'entrée vient de stdin, bufferisée sur disque car lue deux fois).
- La RAM est bornée : au-delà de 512 MiB en attente, les workers sont mis en pause le temps que le flusher draine.
- Les PK/FK sont ajoutées après le chargement (plus rapide que de les vérifier ligne à ligne).

### Gestion des anomalies

Les valeurs incompatibles avec le type inféré produisent :
- une entrée `NULL` dans la base (aucune perte de ligne)
- une entrée dans le rapport d'anomalies (table, colonne, valeur, type observé vs attendu)

Dans un tableau de scalaires, seul l'élément fautif devient `NULL`, et la cellule compte pour une seule anomalie.

Le rapport est exportable en JSON ou CSV via `--anomaly-output`. L'option `--max-anomaly-rate` fait échouer l'import si le taux d'anomalies dépasse le seuil fixé (ex. : `0.01` = 1 %).
