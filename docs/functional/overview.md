# json2sql — Vue d'ensemble fonctionnelle

Outil Rust pour importer des fichiers JSON volumineux (plusieurs Go) dans une base de données PostgreSQL relationnelle, en :

- inférant le schéma (types, nullabilité) à partir d'un scan complet du fichier
- normalisant les objets imbriqués et les tableaux en tables filles liées par clés étrangères
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

Les objets imbriqués et les tableaux deviennent des tables filles — pas de colonne JSONB par défaut. Chaque niveau d'imbrication crée une table préfixée :

```
users                    → table racine
users_address            → objet imbriqué { "address": {...} }
users_orders             → tableau d'objets [ {"amount": ...} ]
users_tags               → tableau de scalaires ["rust", "sql"] → (value, j2s_order)
users_orders_items       → imbrication profonde
```

⚠️ Le flag `--array-as-pg-array` (qui devait transformer les tableaux de scalaires en colonnes `TEXT[]`, `INTEGER[]`, etc. au lieu de tables de jonction) est temporairement désactivé — voir [issue #48](https://github.com/rhoods/json2sql/issues/48) : perte de données silencieuse ou crash DDL sur hétérogénéité scalaire/array. Le comportement par défaut (table de jonction, décrit ci-dessus) n'est pas affecté.

### Clés générées (préfixe `j2s_`)

Toutes les tables reçoivent des colonnes synthétiques réservées :

| Colonne | Type | Rôle |
|---|---|---|
| `j2s_id` | UUID v7 | Clé primaire (time-ordered) |
| `j2s_<parent>_id` | UUID v7 | Clé étrangère vers la table parente |
| `j2s_order` | BIGINT | Préserve l'ordre d'apparition dans les tableaux |

UUID v7 est choisi pour ses propriétés time-ordered (index B-tree efficace, insert-order préservé).

### Inférence de types

Le `TypeTracker` suit un histogramme de types pour chaque colonne. Le type **le plus large** observé gagne (pas le plus fréquent) :

```
int + float       → DOUBLE PRECISION
int + bigint      → BIGINT
string courte     → VARCHAR(max_len × 1.2)   si max_len ≤ seuil (défaut 256)
string longue     → TEXT
ISO date          → DATE
ISO timestamp     → TIMESTAMP
UUID (36 chars)   → UUID
bool              → BOOLEAN
```

Une seule chaîne suffit à faire passer la colonne en texte : `1000 INTEGER + 3 STRING` → `VARCHAR`, sans anomalie. Les anomalies concernent les valeurs qui ne rentrent pas dans le type retenu (ex. : booléen dans une colonne `INTEGER`, chaîne plus longue que le `VARCHAR(N)` inféré) : elles sont insérées en `NULL` et enregistrées dans le rapport. Détails : [schema-inference.md](../technical/schema-inference.md).

### Nommage des tables

- Noms normalisés : minuscules, caractères non-alphanumériques → `_`, underscores consécutifs fusionnés
- Limite PostgreSQL de 63 octets : si dépassée, les derniers caractères sont remplacés par `_` + hash 7 hex du nom original (collision-safe)
- `NOT NULL` si aucun null observé en passe 1

### Performance d'insertion

- Pipeline **diskless** : N workers parsent le JSON en parallèle et accumulent les lignes au format COPY en mémoire ; un flusher les envoie à PostgreSQL via `COPY FROM STDIN` au fil de l'eau. Aucun fichier temporaire (sauf si l'entrée vient de stdin, bufferisée sur disque car lue deux fois).
- La RAM est bornée : au-delà de 512 MiB en attente, les workers sont mis en pause le temps que le flusher draine.
- Les PK/FK sont ajoutées après le chargement (plus rapide que de les vérifier ligne à ligne).

### Gestion des anomalies

Les valeurs incompatibles avec le type inféré produisent :
- une entrée `NULL` dans la base (aucune perte de ligne)
- une entrée dans le rapport d'anomalies (table, colonne, valeur, type observé vs attendu)

Le rapport est exportable en JSON ou CSV via `--anomaly-output`. L'option `--max-anomaly-rate` fait échouer l'import si le taux d'anomalies dépasse le seuil fixé (ex. : `0.01` = 1 %).
