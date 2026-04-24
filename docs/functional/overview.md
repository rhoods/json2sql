# json2sql — Vue d'ensemble fonctionnelle

Outil Rust pour importer des fichiers JSON volumineux (plusieurs Go) dans une base de données PostgreSQL relationnelle, en :

- inférant le schéma (types, nullabilité) à partir d'un scan complet du fichier
- normalisant les objets imbriqués et les tableaux en tables filles liées par clés étrangères
- chargeant les données via `COPY FROM STDIN` — la méthode la plus rapide pour PostgreSQL
- produisant un rapport des anomalies de type (valeurs incohérentes avec le type dominant)

---

## Choix de conception

### Deux passes sur le fichier

| Approche | Avantage | Inconvénient |
|---|---|---|
| **1 passe + ALTER TABLE** (rejeté) | Lecture unique | ALTER TABLE sur des millions de lignes existantes = réécriture coûteuse |
| **2 passes** (retenu) | Schéma définitif avant insertion, COPY optimal | Lecture deux fois (acceptable sur SSD, ~10–30s pour 5 Go) |

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

Avec `--array-as-pg-array`, les tableaux de scalaires deviennent des colonnes `TEXT[]`, `INTEGER[]`, etc. au lieu de tables de jonction.

### Clés générées (préfixe `j2s_`)

Toutes les tables reçoivent des colonnes synthétiques réservées :

| Colonne | Type | Rôle |
|---|---|---|
| `j2s_id` | UUID v7 | Clé primaire (time-ordered) |
| `j2s_parent_id` | UUID v7 | Clé étrangère vers la table parente |
| `j2s_order` | BIGINT | Préserve l'ordre d'apparition dans les tableaux |

UUID v7 est choisi pour ses propriétés time-ordered (index B-tree efficace, insert-order préservé).

### Inférence de types

Le `TypeTracker` suit un histogramme de types pour chaque colonne. Le type "le plus large" gagne :

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

Si plusieurs types sont présents (ex. : 1000 INTEGER + 3 STRING), le type dominant est utilisé et les valeurs incompatibles sont enregistrées comme anomalies (insérées en NULL).

### Nommage des tables

- Noms normalisés : minuscules, caractères non-alphanumériques → `_`, underscores consécutifs fusionnés
- Limite PostgreSQL de 63 octets : si dépassée, les derniers caractères sont remplacés par `_` + hash 7 hex du nom original (collision-safe)
- `NOT NULL` si aucun null observé en passe 1

### Performance d'insertion

- Chaque table est bufférisée dans un fichier temporaire (TSV) pendant la passe 2
- Flush périodique : quand une table atteint `--batch-size` lignes (défaut : 100 000), le COPY est exécuté immédiatement et le fichier est réinitialisé — la taille des temp files reste bornée
- Débit typique : 100k–500k lignes/seconde selon la largeur du schéma

### Gestion des anomalies

Les valeurs incompatibles avec le type inféré produisent :
- une entrée `NULL` dans la base (aucune perte de ligne)
- une entrée dans le rapport d'anomalies (table, colonne, valeur, type observé vs attendu)

Le rapport est exportable en JSON ou CSV via `--anomaly-output`. L'option `--max-anomaly-rate` fait échouer l'import si le taux d'anomalies dépasse le seuil fixé (ex. : `0.01` = 1 %).
