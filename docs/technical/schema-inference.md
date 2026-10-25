# Contrôles de l'analyse de schéma

Ce document décrit tous les mécanismes de décision appliqués pendant la **Pass 1** (inférence du schéma), dans l'ordre où ils s'exécutent.

---

## 1. Inférence de type par colonne

**Fichier :** `src/schema/type_tracker.rs`

Chaque champ JSON observé dans la Pass 1 alimente un `TypeTracker` qui accumule un histogramme de types. À la fin, le type PostgreSQL définitif est déduit selon des règles de priorité.

### Types inférés

| Valeur JSON | Type inféré |
|---|---|
| `null` | Null |
| `true` / `false` | Boolean |
| Entier dans `[-2^31, 2^31-1]` | Integer |
| Entier hors de cette plage | BigInt |
| Nombre flottant | Float |
| Chaîne UUID (pattern RFC 4122) | Uuid |
| Chaîne date `YYYY-MM-DD` | Date |
| Chaîne timestamp ISO 8601 | Timestamp |
| Chaîne ≤ `text_threshold` chars | VarChar(N) |
| Chaîne > `text_threshold` chars | Text |
| Objet `{}` | Object (→ table enfant) |
| Tableau `[]` | Array (→ table enfant ou colonne array) |

### Règles de résolution (`to_pg_type`)

Le type retenu est le **plus large** parmi ceux observés, pas le plus fréquent. Une seule valeur chaîne suffit à faire passer la colonne en texte. Ordre de priorité :

1. **Une chaîne observée** → `VARCHAR(N)` ou `TEXT` (N tient compte de la représentation texte des nombres éventuellement mélangés)
2. Sinon **numérique** : `DOUBLE PRECISION` > `BIGINT` > `INTEGER`
3. Sinon : `BOOLEAN` > `TIMESTAMP` > `DATE` > `UUID`
4. Aucun type observé (que des `null`) → `TEXT`

Exemples : `1000 entiers + 3 chaînes` → `VARCHAR` (aucune anomalie, les entiers sont stockés en texte) ; `entiers + flottants` → `DOUBLE PRECISION`.

### Détection d'anomalie

Une anomalie est détectée en **Pass 2**, quand une valeur ne peut pas être convertie vers le type retenu (`src/pass2/coercer.rs`) : par exemple un booléen dans une colonne `INTEGER` (entiers + booléens → `INTEGER`), ou une chaîne plus longue que le `VARCHAR(N)` inféré. La valeur est insérée en `NULL` et enregistrée dans le rapport d'anomalies. Le taux d'anomalie est comparé à `--max-anomaly-rate` pour faire échouer l'import si les données sont trop incohérentes.

Le rapport de schéma (`--schema-report`) signale en `MIXED` les colonnes ayant plusieurs types observés, avec leur histogramme : utile pour repérer en amont les colonnes qui vont basculer en texte.

---

## 2. Longueur des chaînes et seuil text

**Paramètre CLI :** `--text-threshold` (défaut : 256)

Si la longueur maximale observée d'un champ texte dépasse ce seuil, le type passe de `VARCHAR(N)` à `TEXT`. Cela évite des colonnes `VARCHAR(10000)` inefficaces.

---

## 3. Détection des tables larges

**Fichier :** `src/schema/registry.rs`
**Paramètre CLI :** `--wide-column-threshold` (défaut : 15)

Une table est dite **large** si le nombre de colonnes de données (hors colonnes générées `j2s_*`) dépasse ce seuil. Une fois détectée comme large, plusieurs stratégies peuvent s'appliquer.

---

## 4. Garde ratio_stable (P1)

**Fichier :** `src/schema/registry.rs` — condition [3a]

Avant d'appliquer une stratégie wide, on vérifie :

```
ratio_stable = fraction de clés présentes dans >= stable_threshold % des lignes
```

Si `ratio_stable > 0.5` **et** que la table contient au moins 10 lignes, le schéma est un schéma dense légitime : la stratégie `Columns` est conservée, sans pivot ni AutoSplit.

Cela permet aux schémas réellement denses (ex. table de métriques avec 200 indicateurs tous remplis) de ne pas être faussement identifiés comme "trop larges".

Une table dense gardée en `Columns` (ou en `AutoSplit`) peut ensuite être découpée en tables 1:1 thématiques : `--suggest-groups` propose les groupes pour les tables d'au moins 20 colonnes de données, et la clé `column_groups` de la config les applique (§12).

---

## 5. AutoSplit pour la table racine

**Fichier :** `src/schema/registry.rs` — branche [3c]
**Paramètres CLI :** `--stable-threshold` (défaut : 0.10), `--rare-threshold` (défaut : 0.001)

S'applique uniquement à la **table racine** qui possède des tables enfants (objets imbriqués). La table racine est classifiée en trois zones selon la fréquence d'apparition de chaque clé :

| Zone | Critère | Destination |
|---|---|---|
| **Stable** | fréquence ≥ `stable_threshold` | colonne dans la table principale |
| **Médium** | fréquence entre `rare_threshold` et `stable_threshold` | ligne dans la table `{nom}_wide` (EAV : `key`, `value`) |
| **Rare** | fréquence < `rare_threshold` | **non importée** (ni schéma, ni données) — un `WARNING` sur stderr liste les clés concernées |

La table `_wide` est une table EAV synthétique générée automatiquement avec les colonnes `j2s_id`, `j2s_parent_id`, `j2s_order`, `key`, `value`. Elle est liée à la table principale via une clé étrangère.

**Exemple OpenFoodFacts :** 189 881 clés observées → 125 stables, 184 médiums, 189 572 rares supprimées.

> ⚠️ **Perte de données volontaire.** Les clés rares sont écartées pour éviter une table EAV de centaines de milliers de clés quasi vides. Pour tout conserver, passer `--rare-threshold 0` : toutes les clés non stables vont alors dans `{nom}_wide`.

---

## 6. Détection de suffixes structurés (StructuredPivot)

**Fichier :** `src/schema/suffix_detector.rs`

Pour les tables larges non-racines dont les colonnes suivent un pattern `{base}_{suffixe}` (ex. `calcium_100g`, `calcium_unit`, `calcium_serving`), un pivot structuré est auto-détecté.

### Algorithme

1. Extraction de tous les candidats suffixe (dernière partie après `_`)
2. Sélection des suffixes présents dans ≥ 2 bases distinctes et dans ≥ 30% des colonnes
3. Construction d'un `SuffixSchema` avec les types inférés par suffixe

Le résultat est une table EAV avec une ligne par `(parent_id, base_nom, val_100g, val_unit, ...)`.

Ce comportement peut être forcé manuellement dans le fichier de config TOML via `strategy = "structured_pivot"` et `suffix_columns = ["_100g", "_unit"]`.

---

## 7. Détection de tables sœurs (KeyedPivot)

**Fichier :** `src/schema/registry.rs` — `finalize_siblings()`
**Paramètres CLI :** `--sibling-threshold` (défaut : 2), `--sibling-jaccard` (défaut : 0.8)

Quand une table parent a ≥ `sibling_threshold` tables enfants directes avec des colonnes similaires (similarité de Jaccard ≥ `sibling_jaccard`), ces tables sœurs sont fusionnées. Deux types d'enfants sont détectés :

- **Object** : chaque clé JSON est un objet (`"fr": {...}, "en": {...}`)
- **ObjectArray** : chaque clé JSON est un tableau d'objets (`"fr": [{...}, {...}], "en": [{...}]`)

### Conditions d'éligibilité

- La table parent est un **conteneur pur** : elle n'a aucune colonne de données propre (toutes ses données sont dans les enfants)
- Les tables enfants ont des structures suffisamment proches (similarité de Jaccard ≥ seuil)

### Résultat

Les tables sœurs sont supprimées. La table parent devient une table **KeyedPivot** avec :

- Une colonne clé (`key`, `lang_code`, ou `key_id` selon la forme des clés)
- L'union des colonnes de données des sœurs (peut être vide si les sœurs sont des conteneurs purs)
- Une colonne **`j2s_data JSONB`** contenant l'objet enfant brut (ou l'élément du tableau pour ObjectArray). Cette colonne est toujours présente et préserve la structure complète, y compris les données imbriquées qui ne seraient pas représentables en colonnes plates.
- Pour les ObjectArray : une colonne `j2s_order BIGINT` pour conserver l'ordre des éléments

### Conteneurs purs

Quand les tables sœurs sont elles-mêmes des conteneurs purs (ex. `genome → contig → is_circular`, où `contig` n'a pas de champ scalaire direct), l'union des colonnes est vide. La colonne `j2s_data` capture alors la totalité de la structure imbriquée sous forme JSONB.

### Types de clés détectés (`KeyShape`)

| Forme | Exemple | Colonne générée |
|---|---|---|
| Numeric | `0`, `1`, `2` | `key_id` |
| IsoLang | `fr`, `en`, `de` | `lang_code` |
| Slug | `en:organic`, `fr:bio` | `key` |
| Mixed | mélange | `key` |

---

## 8. Stratégies wide alternatives (Pivot, Jsonb)

**Fichier :** `src/schema/registry.rs` — `suggest_wide_strategy()`

Pour les tables larges non-racines qui ne correspondent ni à un StructuredPivot ni à un AutoSplit, deux stratégies sont proposées :

| Stratégie | Critère | Stockage |
|---|---|---|
| **Pivot** | colonnes de types homogènes | table EAV `(parent_id, key, value)` |
| **Jsonb** | colonnes hétérogènes ou objet complexe | colonne JSONB unique dans la table parent |

Ces stratégies peuvent être forcées dans le TOML de config.

---

## 9. Sanitisation des noms d'identifiants

**Fichier :** `src/schema/naming.rs`

Tous les noms de tables et colonnes issus des clés JSON sont sanitisés avant utilisation :

1. Mise en minuscules
2. Remplacement des caractères non-alphanumériques par `_`
3. Élimination des `_` consécutifs
4. Préfixe `c_` si le nom commence par un chiffre
5. **Troncature à 63 bytes** (limite PostgreSQL) avec hash 7 hex pour préserver l'unicité
6. **Détection de collision** : si plusieurs clés JSON produisent le même identifiant SQL, un suffixe de hash est ajouté

Les noms tronqués et les collisions résolues sont rapportés dans les logs en avertissement.

---

## 10. Déduplication des tables

**Fichier :** `src/schema/registry.rs` — après le tri topologique

Après la construction de toutes les `TableSchema`, une déduplication par nom PG est appliquée. Si deux chemins JSON différents produisent le même nom de table (cas rare de collision non détectée par le registre de nommage), seule la première occurrence est conservée.

---

## 11. Passe d'exclusion des tables absorbées

**Fichier :** `src/schema/registry.rs` — `exclude_absorbed_children()`

Les stratégies **Pivot**, **Jsonb**, **StructuredPivot** et **KeyedPivot** absorbent leurs tables enfants (les données sont inline). La fonction `exclude_absorbed_children` supprime toutes les tables enfants (et leurs descendants) dont le parent a une stratégie absorbante.

`AutoSplit` n'absorbe **pas** ses enfants : les tables enfants (ex. `_nutriments`, `_ingredients`) restent indépendantes.

Cette passe est exécutée **deux fois** :
1. Dans `finalize()` — après la construction du schéma initial
2. Dans `main.rs` après `apply_overrides()` — pour prendre en compte les stratégies définies manuellement dans le TOML

---

## 12. Surcharges manuelles (TOML de config)

**Fichier :** `src/schema/config.rs`
**Paramètre CLI :** `--schema-config <fichier.toml>`

Le fichier TOML permet de forcer des décisions que l'inférence automatique ne peut pas prendre :

```toml
[nom_table]
strategy = "jsonb"          # forcer une stratégie
# strategy = "pivot"
# strategy = "structured_pivot"
# strategy = "columns"
# strategy = "ignore"
suffix_columns = ["_100g", "_unit"]  # suffixes explicites pour structured_pivot

[nom_table.columns]
ma_colonne = "INTEGER"      # forcer un type de colonne
```

Les surcharges sont appliquées après `finalize()` et suivies d'une nouvelle passe d'exclusion.

### Groupes de colonnes (`column_groups`, #78)

**Fichier :** `src/schema/column_groups.rs` (application), `src/pass2/insert.rs` (écriture)

```toml
[nom_table.column_groups]
nutrition = ["energie", "graisses", "sucres"]
```

Une table à stratégie `Columns` ou `AutoSplit` range chaque groupe dans une table fille 1:1 `{table}_{groupe}` (53 octets au plus, hash au-delà). Le groupe est appliqué après les types, la stratégie et `flatten`, dans l'ordre alphabétique des noms. La table fille est un enfant `Object` : `j2s_id` (clé primaire), clé étrangère vers le parent, puis les colonnes déplacées dans l'ordre du parent, avec leurs `flatten_sources`. Le parent garde les autres colonnes, ses autres tables enfants et la liste de ses tables de groupe (`TableSchema.column_groups`).

Un groupe est ignoré avec un avertissement (`ConfigWarning::ColumnGroup*`) pour une colonne inconnue, générée ou déjà dans un autre groupe, un groupe de moins de 3 colonnes, un nom de table déjà pris ou un parent qui n'aurait plus de colonne de données. Réappliquer la même config ne change rien ; une table de groupe existante avec d'autres colonnes est une erreur. Un snapshot qui contient des groupes est écrit en version 3 (`load` accepte 2 et 3).

**Pass 2.** `insert_object` écrit la ligne du parent, puis, pour chaque table de groupe, une ligne dont `j2s_id` et la clé étrangère valent l'id de la ligne du parent. La ligne n'est écrite que si une colonne du groupe a une valeur (`is_present` : ni `null`, ni `""`, ni `[]`, ni `{}`) ; chaque ligne écrite compte dans le taux d'anomalies. Les enfants ordinaires restent rattachés à la ligne d'origine. Cela vaut pour la racine, un enfant objet et les éléments d'un tableau d'objets.

---

## 13. Rapport de statistiques du schéma

**Fichier :** `src/schema/stats.rs`
**Paramètre CLI :** `--schema-report`, `--schema-report-output <fichier>`

Après la construction du schéma, un rapport peut être généré listant toutes les colonnes avec leur type PostgreSQL inféré, leur taux de nullité, et le marqueur `MIXED` si plusieurs types JSON ont été observés pour la même colonne.
