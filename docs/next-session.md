# Prochaine session — état et tâches

## État au 2026-03-22

### Ce qui fonctionne
- Build release OK
- Pass 1 (inférence schéma) : fonctionne sur OpenFoodFacts 70GB → 216 tables avec le TOML de config
- Pass 2 (insertion) : démarre et écrit les données, mais arrêtée faute de temps

### Commande d'import complète

```bash
TMPDIR=/media/dylan/data/tmp mkdir -p /media/dylan/data/tmp && \
TMPDIR=/media/dylan/data/tmp ./target/release/json2sql \
  --input /media/dylan/data/openfoodfacts-products.jsonl \
  --db-url "postgres://dylan:dylan@localhost/lab_json" \
  --wide-column-threshold 50 --stable-threshold 0.10 --rare-threshold 0.001 \
  --schema-config openfoodfacts.toml --drop-existing --parallel 4
```

### Pourquoi TMPDIR sur /media/dylan/data
La Pass 2 écrit tous les COPY dans des fichiers temporaires avant de les envoyer à PostgreSQL.
`/tmp` est un tmpfs de 1.6GB — insuffisant. Le disque data a ~1TB libre.
Les fichiers temporaires peuvent grossir à 100GB+ sur OpenFoodFacts avant que les COPY démarrent.

---

## Amélioration prioritaire : streaming COPY

**Problème actuel :** `TempFileSink` accumule TOUTES les données en fichiers temporaires,
puis exécute les COPY seulement à la fin de la Pass 2. Résultat : ~100GB de temp files
pour OpenFoodFacts avant le premier insert en base.

**Solution à implémenter :** flush périodique par table.

Principe :
- Garder un compteur de lignes par `TempFileSink`
- Quand une table atteint N lignes (ex. 100 000), exécuter le COPY immédiatement,
  vider le fichier temp, continuer à accumuler
- Les COPY partiels sont possibles en mode append (pas de DROP entre les flushes)
- Garder un sémaphore pour le parallélisme (déjà en place)

Avantages :
- Les inserts sont visibles en BDD au fil de l'eau
- La taille des fichiers temporaires est bornée (~quelques GB max au lieu de 100GB+)
- Le disque data n'est plus nécessaire comme TMPDIR

Fichiers à modifier :
- `src/db/copy_sink.rs` : ajouter méthode `flush_if_needed(threshold)` sur `TempFileSink`
- `src/pass2/runner.rs` : appeler le flush après chaque `insert_object()` au niveau root

---

## Bugs corrigés dans cette session

| Bug | Fichier | Fix |
|---|---|---|
| `sources_fields` en double dans les schemas | `registry.rs` | Déduplication par nom PG après tri topologique |
| `ecoscore_extended_data_impact_recipes` > 1600 colonnes | `openfoodfacts.toml` | `ecoscore_extended_data` → jsonb |
| Root table lookup par nom PG au lieu du chemin JSON | `pass2/runner.rs` | `s.path.join(".") == root_table` |
| Erreur DB sans détail (`db error`) | `db/ddl.rs` + `error.rs` | `pg_err()` avec `as_db_error()` pour le message PG complet |

## Tables PostgreSQL présentes dans lab_json

Les 216 tables ont été créées (DDL exécuté), mais **aucune donnée insérée**.
Relancer avec `--drop-existing` pour repartir proprement.
