# Feature : Table name trimming par suppression de segments gauches

## Description
Quand un nom de table dépasse `PG_TABLE_MAX_IDENT` (53 chars), au lieu d'ajouter un hash immédiatement, on supprime progressivement les segments de gauche (séparés par `_`) jusqu'à ce que le nom tienne. En dernier recours seulement (même le minimum `parent_leaf` est trop long), on utilise un hash tout en conservant le segment parent direct dans le nom.

## Motivation
Les noms avec hash (`openfoodfact_production_nu_a1b2c3d`) sont illisibles. `production_nutiments_details` est immédiatement compréhensible et permet de retrouver la table sans documentation.

## Exemple
- Path : `[openfoodfact, production, nutiments, details]`
- Nom complet : `openfoodfact_production_nutiments_details` (trop long)
- Essai 1 : `production_nutiments_details` ✅ si ≤ 53
- Essai 2 : `nutiments_details` ✅ si ≤ 53
- Fallback hash : `nutiments_details_XXXXXXX` (option C — parent + leaf tronqués + hash)

## Modules / fichiers impactés
- `src/schema/naming.rs` — `truncate_to_limit` + nouvelle `truncate_table_name`

## Tâches de développement
1. Créer `truncate_table_name(sanitized: &str, original_key: &str, max_len: usize) -> String` avec la logique de suppression progressive par segments
2. Modifier `NamingRegistry::ensure_unique` pour appeler `truncate_table_name` au lieu de `truncate_to_limit`
3. Mettre à jour les tests existants impactés (`test_truncation_with_hash`, `test_long_table_name`, etc.) + ajouter des tests pour les nouveaux comportements
4. Vérifier que `truncate_to_pg_limit` (colonnes) reste inchangée

## Impacts et edge cases

### Risques de régression identifiés
1. **Panique — segment unique** (`naming.rs`, fallback) : si `original_key` n'a pas de `PATH_SEP` (table racine), `segments[-2]` n'existe pas → panic. Guard : `if segments.len() >= 2 { segments[len-2] } else { segments[0] }`.
2. **`leaf_budget` nul ou négatif** (`naming.rs`, fallback) : si parent ≥ 44 chars, le budget restant pour leaf est ≤ 0 → slice vide ou panic. Guard : `if leaf_budget == 0 { return format!("{parent_used}_{hash}") }`.
3. **UTF-8 byte boundary panic** (`naming.rs`, fallback) : `parent` et `leaf` sont des clés JSON brutes potentiellement multi-byte. Slicer au byte peut couper un char → panic. Utiliser `floor_char_boundary` ou chercher la dernière frontière valide avant le budget.
4. **Backward compatibility snapshots** (`persistence.rs`) : `TruncatedName.pg_name` en snapshot est hash-based ; après la feature le même path produit un nom différent. Divergence silencieuse si on recharge un vieux snapshot. À documenter comme breaking change.
5. **Commentaire `pg_truncate_name`** (`cascading/scoring.rs:148`) : le commentaire "matching the strategy used by NamingRegistry" devient faux. Mettre à jour le commentaire uniquement (la fonction n'a pas besoin de changer).

## Documentation
- Spec technique : `_bmad-output/feature-table-name-trimming-technical.md`
