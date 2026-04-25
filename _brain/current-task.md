# Tâches en cours — json2sql

_Mis à jour automatiquement en fin de session._

## Prochaines tâches

- IHM : bouton "Précédent" sur l'écran Strategy
- Findings adversariaux mineurs restants : #6 (timing test fragile), #9 (static assert InferredType::ALL.len), #11 (sibling[0] non-déterministe dans large-group Jaccard)
- Tester Strategy sur des fichiers complexes (wide tables, dynamic keys, pivot)
- Tester import à grande échelle avec OpenFoodFacts + `--anomaly-dir`
- IHM : compteur d'anomalies par table dans Strategy/Preview (nécessite extension du protocole `ProgressEvent` — ajouter `Pass2AnomalyUpdate { table, count }`)
- IHM : double lancement runner si remontage rapide d'écran (#9 — partiellement mitigé par reset progress, mais `use_coroutine` peut encore lancer deux instances si Dioxus remonte le composant ; solution propre = guard `use_signal` + `once` flag)

## Backlog (non urgent)

- **SSL/TLS pour connexions PG distantes** : actuellement `NoTls` hardcodé dans `tokio-postgres`. À implémenter si usage cloud (RDS, Supabase, Neon, etc.). Nécessite d'activer `tokio-postgres` avec feature `native-tls` ou `openssl` + dépendance système (`libssl-dev`). Checkbox "Require SSL" dans Setup, propagée au connect dans import.rs et setup.rs.
- Picker de fichier lent (xdg-portal) : installer `libgtk-3-dev` sur le host puis `features = ["tokio", "gtk3"]` dans rfd

## Ce qui est livré (session 2026-04-25)

- **Perfs Pass 1** : pangenomegraph 23MB 2-3 min → 5-6 sec
  - O(N²) Jaccard → O(N) fast paths (pure-container + large-group)
  - TypeTracker `[u64; 12]` au lieu de IndexMap
  - naming dot-key sans recompute, `ensure_table_key` entry()
  - ObjectArray clone N → N-1
- **Fix j2s_data JSONB** : `is_generated: true` + check `data_col_name` avant `is_generated` en pass2
- **Fix '.' dans les noms de champs JSON** : depth correct, faux commentaire supprimé
- **Fixes adversariaux** : #1 col_sets avant fast path, #2 clone() supprimé, #3 entry(), #7 pop/push, #8 is_generated
- 142 tests passés

## Ce qui est livré (session 2026-04-12)

- 5 écrans Dioxus desktop complets et fonctionnels
- Fixes webkit : inputs éditables (`-webkit-text-fill-color`), focus JS, checkbox restaurée
- Sécurité : host encodé dans URL PG, timeouts 5s/10s sur connect, zeroize sur password
- UX : labels champs PG, compteurs "Detecting…" pendant analyse, port 0 validation, anomaly-dir picker, schéma PG configurable, taille fichier affichée + warning > 5 GB
- Reset progress au remontage écran (mitigation #9)
