# Feature : Correctifs code review — 10 findings

## Description
Corriger les 10 findings du code review post-commit e3f6fad, regroupés en 3 groupes par fichier cible.
Bugs de production actifs (OOM Phase D, régression durabilité WAL, load-balancing cassé), fuites silencieuses de fichiers temporaires, et nettoyages d'idiomes Rust.

## Motivation
Le commit e3f6fad a introduit des régressions actives : le SET maintenance_work_mem cause des OOM avec high parallelism, synchronous_commit = off sur les connexions contraintes est une régression de durabilité silencieuse, et distribute_sinks utilise row_count au lieu de bytes_buffered, cassant le load-balancing pour les tables à lignes larges.

## Modules / fichiers impactés
- `src/db/ddl.rs`
- `src/pass2/runner.rs`
- `src/db/copy_sink.rs`

## Tâches de développement

### Groupe 1 — src/db/ddl.rs
1. Retirer `SET synchronous_commit = off` des session SQLs des connexions contraintes (finding #2 — régression durabilité WAL)
2. Supprimer `SET maintenance_work_mem = '1GB'` des connexions contraintes (finding #1 — OOM avec high parallelism)
3. Transformer `constraint_session_sqls()` en `const` ou `static` (finding #10 — idiome)

### Groupe 2 — src/pass2/runner.rs
4. Corriger `distribute_sinks` : utiliser `bytes_buffered` au lieu de `row_count` pour le bin-packing (finding #3 — load-balancing cassé)
5. Remplacer `unwrap_or(0)` par `.expect("parallel >= 1 enforced by validate_run_params")` (finding #9 — idiome)

### Groupe 3 — src/db/copy_sink.rs
6. Logger les erreurs dans `cleanup_spill_file` avec `eprintln!` (finding #4 — erreurs silencieuses)
7. Réécrire le test orphelin `stream_file_reads_all_bytes_in_chunks` pour appeler `stream_file_chunks` (finding #5 — faux positif de couverture)
8. Allouer le buffer `BytesMut` hors de la boucle dans `stream_file_chunks` (finding #6 — allocations répétées)
9. Ajouter contexte d'erreur `NotFound` dans `stream_file_chunks` (finding #7 — TOCTOU)
10. `let _ = std::mem::take(&mut self.pending)` → `std::mem::take(&mut self.pending);` (finding #8 — idiome)

## Impacts et edge cases

### Risques de régression identifiés
1. **`ddl.rs:571` — Test `constraint_session_sqls_returns_set_commands` cassé** — asserte `sqls.len()==2` et le contenu exact du tableau. Doit être supprimé lors de la suppression de la fonction.
2. **`ddl.rs:218` — Loop morte / fonction morte** — si les deux SETs sont retirés, supprimer `constraint_session_sqls()` ET la boucle entièrement plutôt que laisser un tableau vide.
3. **`ddl.rs:193` — Commentaire doc périmé** — le commentaire référence "1 GB for maintenance_work_mem" après la suppression. À supprimer avec la fonction.
4. **`runner.rs:1028` — Test `distribute_sinks_uses_row_count_not_total_flushed_as_weight` cassé** — teste explicitement le comportement `row_count`. Doit être réécrit pour valider le comportement `bytes_buffered`.
5. **`runner.rs:982` — Helper `make_sink_with_rows` incomplet** — ne set que `row_count`, laisse `bytes_buffered=0`. Les tests `distribute_sinks_greedy_isolates_heaviest_table` et `distribute_sinks_single_connection` doivent être mis à jour ; ajouter un helper `make_sink_with_bytes` ou étendre l'existant.
6. **`runner.rs:322` — Edge case `bytes_buffered=0` après snapshot** — `unwrap_and_sort_sinks` filtre déjà `row_count==0`, donc les sinks entièrement traités en Phase A sont exclus. Les sinks retenus en Phase B ont `bytes_buffered > 0` par construction. Aucun garde supplémentaire nécessaire.
7. **`copy_sink.rs:689` — Test orphelin** — supprimer `stream_file_reads_all_bytes_in_chunks` ; `stream_file_chunks_reads_all_bytes_via_bytesmut` (ligne 884) couvre déjà le pattern BytesMut.
8. **`ddl.rs:214` — Bug caché RDS/Supabase** — le `SET synchronous_commit = off` actuel sur les connexions contraintes utilise `?` (fatal), contrairement à `try_set_synchronous_commit_off` en Phase A (non-fatal). La suppression du SET corrige ce bug caché sans garde supplémentaire.

## Documentation
- Spec technique : `_bmad-output/feature-code-review-10-findings-technical.md`
