# Feature : Pass 2 — COPY direct pour grandes tables (bypass disque)

## Description
Pour les tables dépassant un seuil configurable (en rows estimés ou MB), envoyer les données directement via `COPY FROM STDIN` vers PostgreSQL sans passer par un fichier temporaire CSV, en utilisant un channel borné Rust pour assurer le backpressure naturel. Les petites tables conservent le comportement actuel (fichier CSV → COPY).

## Motivation
Avec un fichier source 70 GB / 251 tables, les fichiers temporaires s'accumulent à 109 GB sur SSD (producteur JSON plus rapide que le consommateur BDD), alors que la BDD cible est sur HDD. Le COPY direct élimine ce tampon non-borné, réduit la latence d'insertion et supprime la double sérialisation (CSV disque → relecture COPY).

## Modules / fichiers impactés
- `src/pass2/` — logique principale phase B COPY (workers, scheduling inserts)
- Worker pool et dispatch grandes tables vs petites tables
- Configuration Pass 2 (`parallelism`, nouveau `large_table_threshold`)
- Gestion semaphore connexions PostgreSQL

## Tâches de développement
1. Définir `large_table_threshold` (rows estimés ou bytes) dans la config Pass 2, configurable via CLI/UI
2. Pour les grandes tables : créer un channel borné `(tx, rx)` + task dédiée COPY STDIN via tokio-postgres
3. Les workers envoient les lignes dans `tx` au lieu d'écrire sur disque — backpressure automatique si la BDD est lente
4. Borner les COPY directs simultanés via semaphore partagé (réutiliser `parallelism`)
5. Petites tables : comportement fichier actuel inchangé
6. Vérifier que les fichiers temporaires sont bien supprimés dès la fin du COPY (comportement déjà présent)
7. Benchmarks : comparer temps total et pic disque avant/après sur le dataset OpenFoodFacts 70 GB

## Impacts et edge cases

### Risques de régression identifiés
1. **`RowSink::write_row` synchrone** (`pass2/sink.rs:14`) — le trait est sync ; un channel sender ne peut pas l'implémenter sans bloquer le thread Tokio async. Soit le trait devient async (refactor ~500 LOC), soit les grandes tables utilisent un `HashMap<String, Sender>` parallèle, contournant `RowSink` entièrement.
2. **`Arc::try_unwrap` race** (`pass2/runner.rs:251`) — si un handle COPY-direct tient encore un clone de l'`Arc` quand `phase_copy` appelle `try_unwrap`, l'import échoue. Tous les `JoinHandle` COPY-direct doivent être inclus dans `all_copy_handles` de `join_phase_a` avant que `phase_copy` démarre.
3. **Classification grande/petite table doit être globale** (`pass2/runner.rs:560`) — la décision doit être prise une seule fois avant `spawn_pass2_workers`, pas par worker individuellement. Sinon des workers peuvent écrire la même table dans des destinations différentes (fichier + channel).
4. **Propagation `large_table_threshold`** (`pass2/runner.rs:84`) — nouveau champ à ajouter dans `Pass2Config`, `PipelineConfig`, `pipeline.rs::run_pass2`, CLI, et l'UI Dioxus (`json2sql-ui`).
5. **`try_set_synchronous_commit_off` manquant** (`pass2/runner.rs:130`) — les connexions ouvertes par les tasks COPY-direct doivent appeler cette fonction, comme le font déjà les interim copies.
6. **`trigger_budget_flush` incompatible avec sinks channel** (`pass2/runner.rs:146`) — le flush budget vérifie `bytes_buffered` sur `TempFileSink` ; les tables channel-backed n'ont pas de sink fichier à flusher. Il faut ignorer ces tables dans la boucle de flush.
7. **Comptage `rows_per_table` — grandes tables absentes** (`pass2/runner.rs:279`) — les lignes des tasks COPY-direct doivent remonter via `interim_rows` (pattern `(table_name, rows)` dans `all_copy_handles`), sinon `Pass2Result.rows_per_table` affiche 0 pour les grandes tables.
8. **Backpressure du channel COPY-direct** (`pass2/runner.rs:676`) — la capacité du channel borne la mémoire en transit. Trop petite → workers bloquent et throughput effondré. Trop grande → spike mémoire. Doit être calibrée ou exposée en config.

## Documentation
- Spec technique : `_bmad-output/feature-pass2-copy-direct-large-tables-technical.md`
