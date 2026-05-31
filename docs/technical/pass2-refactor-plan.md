# Pass 2 — Plan de refactoring (streaming → COPY séparé)

_Rédigé après analyse en session du 2026-05-29 sur le dataset OpenFoodFacts (70 GB, 245 tables, 16 workers)._

---

## Problème observé

Après 12 GB scannés sur 70 GB, le répertoire `/tmp` contenait **24 015 fichiers** (~22 GB).
Après mesure suivante : **36 237 fichiers** (~34 GB). Taux : ~1 019 fichiers/GB scanné.
Projection fin de run : ~71 000 fichiers, ~68 GB de données temporaires.

Distribution des tailles de fichiers :

```
p10 → p99 : 1 048 581 … 1 069 967 bytes (~1 MiB pile)
```

**Tous les fichiers font exactement `MIN_SINK_HANDOFF_BYTES = 1 MiB`.**

---

## Root cause

Le pipeline actuel tente de pipeliner le streaming et les COPYs PostgreSQL via un `flush_task` qui reçoit des sinks des workers et les dispatche à des connexions PG parallèles.

Le problème est structurel : le `flush_task` attend d'avoir accumulé
`flush_dispatch_threshold = parallel × MIN_SINK_HANDOFF_BYTES = 16 × 1 MiB = 16 MiB`
par table avant de déclencher un COPY. Avec 245 tables et seulement 16 connexions PG :

- Les workers créent des sinks de 1 MiB à raison de ~1 019 sinks/GB scanné
- Le `flush_task` accumule des sinks plus vite que les 16 connexions PG ne peuvent les consommer
- `flush_rx` est **unbounded** → aucune backpressure → accumulation sans borne

Le faux pipelining ajoute toute la complexité sans délivrer la promesse de latence.

---

## Architecture retenue : séparation franche streaming / COPY

```
Phase A — Streaming (aucune connexion PG)
  N workers, chacun avec 1 TempFileSink par table
  Accumulent TOUTES les données sur disque
  Pas de flush intermédiaire, pas de handoff

Phase B — COPY post-streaming
  Pour chaque table : collecter les N sinks workers
  merge_copy_to_db (streaming par chunks) → COPY vers PG
  Suppression automatique des fichiers via TempFilePath::drop
  Paralléliser sur `parallel` connexions PG
```

**Trade-off accepté** : le premier INSERT dans PG arrive après la fin du streaming
(~30–40 min de plus pour un fichier de 70 GB). Acceptable pour un batch import.

**Disk usage** : identique (~68 GB peak), mais en **~3 920 fichiers** (245 × 16)
au lieu de ~71 000. Même espace, filesystem sain, performance COPY améliorée.

**Sur les contraintes FK/PK** : les UUIDs sont générés pendant le streaming (pas d'auto-increment).
Les contraintes sont ajoutées après tous les COPYs. Aucun ordre de table requis.

---

## Tâches

### T1 — `--temp-dir` CLI + propagation dans TempFileSink

- Ajouter `Option<&Path>` à `TempFileSink::new()` → `NamedTempFile::new_in(dir)`
- Défaut : `std::env::temp_dir()` (comportement actuel)
- Ajouter `--temp-dir <DIR>` dans `cli.rs`
- Propager dans `pass2::runner::run()` → tous les workers

**Fichiers** : `src/db/copy_sink.rs`, `src/cli.rs`, `src/pass2/runner.rs`

---

### T2 — Streaming COPY dans `merge_copy_to_db`

Actuellement `tokio::fs::read()` charge le fichier entier en mémoire avant de l'envoyer à PG.
Pour une table dense (ex. `products` : ~1 M lignes × 500 B = 500 MB), c'est un OOM garanti.

Remplacer par lecture en chunks :

```rust
// Avant (dangereux)
let file_data = tokio::fs::read(&guard.0).await?;

// Après (streaming)
let mut file = tokio::fs::File::open(&guard.0).await?;
let mut buf = vec![0u8; 4 * 1024 * 1024]; // 4 MiB chunks
loop {
    let n = file.read(&mut buf).await?;
    if n == 0 { break; }
    pinned.send(Bytes::copy_from_slice(&buf[..n])).await?;
}
```

**Fichiers** : `src/db/copy_sink.rs`

---

### T3 — Simplifier le FD management dans `TempFileSink`

Sans drain cycle, un worker peut avoir jusqu'à 245 FDs ouverts simultanément
si tous les sinks auto-spillent. Avec 16 workers : 3 920 FDs → dépasse `ulimit -n 1024`.

Solution : auto-hiberner après chaque `spill()`. Le FD est ouvert seulement pendant le `write_all()`.

```rust
fn spill(&mut self) -> Result<()> {
    if self.pending.is_empty() { return Ok(()); }
    let data = std::mem::take(&mut self.pending);
    let file = self.ensure_file()?;
    file.write_all(&data).map_err(J2sError::Io)?;
    self.writer = None; // hiberner immédiatement
    Ok(())
}
```

Conséquence : `is_open()` retourne toujours `false` après un `spill()`.
`hibernate()` reste disponible mais devient un no-op dans la plupart des cas.
Supprimer `global_open_fds`, `FD_GLOBAL_THRESHOLD`, `global_sub()` du runner.

**Fichiers** : `src/db/copy_sink.rs`, `src/pass2/runner.rs`

---

### T4 — Supprimer l'infrastructure flush-during-streaming

Éléments à supprimer entièrement de `src/pass2/runner.rs` :

| Élément | Où |
|---|---|
| `flush_tx`, `flush_rx`, `flush_task` | runner.rs |
| Drain cycle (`else if total_bytes > per_worker_flush_threshold`) | runner.rs (workers) |
| `per_worker_flush_threshold`, `INTERIM_FLUSH_THRESHOLD` | runner.rs, copy_sink.rs |
| `flush_dispatch_threshold`, `MIN_SINK_HANDOFF_BYTES` | runner.rs, copy_sink.rs |
| `sink_eligible_for_handoff()` | runner.rs |
| `conn_senders`, `conn_handles` (pool streaming) | runner.rs |
| `global_open_fds`, `FD_GLOBAL_THRESHOLD`, `global_sub()` | runner.rs |
| `bytes_on_disk` (champ + usages) | copy_sink.rs |
| `memory_pressure` + RAM pressure check (drain cycle) | runner.rs |

Le `memory_pressure` flag est lié au drain cycle ; sans celui-ci, la pression RAM
est gérée naturellement : `pending` max = 256 KB × 245 × 16 ≈ 900 MB total.

**Fichiers** : `src/db/copy_sink.rs`, `src/pass2/runner.rs`

---

### T5 — Phase COPY post-streaming

Après join de tous les workers, remplacer l'actuelle attente du `flush_task` par :

```
1. Grouper les sinks par table_name (depuis tous les workers)
2. Pour chaque table avec rows > 0 :
   a. Envoyer les N sinks (un par worker) vers un conn worker disponible
   b. conn worker : appelle merge_copy_to_db (streaming, T2)
   c. TempFilePath::drop supprime automatiquement les fichiers
3. Paralléliser sur `parallel` connexions PG (même pool que l'actuel)
4. Collecter rows_per_table depuis les résultats
```

Supprimer l'actuel `result_tx`/`result_rx` inter-tasks ; utiliser `JoinHandle<Result<(String, u64)>>` directement.

**Fichiers** : `src/pass2/runner.rs`

---

## Tests à mettre à jour

**À supprimer** (tests de l'infrastructure supprimée) :
- `drain_filter_not_triggered_before_spill`
- `drain_filter_not_triggered_below_threshold`
- `drain_filter_triggered_at_threshold`
- `drain_filter_large_bytes_buffered_insufficient_without_enough_spill`
- `per_worker_flush_threshold_never_zero`
- `bytes_on_disk_*` (4 tests)

**À adapter** :
- `is_open_false_after_hibernate` → vérifier que spill() auto-hiberne
- `force_spill_then_hibernate_releases_fd` → fd déjà fermé après spill

**À ajouter** :
- Test streaming COPY : fichier > 4 MiB envoyé en chunks (sans PG, vérifier les bytes envoyés)
- Test temp-dir : `TempFileSink::new()` avec dir custom crée le fichier dans ce répertoire

---

## Ce qui ne change pas

- `TempFileSink` : structure, `pending`, `write_row()`, `force_spill()`, `TempFilePath::drop`
- `merge_copy_to_db` : signature externe (seul le corps change, T2)
- `pass2::runner::run()` : signature externe (ajout de `temp_dir: Option<PathBuf>`)
- Phase D (contraintes PK/FK) : inchangée
- Anomaly collection : inchangée
- `RowBuilder`, `CopyEscaped`, `COPY_NULL` : inchangés
