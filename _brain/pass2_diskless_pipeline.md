# Pass 2 — Pipeline sans disque (diskless)

**Date :** 2026-06-11  
**Contexte :** Discussion brain-party-mode — Fred, Alex, Wendy, Quinn  
**Problème :** 3 000 fichiers temp, 110 GB sur disque après Phase A, 3h pour Phase B → éliminer Phase B

---

## Problème actuel

| Métrique | Valeur |
|---|---|
| Fichiers temp générés | ~3 000 |
| Volume Phase B | ~110 GB |
| Durée streaming (Phase A) | 80 min pour 70 GB |
| Durée Phase B (COPY vers PG) | ~3h après streaming |
| Inflation JSON → COPY text | 70 GB → 110 GB (format plat, NULLs explicites) |

**Cause :** Les petites et moyennes tables ne passent pas par COPY direct (threshold trop haut) → elles s'accumulent en fichiers temp. Phase B relit 3 000 fichiers séquentiellement.

---

## Solution : pipeline full in-memory avec flusher concurrent

Principe : supprimer Phase B. Pendant le streaming, un **flusher dédié** maintient les buffers en RAM et flush vers PG en continu. Fin de streaming = tout est déjà dans PG.

### Architecture

```
Workers (N)
  │  RowBuilder.finalize() → Vec<u8> (COPY text pré-encodé)
  │  mpsc::Sender<(TableId, Bytes)>   ← channel unique, bounded
  ▼
Flusher thread
  │  HashMap<TableId, BytesMut>       ← buffers RAM par table
  │  BinaryHeap<(usize, TableId)>     ← tri par taille O(log n)
  │  AtomicUsize total_buffered       ← compteur global bytes
  │  sysinfo poll (1-2s)              ← backstop RAM système
  ▼
PostgreSQL COPY FROM STDIN
  │  une connexion dédiée par flush
  │  plusieurs COPY successifs pour la même table = OK (contraintes désactivées)
  ▼
Pass 2 terminé (plus de Phase B)
```

---

## Format Row : `Vec<u8>` pré-encodé

- Les workers encodent en COPY text (tab-séparé, `\N` pour NULL) **au moment du parse**, via `RowBuilder` existant.
- Le `Vec<u8>` résultant est envoyé directement dans le channel.
- Le flusher concatène dans `BytesMut` par table → `COPY FROM STDIN` = écriture directe des bytes. **Zéro ré-encodage.**
- Avantage mémoire : pas de `Vec<Option<String>>` (50 allocations heap par ligne de 50 colonnes).

---

## Gestion mémoire : double signal

### Signal 1 — compteur `BytesMut` (rapide, sans syscall)
- `AtomicUsize total_buffered` : workers incrémentent en envoyant, flusher décrémente après COPY.
- Le flusher surveille en continu : dès qu'une table dépasse `MEM_FLUSH_THRESHOLD` (ex. 64 MB), flush immédiat.

### Signal 2 — sysinfo (backstop système)
- Poll toutes les **1-2s** (non-critique au vu de la vitesse de streaming).
- Capture la pression RAM réelle incluant PG qui grossit son buffer cache pendant l'import.
- Déclenche le flag `pause_workers` si RAM système > `RAM_HIGH_WATERMARK` (ex. 85%).

### Hysteresis workers
- `Arc<AtomicBool> pause_workers` : flusher lève le flag à 85%, le clear à 70%.
- Workers checkent le flag **entre chaque record** : `if pause_flag.load(Relaxed) { spin_wait }`.
- Workers suspendus conservent leur état interne → reprise immédiate à l'identique.
- Pas besoin d'un polling serré côté workers : `load(Relaxed)` = 1 instruction CPU.

### Sélection de la table à flusher : `BinaryHeap`
- Le flusher maintient un max-heap par `BytesMut.len()`.
- "flush la plus grosse table" = O(log 3000) au lieu de O(3000).

---

## Comportement sur crash

Identique à l'état actuel : les fichiers temp ne sont pas récupérés. En mode diskless, une erreur fatale (PG drop, OOM) perd les données en RAM — même comportement accepté.

L'erreur PG pendant un flush doit être **immédiatement fatale et visible** (pas silencieuse).

---

## Fichiers à modifier

### `src/db/copy_sink.rs`
- **Supprimer** : `TempFileSink` (struct + `force_spill`, `take_flush_snapshot`, chemins disque)
- **Garder** : `RowBuilder` (inchangé — produit déjà des `Vec<u8>` COPY text)
- **Garder** : `COPY_NULL`, `COPY_DELIMITER` (constantes)
- **Créer** : `MemSink` — wrappeur `BytesMut` sans fichier temp
  ```rust
  pub struct MemSink {
      pub buf: BytesMut,
      pub row_count: u64,
      pub copy_sql: String,
  }
  ```
- **Supprimer** : `FlushSnapshot` (spécifique aux fichiers temp), `stream_snapshot_to_open_copy`, `copy_snapshot_to_pg`, `merge_copy_to_db`
- **Créer** : `fn flush_mem_sink_to_pg(buf: Bytes, copy_sql: &str, client: &Client) -> Result<u64>` (wrappeur simple COPY FROM STDIN)

### `src/pass2/sink.rs`
- `RowSink` trait : garder l'interface `write_row(&mut self, row: &[u8])`.
- Remplacer `impl RowSink for TempFileSink` par `impl RowSink for MemSink`.
- Remplacer `impl RowSink for Arc<Mutex<TempFileSink>>` par `impl RowSink for Arc<Mutex<MemSink>>`.

### `src/pass2/runner.rs`
- **Supprimer** : `TempFileSink`, `FlushSnapshot`, `InterimCopyHandle`, `CopyDirectHandle`
- **Supprimer** : `spawn_copy_direct_task`, `run_copy_direct_task` (remplacés par flusher)
- **Supprimer** : `trigger_budget_flush` côté worker (workers n'ont plus de décision de flush)
- **Supprimer** : `worker_teardown_flush` (workers envoient juste leurs derniers bytes)
- **Supprimer** : `phase_copy`, `distribute_sinks`, `copy_batch`, `unwrap_and_sort_sinks`
- **Modifier** : `run_worker` — au lieu de gérer des sinks locaux, les workers ont un `Sender<(TableId, Bytes)>` et envoient directement après chaque record ou batch.
- **Créer** : `run_flusher(rx, pg_url, pause_flag, progress_tx)` — tâche dédiée :
  - Reçoit `(TableId, Bytes)` en continu
  - Accumule dans `HashMap<TableId, BytesMut>` + met à jour `AtomicUsize`
  - À chaque réception : si table > `MEM_FLUSH_THRESHOLD` → flush immédiat
  - Poll sysinfo toutes ~1s via `Instant::elapsed()`
  - À la fin du channel (workers terminés) : flush toutes les tables restantes
- **Modifier** : `run()` — supprimer Phase B, lancer `run_flusher` en tâche concurrente pendant le streaming.
- **Modifier** : `WorkerConfig` — supprimer `worker_budget`, `interim_copy_threshold`, `min_spill_bytes`, `copy_direct_senders`, `copy_sem`. Ajouter `flush_tx: Sender<(TableId, Bytes)>`, `pause_flag: Arc<AtomicBool>`.

### `src/pass2/mod.rs`
- `Pass2Config` :
  - **Supprimer** : `temp_dir`, `per_worker_budget`, `min_interim_copy_bytes`, `large_table_threshold`, `copy_direct_channel_cap`, `min_spill_bytes`, `ram_usage_factor`, `min_budget_floor`
  - **Ajouter** : `mem_flush_threshold_bytes: Option<u64>` (seuil par table, défaut 64 MB), `ram_high_watermark: Option<f64>` (défaut 0.85), `ram_low_watermark: Option<f64>` (défaut 0.70)

---

## Paramètres à tuner (valeurs de départ suggérées)

| Paramètre | Défaut suggéré | Rôle |
|---|---|---|
| `MEM_FLUSH_THRESHOLD` | 64 MB | Flush une table quand son buffer dépasse ce seuil |
| `RAM_HIGH_WATERMARK` | 0.85 | Pause workers au-dessus de 85% RAM système |
| `RAM_LOW_WATERMARK` | 0.70 | Reprise workers sous 70% RAM système |
| Sysinfo poll interval | 1s | Fréquence check RAM système dans le flusher |
| Channel bound | 1024 | Backpressure naturel si flusher en retard |

---

## Ce qui ne change pas

- `src/pass2/insert.rs` — logique d'insertion JSON → COPY text : inchangée
- `src/pass2/traversal.rs` — parcours JSON : inchangé
- `src/pass2/coercer.rs` — coercions de types : inchangé
- `src/db/copy_text.rs` — `CopyEscaped` : inchangé
- `src/db/ddl.rs` — `add_constraints` : inchangé (Phase D garde la même structure)
- `src/anomaly/` — collecteur d'anomalies : inchangé
- `RowBuilder` dans `copy_sink.rs` : inchangé
- Tests existants sur `RowBuilder`, `classify_tables`, `compute_worker_budget` : à supprimer/adapter selon les structs supprimées
