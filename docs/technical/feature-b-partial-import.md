# Technical Spec — Feature B: Partial Import (Sample Mode)

**Status:** Draft
**Date:** 2026-05-31
**PRD:** [_bmad-output/specs/prd-feature-b-inspect-sandbox.md](../../_bmad-output/specs/prd-feature-b-inspect-sandbox.md)
**Prerequisite:** Feature A (strategy flags) — `Pass1Config` struct, `StrategyName`

---

## Overview

Feature B adds an optional `--limit N` flag to pass 2. Pass 1 always runs on the complete file. Pass 2 stops dispatching root objects to workers after N have been sent. The limit is applied at the **dispatch level** (main thread, before workers) — no cross-worker coordination required, parallel correctness is trivial.

---

## Core Change — `src/pass2/runner.rs`

### Signature update

Add `limit: Option<u64>` to `pass2::runner::run()`:

```rust
pub async fn run(
    path: &Path,
    root_table: &str,
    schemas: &[TableSchema],
    client: &Client,
    pg_url: &str,
    pg_schema: &str,
    parallel: usize,
    anomaly_dir: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
    progress_tx: Option<ProgressTx>,
    per_worker_budget: Option<u64>,
    min_interim_copy_bytes: Option<u64>,
    limit: Option<u64>,          // ← NEW: None = no limit (full import)
) -> Result<Pass2Result>
```

> Note: Once Feature A's `Pass1Config` refactor lands, consider a parallel `Pass2Config` struct to bundle these parameters. For Feature B MVP, adding `limit` as a trailing parameter is the minimal change.

### Dispatch loop modification

The dispatch loop (`'dispatch`) already increments `rows_processed` after each send. Add a 3-line limit check immediately after:

```rust
'dispatch: while let Some(item) = reader.next_raw() {
    let bytes = item?;
    if senders[robin].send(bytes).await.is_err() {
        worker_died = true;
        break 'dispatch;
    }
    rows_processed += 1;
    // ← NEW: stop after N root objects dispatched
    if limit.map_or(false, |n| rows_processed >= n) {
        break 'dispatch;
    }
    robin = (robin + 1) % parallel;
    // ... progress reporting unchanged ...
}
drop(senders);  // signals workers: no more data
```

**Why dispatch-level?** The main thread owns the reader and distributes objects round-robin. Stopping here guarantees exactly N objects are dispatched, regardless of `parallel`. Workers drain their channels and complete normally — no cancellation, no special signaling needed.

**`limit = Some(0)`** — the loop body never executes (`rows_processed` starts at 0, the check fires before any send... wait: check is after `rows_processed += 1`). Actually with `Some(0)`, `rows_processed >= 0` is always true after the first increment — so exactly 0 objects are dispatched... no, wait.

Correction for `limit = Some(0)`: the check `rows_processed >= n` after the first increment gives `1 >= 0 = true` → breaks after dispatching 1 object. To correctly handle 0, add a pre-loop guard:

```rust
// Pre-loop guard for limit=0: no objects dispatched, tables still created.
if limit == Some(0) {
    drop(senders);
    // fall through to Phase B (COPY empty sinks) and constraint creation
} else {
    'dispatch: while let Some(item) = reader.next_raw() {
        // ... as above, with limit check after rows_processed += 1 ...
    }
    drop(senders);
}
```

---

## CLI — `src/cli.rs`

```rust
/// Limit pass 2 to the first N root objects. Pass 1 always runs on the full file.
/// 0 = create tables with no rows. Default: no limit (full import).
#[arg(long = "limit", value_name = "N")]
pub limit: Option<u64>,
```

Call site in `main.rs` (line ~371):

```rust
let pass2 = pass2::runner::run(
    &input_path,
    &root_table,
    &pass1.schemas,
    &client,
    db_url,
    &cli.schema,
    cli.parallel,
    cli.anomaly_dir.clone(),
    cli.temp_dir.clone(),
    None,
    None,
    None,
    cli.limit,     // ← NEW
).await?;
```

---

## UI — `json2sql-ui`

### `AppState` (`src/state.rs`)

```rust
pub struct AppState {
    // ... existing fields ...
    pub import_limit: Option<u64>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            // ... existing defaults ...
            import_limit: None,
        }
    }
}
```

### `ProjectConfig` (`src/config.rs`)

```rust
pub struct ProjectConfig {
    // ... existing fields ...
    #[serde(default)]
    pub import_limit: Option<u64>,
}
```

`apply_to`: `state.import_limit = self.import_limit;`
`from_project`: `import_limit: project.import_limit,`

### Setup Screen (`src/screens/setup.rs`) — Advanced accordion

Add a "Sample import" card in the Advanced step (Step 4), alongside the parallelism cards:

```
┌─ Sample import ──────────────────────────────────────┐
│  Limit import to  [_____]  root objects              │
│  (empty = full import)                               │
│  Pass 1 always runs on the full file.                │
└──────────────────────────────────────────────────────┘
```

- Input type: numeric text field, `Option<u64>` — empty string maps to `None`
- Invalid (non-numeric) input: reject inline, do not update state
- On change: write to `state.import_limit` + auto-save `ProjectConfig`

### Import Screen (`src/screens/import.rs`) — Sample mode badge

When `state.import_limit.is_some()`, render a badge in the Import screen header:

```rust
if let Some(n) = state.import_limit {
    rsx! { div { class: "badge badge-warning",
        "⚠ SAMPLE MODE — importing first {n} rows only"
    }}
}
```

Pass `state.import_limit` to the `pass2::runner::run()` call at line ~76 of `import.rs`.

---

## File Changeset

| File | Change |
|---|---|
| `src/cli.rs` | Add `limit: Option<u64>` field |
| `src/pass2/runner.rs` | Add `limit: Option<u64>` param; dispatch loop guard + break |
| `src/main.rs` | Pass `cli.limit` to `pass2::runner::run()` |
| `json2sql-ui/src/state.rs` | Add `import_limit: Option<u64>` to `AppState` |
| `json2sql-ui/src/config.rs` | Add `import_limit: Option<u64>` to `ProjectConfig` |
| `json2sql-ui/src/screens/setup.rs` | "Sample import" card in Advanced accordion |
| `json2sql-ui/src/screens/import.rs` | Sample mode badge; pass `import_limit` to runner |

---

## Test Plan

| Test | Location | Assertion |
|---|---|---|
| `test_limit_stops_after_n_objects` | `tests/integration/` | limit=N → exactly N root objects in DB |
| `test_limit_zero_creates_empty_tables` | `tests/integration/` | limit=0 → tables exist, 0 rows |
| `test_limit_exceeds_file_size` | `tests/integration/` | limit=999999 on 10-row file → 10 rows (full import) |
| `test_no_limit_identical_output` | `tests/integration/` | no limit → identical to pre-feature output |
| `test_limit_with_parallel` | `tests/integration/` | limit=N with parallel=4 → exactly N root objects, consistent with parallel=1 |
| `test_limit_composable_with_disable_strategy` | `tests/integration/` | `--disable-strategy sibling --limit 100` → correct schema + 100 rows |

---

## Architecture Note: Dispatch-Level vs Worker-Level Limit

Two approaches were considered:

| Approach | Pro | Con |
|---|---|---|
| **Dispatch-level** (chosen) | Single counter, no worker coordination, exact limit | Workers may process slightly more if channel has buffered items — mitigated by small `CHANNEL_CAP` |
| Worker-level atomic counter | Closer to "N rows inserted" | Cross-worker contention, more complex, limit counting is ambiguous with child table rows |

Dispatch-level is correct and sufficient. `CHANNEL_CAP = 256` means at most 256 extra items could be in flight when the break fires — acceptable for N >> 256. For very small N (e.g., N=1), the pre-loop `limit=0` guard pattern ensures correctness.

> For exact row counts (not object counts), the caller can sum `Pass2Result.rows_per_table` values.
