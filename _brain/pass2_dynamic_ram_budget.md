# Pass 2 — Budget RAM dynamique par worker

## Contexte

Problème observé : 12 GB de fichiers temporaires pour 6.8 GB scannés avec 16 workers.
Cause : `PER_WORKER_FLUSH_THRESHOLD` hardcodé à 256 MiB × 16 workers = 4 GB alloués,
mais beaucoup trop de spills disque car le budget est atteint en permanence avec 251 tables.

Machine : 16 GB RAM, ~6 GB libres au lancement (VSCode + autres outils actifs).

## Décisions de design (validées en discussion)

- `sysinfo` est déjà dans les dépendances (`sysinfo = "0.39.2"`)
- `usage_factor = 0.4` (laisser 60% de RAM libre pour PG, OS, buffers)
- `MIN_BUDGET_FLOOR = 64 MiB` par worker — en dessous, warn + appliquer le minimum
- Le calcul est bypass si `Pass2Config.per_worker_budget` est fourni explicitement
- `MIN_SPILL_BYTES` doit être aligné sur `MIN_SINK_COPY_BYTES` (4 MiB) pour éviter tout spill disque pendant le scan

## Formule

```
available_ram     = sysinfo::System::new_all().available_memory()  // en bytes
raw_budget        = (available_ram * 0.4) / num_workers
per_worker_budget = max(raw_budget, MIN_BUDGET_FLOOR)

si raw_budget < MIN_BUDGET_FLOOR:
    WARN "RAM insuffisante pour {num_workers} workers — budget minimum appliqué ({MIN_BUDGET_FLOOR} MiB/worker)"
```

## Tâches à développer

### Tâche 1 — Changer `MIN_SPILL_BYTES` de 1 MiB → 4 MiB

Fichier : `src/pass2/runner.rs`
- `const MIN_SPILL_BYTES: u64 = 4 * 1024 * 1024;`
- Aligné sur `MIN_SINK_COPY_BYTES` : pendant le scan, on copie en PG ou on garde en RAM, jamais sur disque.
- Test : vérifier que le test existant `trigger_budget_flush_tiny_sink_stays_in_memory` passe toujours
  (il utilise `1 MiB` comme seuil dans le WorkerConfig — mettre `4 MiB` pour être cohérent).

### Tâche 2 — Calcul dynamique du budget au lancement

Fichier : `src/pass2/runner.rs` (fonction `run()`)

Ajouter une fonction `compute_worker_budget`:

```rust
const MIN_BUDGET_FLOOR: u64 = 64 * 1024 * 1024; // 64 MiB
const RAM_USAGE_FACTOR: f64 = 0.4;

fn compute_worker_budget(num_workers: usize, override_budget: Option<u64>) -> u64 {
    if let Some(b) = override_budget {
        return b;
    }
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let available = sys.available_memory(); // bytes
    let raw = ((available as f64 * RAM_USAGE_FACTOR) / num_workers as f64) as u64;
    if raw < MIN_BUDGET_FLOOR {
        eprintln!(
            "WARNING: available RAM too low for {num_workers} workers — \
             applying minimum budget ({} MiB/worker)",
            MIN_BUDGET_FLOOR / 1024 / 1024
        );
        MIN_BUDGET_FLOOR
    } else {
        raw
    }
}
```

Remplacer dans `run()` :
```rust
// Avant :
let worker_budget = config.per_worker_budget.unwrap_or(PER_WORKER_FLUSH_THRESHOLD);
// Après :
let worker_budget = compute_worker_budget(config.parallel, config.per_worker_budget);
```

Tests :
- `compute_worker_budget_override_bypasses_calculation` — `Some(X)` retourne X
- `compute_worker_budget_floors_at_minimum` — si RAM simulée trop basse, retourne `MIN_BUDGET_FLOOR`
  (mock difficile avec sysinfo ; tester via la logique `max(raw, floor)` isolée)

### Tâche 3 — Exposer `MIN_BUDGET_FLOOR` et `RAM_USAGE_FACTOR` dans `Pass2Config`

Optionnel mais cohérent avec le pattern existant (`per_worker_budget`, `min_interim_copy_bytes`, etc.)

Champs à ajouter :
```rust
/// Override the RAM usage factor (0.0–1.0) for dynamic budget calculation. None = 0.4.
pub ram_usage_factor: Option<f64>,
/// Override the minimum per-worker budget floor. None = MIN_BUDGET_FLOOR (64 MiB).
pub min_budget_floor: Option<u64>,
```

Tous les call sites : ajouter `ram_usage_factor: None, min_budget_floor: None`.

## Notes d'implémentation

- `sysinfo::System::refresh_memory()` est l'appel minimal (pas besoin de `new_all()`)
- `available_memory()` retourne des bytes dans sysinfo >= 0.30
- Le warning doit aller sur `progress_tx` si disponible, sinon `eprintln!`
- `PER_WORKER_FLUSH_THRESHOLD` reste comme fallback documenté mais n'est plus utilisé par défaut

## Fichiers impactés

- `src/pass2/runner.rs` — constantes, `Pass2Config`, `run()`, nouvelle fn `compute_worker_budget`
- `src/pipeline.rs` — construction `Pass2Config`
- `json2sql-ui/src/screens/import.rs` — construction `Pass2Config`
- `tests/common/mod.rs` — construction `Pass2Config`
- `tests/integration_*.rs` — constructions `Pass2Config`
