# Refactoring SOLID — json2sql

> Document de référence pour le refactoring architectural.  
> Mis à jour au fil du développement. Priorité : compréhension du code + séparation claire des responsabilités.

---

## Diagnostic — Violations actuelles

| Fichier | Lignes | Violations SOLID |
|---|---|---|
| `src/schema/registry.rs` | 3 416 | SRP (5 responsabilités), ISP (expose tout aux consommateurs) |
| `src/pass2/insert.rs` | 1 009 | SRP (traversal + coercion + dispatch mélangés), DIP (TempFileSink câblé en dur) |
| `src/pass2/runner.rs` | 701 | SRP mineur (orchestration + gestion FD) |
| `src/pass1/runner.rs` | 457 | Orchestrateur pur — OK, mais impacté par le split de SchemaRegistry |
| `src/main.rs` | 461 | appelle `schema::registry::exclude_absorbed_children` directement (fuite d'impl) |

---

## Architecture cible

```
src/
├── schema/
│   ├── mod.rs              — re-exports publics, constante PATH_SEP
│   ├── table_schema.rs     — types de données (TableSchema, ColumnSchema, ChildKind…) [inchangé]
│   ├── type_tracker.rs     — inférence de type par colonne [inchangé]
│   ├── naming.rs           — truncation noms PG, collisions [inchangé]
│   ├── config.rs           — overrides utilisateur [inchangé]
│   ├── stats.rs            — ColumnStats [inchangé]
│   ├── suffix_detector.rs  — détection de suffixes [inchangé]
│   │
│   ├── observer.rs         — [T1] SchemaObserver : observation row-by-row
│   │                          observe_root(), merge(), TableEntry (privé)
│   │
│   ├── finalizer.rs        — [T2] SchemaFinalizer : transformations post-stream
│   │                          finalize(), build_entry_schema(), type_histogram(),
│   │                          apply_column_limit_guard(), exclude_absorbed_children(),
│   │                          PG_MAX_COLUMNS, OverflowWarning
│   │
│   ├── cascading.rs        — [T8] algorithme de détection de frères (sibling detection)
│   │                          finalize_cascading(), child_compatibility_score(),
│   │                          pairwise_jaccard_min(), build_parent_child_maps(),
│   │                          run_sibling_wave(), process_co_sibling_group()
│   │
│   ├── wide_strategies.rs  — [T9] fonctions d'application des stratégies wide
│   │                          apply_wide_strategy_columns(), apply_structured_pivot_columns(),
│   │                          apply_flatten(), apply_jsonb_flatten(),
│   │                          apply_normalize_dynamic_keys(),
│   │                          build_union_columns(), classify_key_shape(), suggest_wide_strategy()
│   │
│   ├── reporter.rs         — [T3] SchemaReporter : lecture des résultats d'observation
│   │                          collect_stats(), anomaly_iter(), truncated_names(),
│   │                          column_collisions()
│   │
│   └── registry.rs         — [RÉDUIT] SchemaRegistry façade (~100 lignes prod)
│                              new() + délègue à Observer / Finalizer / Reporter
│
├── pass1/
│   ├── mod.rs              — re-exports [inchangé]
│   └── runner.rs           — [ADAPTÉ] orchestration Pass 1
│                              instancie SchemaObserver + SchemaFinalizer
│                              au lieu de SchemaRegistry monolithique
│
└── pass2/
    ├── mod.rs              — re-exports [inchangé]
    ├── coercer.rs          — coerce() [déjà isolé ✓]
    ├── runner.rs           — orchestration pass2 [inchangé structurellement]
    │
    ├── sink.rs             — [T4] trait RowSink + implémentation TempFileSink
    │                          trait RowSink { write_row(&mut self, row: Vec<u8>) -> Result<()>; }
    │                          TempFileSink, RowBuilder, TempFilePath (déplacés depuis insert.rs)
    │
    ├── traversal.rs        — [T5] toutes les fn insert_* privées
    │                          insert_pivot_object(), insert_jsonb_object(),
    │                          insert_structured_pivot_object(), dispatch_child_routes(),
    │                          insert_keyed_pivot_object(), insert_keyed_pivot_array_of_objects(),
    │                          insert_normalize_dynamic_keys(), insert_multi_keyed_pivot(),
    │                          insert_array()
    │
    └── insert.rs           — [RÉDUIT] insert_object() uniquement (~50 lignes d'orchestration)
                               délègue à traversal.rs, dépend du trait RowSink (pas TempFileSink)
```

**Architecture retenue : monolithique modulaire** — pas de microservices, c'est une lib/CLI. On découpe en modules Rust avec des responsabilités claires, pas en binaires séparés.

### Graphe de dépendances final (schema/)

```
registry   →  observer, finalizer, reporter
finalizer  →  cascading, wide_strategies
cascading  →  wide_strategies
wide_strategies → finalizer (exclude_absorbed_children seulement)
config     →  wide_strategies
```

---

## Flux de données (inchangé, rendu explicite)

```
JSON file
   │
   ▼  pass1/runner.rs
SchemaObserver::observe_root()   ← [T1] était SchemaRegistry::observe_root()
   │
   ▼
SchemaFinalizer::finalize()      ← [T2] était SchemaRegistry::finalize()
   │
   ▼
Vec<TableSchema>  ──────────────────────────────────────────────────────────┐
   │ (via Pass1Result)                                                       │
   ▼  pass2/runner.rs                                                        │
HashMap<String, TableSchema>                                                 │
   │                                                                         │
   ▼  pass2/insert.rs (insert_object)                                        │
traversal.rs (insert_pivot_object, …)  ← [T5] extrait de insert.rs          │
   │                                                                         │
   ▼                                                                         │
RowSink::write_row()            ← [T4] trait — TempFileSink est une impl    │
   │                                                                         │
   ▼                                                                         │
COPY → PostgreSQL ◄─────────────────────────────────────────────────────────┘
```

---

## Invariants à préserver

Ces tests doivent passer sans modification après chaque tâche :

- `tests/integration_schema.rs` — inférence de types, nommage PG
- `tests/integration_overrides.rs` — overrides de type (11 cas)
- `tests/integration_anomalies.rs` — collecte d'anomalies
- `tests/integration_strategies.rs` — wide/pivot/flatten/sibling strategies

Commande de vérification :
```
cargo test --test integration_schema --test integration_overrides --test integration_anomalies --test integration_strategies
```

---

## Tâches

### T0 — REFACTORING.md (ce document) ✅
Créer la carte de navigation avant de toucher au code.

---

### T1 — Extraire `SchemaObserver` dans `src/schema/observer.rs`

**Responsabilité :** observation mutable row-by-row, pendant le streaming JSON.

**Ce qui bouge :**
- `struct TableEntry` (privée) + tout son `impl` (observe_field, observe_scalar_element…)
- `SchemaRegistry::observe_root()`
- `SchemaRegistry::merge()`

**Types introduits :** `SchemaObserver` (struct publique wrappant `IndexMap<String, TableEntry>`)

**Interface résultante :**
```rust
impl SchemaObserver {
    pub fn new(text_threshold: u32, array_as_pg_array: bool, ...) -> Self
    pub fn observe_root(&mut self, root_name: &str, obj: &Map<String, Value>)
    pub fn merge(&mut self, other: SchemaObserver)
}
```

**Impact `pass1/runner.rs` :** instancie `SchemaObserver` au lieu de `SchemaRegistry` (pour la phase d'observation). La phase de finalisation reste via `SchemaRegistry` pour l'instant (sera nettoyée en T2).

**`registry.rs` après :** délègue `observe_root` / `merge` à `self.observer: SchemaObserver`.

**Critère de done :** `cargo test` passe, `registry.rs` réduit d'au moins 300 lignes.

---

### T2 — Extraire `SchemaFinalizer` dans `src/schema/finalizer.rs`

**Responsabilité :** transformations post-stream sur `Vec<TableSchema>`. Aucun état mutable pendant le streaming.

**Ce qui bouge (fonctions libres actuelles dans `registry.rs`) :**
- `apply_column_limit_guard()`
- `exclude_absorbed_children()`
- `apply_wide_strategy_columns()`
- `apply_structured_pivot_columns()`
- `apply_normalize_dynamic_keys()`
- `apply_flatten()`
- `apply_jsonb_flatten()`
- `build_union_columns()`
- `classify_key_shape()`
- `child_compatibility_score()`
- `finalize_cascading()` + helpers (`build_parent_child_maps`, `run_sibling_wave`, `process_co_sibling_group`…)
- `SchemaRegistry::finalize()` → devient `SchemaFinalizer::finalize(observer: SchemaObserver) -> Vec<TableSchema>`

**Interface résultante :**
```rust
pub struct SchemaFinalizer { /* wide_threshold, sibling_threshold, jaccard… */ }
impl SchemaFinalizer {
    pub fn new(wide_column_threshold: usize, sibling_threshold: usize, ...) -> Self
    pub fn finalize(&self, observer: SchemaObserver) -> Vec<TableSchema>
}
```

**Impact `pass1/runner.rs` :** après T2, `pass1/runner.rs` instancie `SchemaObserver` + `SchemaFinalizer` explicitement — le `SchemaRegistry` n'apparaît plus dans pass1.

**Impact `main.rs` :** `schema::registry::exclude_absorbed_children` → `schema::finalizer::exclude_absorbed_children`.

**Critère de done :** `registry.rs` < 200 lignes, `cargo test` passe.

---

### T3 — Extraire `SchemaReporter` dans `src/schema/reporter.rs`

**Responsabilité :** lecture des résultats après observation (stats, anomalies, noms tronqués).

**Ce qui bouge :**
- `SchemaRegistry::collect_stats()`
- `SchemaRegistry::anomaly_iter()`
- `SchemaRegistry::truncated_names()`
- `SchemaRegistry::column_collisions()`
- Helper `type_histogram()`

**Note :** ces méthodes lisent l'état de `SchemaObserver`. `SchemaReporter` peut être un ensemble de fonctions libres prenant `&SchemaObserver`, ou un wrapper léger — à décider pendant l'implémentation selon ce qui est le plus lisible.

**Critère de done :** `registry.rs` devient une pure façade de ~100 lignes, `cargo test` passe.

---

### T4 — Extraire trait `RowSink` + déplacer `TempFileSink` dans `src/pass2/sink.rs`

**Responsabilité :** découpler `insert_object` du stockage concret.

**Ce qui bouge (depuis `insert.rs`) :**
- `struct RowBuilder` + impl
- `struct TempFileSink` + impl
- `struct TempFilePath` + impl Drop

**Trait introduit :**
```rust
pub trait RowSink {
    fn write_row(&mut self, row: Vec<u8>) -> Result<()>;
    fn table_name(&self) -> &str;
}
impl RowSink for TempFileSink { … }
```

**Bénéfice :** `insert_object` devient testable sans filesystem (impl `RowSink` in-memory pour les tests).

**Critère de done :** `insert.rs` n'importe plus `TempFileSink` directement, `cargo test` passe.

---

### T5 — Extraire `src/pass2/traversal.rs` depuis `insert.rs`

**Responsabilité :** traversal du graphe JSON selon les stratégies de schéma.

**Ce qui bouge (fonctions privées de `insert.rs`) :**
- `insert_pivot_object()`
- `insert_jsonb_object()`
- `insert_structured_pivot_object()`
- `dispatch_child_routes()`
- `insert_keyed_pivot_object()`
- `insert_keyed_pivot_array_of_objects()`
- `insert_normalize_dynamic_keys()`
- `insert_multi_keyed_pivot()`
- `insert_array()`

**`insert.rs` après :** contient uniquement `pub fn insert_object()` (~50 lignes d'orchestration).

**Critère de done :** `insert.rs` < 100 lignes, `cargo test` passe.

---

## Ordre d'exécution

```
T0 ✅ → T1 → T2 → T3     (SchemaRegistry — séquence obligatoire, même fichier source)
              ↓
             T4 → T5      (Pass2 — indépendant de T1-T3, peut démarrer après T0)

T6 → T7 → T8 → T9        (Affinement schema — après T0-T5, registry.rs stable)
```

T6→T9 sont une seconde passe de découpage sur `schema/` : suppressions de dead code et
extractions de clusters algorithmiques depuis `finalizer.rs`.

---

## Progression

| Tâche | Statut | Résultat |
|---|---|---|
| T0 — REFACTORING.md | ✅ done | `REFACTORING.md` créé |
| T1 — SchemaObserver | ✅ done | `schema/observer.rs` (new), `registry.rs` 3416→2437 lignes |
| T2 — SchemaFinalizer | ✅ done | `schema/finalizer.rs` (new), `registry.rs` 2437→979 lignes |
| T3 — SchemaReporter | ✅ done | `schema/reporter.rs` (new), `registry.rs` 979→~100 lignes prod |
| T4 — trait RowSink | ✅ done | `pass2/sink.rs` (new), `pass2/insert.rs` −737 lignes |
| T5 — traversal.rs | ✅ done | `pass2/traversal.rs` (new), `pass2/insert.rs` ≤ 100 lignes |
| T6 — Suppression bridges `pub use` | ✅ done | `registry.rs` bridges supprimés, 5 callers mis à jour |
| T7 — Suppression dead code | ✅ done | `_finalize_siblings_archived` supprimé (−524 lignes) |
| T8 — Extraction `cascading.rs` | ✅ done | `schema/cascading.rs` (949 lignes), `finalizer.rs` 2306→837 lignes |
| T9 — Extraction `wide_strategies.rs` | ✅ done | `schema/wide_strategies.rs` (399 lignes), `finalizer.rs` 837→450 lignes |

### Réduction finale

| Fichier | Avant | Après | Réduction |
|---|---|---|---|
| `schema/registry.rs` | 3 416 lignes | ~100 lignes prod (914 avec tests) | −97% prod |
| `schema/finalizer.rs` | — | 450 lignes | nouveau |
| `schema/cascading.rs` | — | 949 lignes | nouveau |
| `schema/wide_strategies.rs` | — | 399 lignes | nouveau |
| `pass2/insert.rs` | 1 009 lignes | ~50 lignes | −95% |
