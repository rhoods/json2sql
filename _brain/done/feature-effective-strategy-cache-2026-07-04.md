# feature: cache effective_strategy() — 2026-07-04

## Résumé

`TableSchema::effective_strategy()` allouait un nouvel `InferredStrategy` (clone de `String`) à
chaque appel pour les variants `Flatten { prefix }` / `NormalizeDynamicKeys { id_column }`, dans le
hot path de Pass 2 (un appel par objet JSON, traversal + insert). Ajout d'un champ
`cached_strategy: Option<InferredStrategy>` recalculé aux points de mutation fiables du pipeline,
permettant à `effective_strategy()` de retourner `Cow::Borrowed` sans allocation.

## Fichiers modifiés

- `src/schema/table_schema.rs` — champ `cached_strategy` (`#[serde(skip)]`), `effective_strategy()`
  lit le cache avec fallback, `recompute_cached_strategy()` (rendue `pub` pour le bench)
- `src/schema/registry.rs` / `finalizer.rs` — calcule le cache baseline dans `finalize_inner()`
- `src/schema/config.rs` — `apply_overrides_complete()` recalcule le cache **avant**
  `exclude_absorbed_children()` (sinon lecture d'un cache figé au baseline)
- `src/schema/persistence.rs` — `load()` recalcule le cache inconditionnellement (pas seulement sur
  la migration legacy `strategy_overrides`)
- `benches/effective_strategy.rs` — bench dédié (nouveau)

## Points clés

- Garde-fou : si `cached_strategy` est `None`, fallback sur le calcul direct (jamais de donnée
  fausse, juste un site non optimisé)
- **Bug trouvé en clôturant la feature :** le bench (task 1) posait `ui_override`/`toml_override`
  sans appeler `recompute_cached_strategy()` → `cached_strategy` restait `None`, le bench mesurait
  toujours le fallback allouant, masquant un gain de ~0% sur les 3 scénarios override. Corrigé en
  task 9 (commit `a8934cc`) : `recompute_cached_strategy()` passée en `pub`, appelée dans le setup
  du bench.
- Gain réel confirmé (`cargo bench --bench effective_strategy`, 1000 appels/itération) :
  `ui_override_flatten` ~29.9µs → ~5.0µs (-83%), `ui_override_normalize_dynamic_keys` ~28.8µs →
  ~5.1µs (-82%), `toml_override_normalize_dynamic_keys` ~28.2µs → ~4.9µs (-83%) — au niveau du cas
  sans override.
- 502 tests unitaires + tests d'intégration verts, merge sans conflit sur `master`.

## Issue

GitHub #22 — fermée le 2026-07-04, mergée sur `master` (commit `74bc8e7`).
