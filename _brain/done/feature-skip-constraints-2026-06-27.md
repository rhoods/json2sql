# feature: skip_constraints — 2026-06-27

## Résumé

Ajout d'un flag `--no-constraints` (CLI) et d'une option `skip_constraints` (IHM) permettant de sauter entièrement la phase `add_constraints` en fin de pass2.

## Fichiers modifiés

- `src/pass2/runner.rs` — `skip_constraints: bool` dans `Pass2Config` ; `add_constraints` conditionnel
- `src/pipeline.rs` — `skip_constraints: bool` dans `PipelineConfig` ; propagation dans `run_pass2`
- `src/cli.rs` — flag `--no-constraints` via clap
- `src/main.rs` — mapping `cli.no_constraints → PipelineConfig.skip_constraints`
- `json2sql-ui/src/state.rs` — `ProjectState.skip_constraints` + `Pass2Progress.constraints_skipped` ; `Pass2Done` force `constraints_complete = true` si skipped
- `json2sql-ui/src/config.rs` — `ProjectConfig.skip_constraints` avec `#[serde(default)]`
- `json2sql-ui/src/screens/setup.rs` — checkbox "Skip constraints" dans accordion Advanced
- `json2sql-ui/src/screens/import.rs` — propagation dans `Pass2Config` ; Phase D affiche "Skipped"

## Points clés

- `#[serde(default)]` sur `ProjectConfig.skip_constraints` → compat TOML ascendante
- `constraints_skipped` positionné avant le spawn → `Pass2Done` peut forcer `constraints_complete`
- Phase D : `pct = 100` et `label = "Skipped"` quand `constraints_skipped = true`
- Fixes pré-existants dans les tests UI : `detected_format`, `SiblingSchema`, `non_wide_table_not_flagged`

## Issue

GitHub #15 — fermée le 2026-06-27
