# Technical Spec — Feature A: Optional Strategy Selection

**Status:** Draft
**Date:** 2026-05-31
**PRD:** [_bmad-output/specs/prd-feature-a-strategy-flags.md](../../_bmad-output/specs/prd-feature-a-strategy-flags.md)

---

## Overview

Feature A adds per-strategy opt-out flags to the CLI and UI. Users can disable optional inference strategies (sibling detection, pivot, structured pivot) before analysis. Mandatory strategies (wide-table split) remain always active.

**Architecture decision: upstream control.** Strategy flags gate what the analyzer *tries*, not what it does with the result. This differs from `--schema-config` TOML which corrects the schema post-analysis.

---

## New Types — `src/schema/strategies.rs` (new file)

```rust
/// Normalized strategy identifier. Stable across versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyName {
    Sibling,
    Pivot,
    StructuredPivot,
    // Split is mandatory — intentionally absent from this enum
}

impl StrategyName {
    /// All optional (disableable) strategies.
    pub const OPTIONAL: &'static [StrategyName] = &[
        StrategyName::Sibling,
        StrategyName::Pivot,
        StrategyName::StructuredPivot,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            StrategyName::Sibling => "sibling",
            StrategyName::Pivot => "pivot",
            StrategyName::StructuredPivot => "structured_pivot",
        }
    }
}

impl TryFrom<&str> for StrategyName {
    type Error = StrategyError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "sibling" => Ok(StrategyName::Sibling),
            "pivot" => Ok(StrategyName::Pivot),
            "structured_pivot" => Ok(StrategyName::StructuredPivot),
            "split" => Err(StrategyError::Mandatory(s.to_string())),
            _ => Err(StrategyError::Unknown(s.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StrategyError {
    #[error("strategy '{0}' is mandatory and cannot be disabled\n  → mandatory strategies: split\n  → optional strategies: sibling, pivot, structured_pivot")]
    Mandatory(String),
    #[error("unknown strategy '{0}'\n  → optional strategies: sibling, pivot, structured_pivot")]
    Unknown(String),
}

/// Validates CLI flag values. Returns the set of disabled strategies or the
/// first error encountered. Called in main.rs before any file I/O.
pub fn parse_disabled_strategies(
    raw: &[String],
) -> Result<HashSet<StrategyName>, StrategyError> {
    raw.iter()
        .map(|s| StrategyName::try_from(s.as_str()))
        .collect()
}
```

---

## Pass 1 Config Struct — `src/pass1/runner.rs`

Replace the 10-parameter flat signatures with a bundled struct:

```rust
/// All parameters controlling a Pass 1 run.
pub struct Pass1Config {
    pub root_table: String,
    pub text_threshold: u32,
    pub array_as_pg_array: bool,
    pub wide_column_threshold: usize,
    pub sibling_threshold: usize,
    pub sibling_jaccard: f64,
    pub stable_threshold: f64,
    pub rare_threshold: f64,
    pub disabled_strategies: HashSet<StrategyName>,
    // run_parallel only:
    pub num_workers: Option<usize>,
}
```

Updated signatures:

```rust
pub fn run(path: &Path, config: &Pass1Config, progress_tx: Option<ProgressTx>) -> Result<Pass1Result>
pub fn run_parallel(path: &Path, config: &Pass1Config, progress_tx: Option<ProgressTx>) -> Result<Pass1Result>
pub fn run_inspect(path: &Path, config: &Pass1Config, limit: usize) -> Result<InspectResult>
```

`run_inspect()` no longer needs to hardcode `sibling_threshold: usize::MAX` — it passes `disabled_strategies: HashSet::from([StrategyName::Sibling])` explicitly instead.

---

## Strategy Gating — `src/schema/registry.rs`

`SchemaRegistry` receives `disabled_strategies` at construction:

```rust
impl SchemaRegistry {
    pub fn new(
        text_threshold: u32,
        array_as_pg_array: bool,
        wide_column_threshold: usize,
        sibling_threshold: usize,
        sibling_jaccard: f64,
        stable_threshold: f64,
        rare_threshold: f64,
        disabled_strategies: HashSet<StrategyName>,
    ) -> Self { ... }
}
```

Gating points (one condition per optional strategy):

| Strategy | Gating location | Condition |
|---|---|---|
| `sibling` | `finalize_siblings()` | `if self.disabled_strategies.contains(&StrategyName::Sibling) { return; }` |
| `pivot` | `suggest_wide_strategy()` — Pivot branch | `if !disabled { allow Pivot } else { fall through to Jsonb }` |
| `structured_pivot` | `suffix_detector.rs` detection call | `if self.disabled_strategies.contains(&StrategyName::StructuredPivot) { skip detection }` |

`split` (AutoSplit / wide-table split) has no gating condition — it is never checked against `disabled_strategies`.

---

## CLI — `src/cli.rs`

```rust
/// Disable an optional inference strategy. Repeatable.
/// Valid values: sibling, pivot, structured_pivot.
/// Mandatory strategies (split) cannot be disabled.
#[arg(long = "disable-strategy", value_name = "STRATEGY")]
pub disable_strategy: Vec<String>,
```

Validation in `main.rs`, before any file I/O:

```rust
let disabled_strategies = parse_disabled_strategies(&cli.disable_strategy)
    .map_err(|e| anyhow::anyhow!("{e}"))?;
```

On error, `anyhow` prints the `StrategyError` message (which already lists mandatory vs optional strategies) and exits non-zero.

---

## UI — `json2sql-ui`

### `AppState` (`src/state.rs`)

```rust
pub struct AppState {
    // ... existing fields ...
    pub disabled_strategies: HashSet<StrategyName>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            // ... existing defaults ...
            disabled_strategies: HashSet::new(), // all strategies active by default
        }
    }
}
```

### `ProjectConfig` (`src/config.rs`)

```rust
pub struct ProjectConfig {
    // ... existing fields ...
    #[serde(default)]
    pub disabled_strategies: Vec<String>, // serialized as strategy name strings
}
```

`ProjectConfig::apply_to(&mut state)` maps `Vec<String>` → `HashSet<StrategyName>` (silently ignoring invalid entries — config file may predate a strategy being renamed).

`ProjectConfig::from_project(&project)` maps `HashSet<StrategyName>` → `Vec<String>` via `StrategyName::as_str()`.

### Setup Screen (`src/screens/setup.rs`) — Advanced accordion (Step 4)

Add a "Strategy selection" card alongside the existing Pass 1 / Pass 2 parallelism cards:

```
┌─ Strategy selection ──────────────────────────────────┐
│  ☑ Sibling detection     (sibling)                    │
│  ☑ Pivot inference       (pivot)                      │
│  ☑ Structured pivot      (structured_pivot)           │
│                                                       │
│  Note: Wide-table split is mandatory and always runs. │
└───────────────────────────────────────────────────────┘
```

Each checkbox reads/writes `state.disabled_strategies` — checked = enabled (not in the disabled set), unchecked = disabled (in the set). The card saves to `ProjectConfig` via the existing auto-save on change.

---

## File Changeset

| File | Change |
|---|---|
| `src/schema/strategies.rs` | **New** — `StrategyName`, `StrategyError`, `parse_disabled_strategies` |
| `src/schema/mod.rs` | Export `strategies` module |
| `src/pass1/runner.rs` | Introduce `Pass1Config`, update `run` / `run_parallel` / `run_inspect` signatures |
| `src/schema/registry.rs` | Add `disabled_strategies` field, gating conditions for sibling / pivot / structured_pivot |
| `src/schema/suffix_detector.rs` | Skip detection when `structured_pivot` is disabled |
| `src/cli.rs` | Add `disable_strategy: Vec<String>` field |
| `src/main.rs` | Call `parse_disabled_strategies`, build `Pass1Config`, pass to runner |
| `json2sql-ui/src/state.rs` | Add `disabled_strategies: HashSet<StrategyName>` to `AppState` |
| `json2sql-ui/src/config.rs` | Add `disabled_strategies: Vec<String>` to `ProjectConfig`, update `apply_to` / `from_project` |
| `json2sql-ui/src/screens/setup.rs` | Add strategy selection card to Advanced accordion |

---

## Test Plan

| Test | Location | Assertion |
|---|---|---|
| `test_disable_sibling_no_keyed_pivot` | `tests/integration/` | disable sibling → output has zero `KeyedPivot` tables |
| `test_disable_pivot_no_pivot_strategy` | `tests/integration/` | disable pivot → output has no `Pivot` strategy tables |
| `test_disable_structured_pivot` | `tests/integration/` | disable structured_pivot → suffix detection not applied |
| `test_disable_mandatory_split_error` | `tests/integration/` | `--disable-strategy split` → non-zero exit, error message contains "mandatory" |
| `test_unknown_strategy_error` | `tests/integration/` | `--disable-strategy foobar` → non-zero exit, error message contains "unknown" |
| `test_no_flags_identical_output` | `tests/integration/` | zero flags → output identical to pre-feature baseline on all existing fixtures |
| `test_strategy_name_round_trip` | `tests/unit/` | `StrategyName::try_from(name.as_str()) == Ok(name)` for all variants |

---

## Architecture Decision: `StrategyName` in `src/schema/` not `src/cli.rs`

`StrategyName` lives in `src/schema/strategies.rs` (not in `cli.rs`) because:
- `SchemaRegistry` (backend) needs it to gate strategies
- `AppState` (UI) needs it without pulling in CLI dependencies
- `cli.rs` receives raw `Vec<String>` and calls `parse_disabled_strategies()` — clean boundary

This also means the UI crate can import `json2sql::schema::strategies::StrategyName` directly without depending on CLI logic.
