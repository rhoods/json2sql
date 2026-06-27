#![allow(clippy::disallowed_methods, clippy::derive_partial_eq_without_eq)]
pub mod setup;
pub mod analysis;
pub mod strategy;
pub mod preview;
pub mod import;
pub mod table_list;


use std::collections::{HashMap, HashSet};
use dioxus::prelude::*;
use json2sql::schema::table_schema::{TableSchema, InferredStrategy, UserOverride};

// ---------------------------------------------------------------------------
// Shared hook: elapsed timer
// ---------------------------------------------------------------------------

/// Increments a seconds counter every second until `is_done()` returns true.
/// Returns the `Signal<u32>` so the caller can display elapsed time.
pub fn use_elapsed_timer<F>(is_done: F) -> Signal<u32>
where
    F: Fn() -> bool + Clone + 'static,
{
    let mut elapsed_secs: Signal<u32> = use_signal(|| 0);
    use_coroutine(move |_: UnboundedReceiver<()>| {
        let is_done = is_done.clone();
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if is_done() { break; }
                let e = *elapsed_secs.read();
                if e < u32::MAX { *elapsed_secs.write() = e + 1; }
            }
        }
    });
    elapsed_secs
}

// ---------------------------------------------------------------------------
// TableRowViewModel — presentation data for one row in the table list panel
// ---------------------------------------------------------------------------

/// Pre-computed display data for one table row.
/// Built by [`build_table_rows`] from raw schema + state. Contains no RSX — purely testable.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct TableRowViewModel {
    pub index:       usize,
    pub name:        String,
    // badge
    pub badge_cls:   &'static str,
    pub badge_lbl:   &'static str,
    // columns
    pub col_count:   usize,
    pub is_wide:     bool,           // col_count > PASS1_WIDE_COLUMN_THRESHOLD
    // tree
    pub depth:       usize,
    pub connector:   &'static str,   // "" | "├─ " | "└─ "
    pub indent_px:   usize,          // depth * 12
    // state
    pub is_selected: bool,
    pub is_routing:  bool,
    pub has_warn:    bool,
    pub row_cls:     &'static str,   // "sel" | "muted" | ""
    // visibility
    pub visible:     bool,
    pub has_children: bool,
    /// Anomaly count from Pass 2 (0 if Pass 2 hasn't run yet).
    pub anomaly_count: u64,
}

/// Input context for [`build_table_rows`]. Groups the per-call parameters so the
/// function signature stays within the `too_many_arguments` threshold.
pub struct TableRowsCtx<'a> {
    pub overrides:        &'a HashMap<String, UserOverride>,
    pub overflow_names:   &'a HashSet<String>,
    pub selected_indices: &'a HashSet<usize>,
    pub absorbed_names:   &'a HashSet<String>,
    pub filter:           &'a str,
    pub show_warn_only:   bool,
    pub anomaly_counts:   &'a HashMap<String, u64>,
}

struct RowFlags {
    is_routing:  bool,
    is_absorbed: bool,
    has_warn:    bool,
    badge_cls:   &'static str,
    badge_lbl:   &'static str,
}

impl RowFlags {
    fn compute(table: &TableSchema, ctx: &TableRowsCtx<'_>) -> Self {
        let user_overrode = ctx.overrides.contains_key(&table.name);
        let is_absorbed = ctx.absorbed_names.contains(&table.name);
        let is_overflow = !user_overrode
            && matches!(table.inferred_strategy, InferredStrategy::Jsonb)
            && ctx.overflow_names.contains(&table.name);
        let is_routing = !user_overrode
            && matches!(table.inferred_strategy, InferredStrategy::SiblingCollapseMulti(_))
            && table.columns.iter().all(|c| c.is_generated);
        let has_warn = is_overflow || is_routing;
        let (badge_cls, badge_lbl) = if is_absorbed { ("muted", "merged") }
            else if is_routing { ("muted", "ROUTE") }
            else if is_overflow { ("warn", "JSONB ⚠") }
            else if let Some(ov) = ctx.overrides.get(&table.name) { user_override_badge(ov) }
            else { strategy_badge(&table.inferred_strategy) };
        Self { is_routing, is_absorbed, has_warn, badge_cls, badge_lbl }
    }
}

fn build_row(
    pos: usize,
    i: usize,
    schemas: &[TableSchema],
    is_last: &[bool],
    parent_names: &HashSet<&str>,
    filter_lc: &str,
    ctx: &TableRowsCtx<'_>,
) -> TableRowViewModel {
    let table = &schemas[i];
    let flags = RowFlags::compute(table, ctx);
    let col_count = table.columns.len();
    let is_wide = col_count > crate::state::PASS1_WIDE_COLUMN_THRESHOLD;
    let connector: &'static str = if table.depth == 0 { "" }
        else if is_last[pos] { "└─ " } else { "├─ " };
    let is_selected = ctx.selected_indices.contains(&i);
    let row_cls: &'static str = if is_selected { "sel" }
        else if flags.is_routing || flags.is_absorbed { "muted" } else { "" };
    let visible = (!ctx.show_warn_only || flags.has_warn)
        && (filter_lc.is_empty() || table.name.to_lowercase().contains(filter_lc));
    TableRowViewModel {
        index: i, name: table.name.clone(),
        badge_cls: flags.badge_cls, badge_lbl: flags.badge_lbl,
        col_count, is_wide, depth: table.depth, connector,
        indent_px: table.depth * 12, is_selected,
        is_routing: flags.is_routing, has_warn: flags.has_warn, row_cls, visible,
        has_children: parent_names.contains(table.name.as_str()),
        anomaly_count: ctx.anomaly_counts.get(&table.name).copied().unwrap_or(0),
    }
}

/// Build the view-model for every table row.
///
/// - Computes badge, overflow/routing flags, tree connectors and indentation.
/// - Filters rows according to `filter` (substring match on name) and `show_warn_only`.
/// - `visible: false` rows are excluded from rendering but retained for index stability.
/// - `overflow_names`: tables auto-promoted to Jsonb by Pass 1 without user override.
/// - `selected_indices`: set of selected row indices (empty → all unselected).
pub fn build_table_rows(schemas: &[TableSchema], ctx: &TableRowsCtx<'_>) -> Vec<TableRowViewModel> {
    let order = tree_display_order(schemas);
    let is_last = compute_last_child(&order, schemas);
    let filter_lc = ctx.filter.to_lowercase();
    let parent_names: HashSet<&str> = schemas.iter()
        .filter_map(|t| t.parent_table.as_deref())
        .collect();
    order.iter().enumerate()
        .map(|(pos, &i)| build_row(pos, i, schemas, &is_last, &parent_names, &filter_lc, ctx))
        .collect()
}

/// Depth-first display order for `schemas`: roots alphabetically, then their
/// children alphabetically directly underneath, recursively.
/// Tables whose parent is absent from `schemas` are appended at the end.
#[allow(clippy::too_many_lines)]
pub fn tree_display_order(schemas: &[TableSchema]) -> Vec<usize> {
    fn dfs(
        i: usize,
        schemas: &[TableSchema],
        children_of: &HashMap<&str, Vec<usize>>,
        order: &mut Vec<usize>,
        visited: &mut Vec<bool>,
    ) {
        if visited[i] { return; }
        visited[i] = true;
        order.push(i);
        if let Some(children) = children_of.get(schemas[i].name.as_str()) {
            for &c in children { dfs(c, schemas, children_of, order, visited); }
        }
    }

    let name_set: HashSet<&str> = schemas.iter().map(|t| t.name.as_str()).collect();
    let mut children_of: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut true_roots:   Vec<usize> = Vec::new();
    let mut orphan_roots: Vec<usize> = Vec::new();

    for (i, t) in schemas.iter().enumerate() {
        match t.parent_table.as_deref() {
            None                              => true_roots.push(i),
            Some(p) if name_set.contains(p)  => children_of.entry(p).or_default().push(i),
            Some(_)                           => orphan_roots.push(i),
        }
    }

    true_roots.sort_by(|&a, &b| schemas[a].name.cmp(&schemas[b].name));
    orphan_roots.sort_by(|&a, &b| schemas[a].name.cmp(&schemas[b].name));
    for v in children_of.values_mut() {
        v.sort_by(|&a, &b| schemas[a].name.cmp(&schemas[b].name));
    }

    let mut order   = Vec::with_capacity(schemas.len());
    let mut visited = vec![false; schemas.len()];

    for &r in &true_roots  { dfs(r, schemas, &children_of, &mut order, &mut visited); }
    for &r in &orphan_roots { dfs(r, schemas, &children_of, &mut order, &mut visited); }

    order
}

// ---------------------------------------------------------------------------
// Shared file picker helpers (rfd — native OS dialog, cross-platform)
// ---------------------------------------------------------------------------

/// Result of a file picker invocation.
pub enum PickResult {
    Selected(std::path::PathBuf),
    Cancelled,
    /// Never returned by rfd (compiled-in library); kept for exhaustive matches.
    #[allow(dead_code)]
    NotAvailable,
}

fn option_to_pick_result(opt: Option<std::path::PathBuf>) -> PickResult {
    opt.map_or(PickResult::Cancelled, PickResult::Selected)
}

/// Parse a glob pattern string like `"*.json *.jsonl"` into rfd extension list `["json", "jsonl"]`.
fn parse_ext(pattern: &str) -> Vec<String> {
    pattern
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_start_matches("*.").to_string())
        .collect()
}

// One dialog at a time — the OS only supports a single native file dialog per process.
static PICKER_ACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// RAII guard: resets PICKER_ACTIVE to false when dropped, even on panic or future cancellation.
struct PickerGuard;
impl Drop for PickerGuard {
    fn drop(&mut self) {
        PICKER_ACTIVE.store(false, std::sync::atomic::Ordering::Release);
    }
}

pub async fn pick_file(filters: &[(&str, &str)]) -> PickResult {
    if PICKER_ACTIVE.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return PickResult::Cancelled;
    }
    let _guard = PickerGuard;
    let mut dialog = rfd::AsyncFileDialog::new().set_title("Select file");
    for (name, pattern) in filters {
        let exts = parse_ext(pattern);
        dialog = dialog.add_filter(*name, &exts);
    }
    let handle = dialog.pick_file().await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub async fn pick_folder() -> PickResult {
    if PICKER_ACTIVE.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return PickResult::Cancelled;
    }
    let _guard = PickerGuard;
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Select folder")
        .pick_folder()
        .await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub async fn pick_save_file(default_name: &str) -> PickResult {
    if PICKER_ACTIVE.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return PickResult::Cancelled;
    }
    let _guard = PickerGuard;
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Save schema as")
        .set_file_name(default_name)
        .add_filter("JSON", &["json"])
        .save_file()
        .await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub const fn strategy_label(s: &InferredStrategy) -> &'static str {
    match s {
        InferredStrategy::Columns                     => "DEFAULT",
        InferredStrategy::Pivot                       => "PIVOT",
        InferredStrategy::Jsonb                       => "JSONB SÉP.",
        InferredStrategy::JsonbFlatten                => "JSONB INLINE",
        InferredStrategy::StructuredPivot(_)          => "STRUCT PIVOT",
        InferredStrategy::SiblingCollapse(_)               => "SIBLING COLLAPSE",
        InferredStrategy::SiblingCollapseMulti(_)          => "SIBLING COLLAPSE MULTI",
        InferredStrategy::AutoSplit { .. }            => "AUTO SPLIT",
        InferredStrategy::Ignore                      => "SKIP",
        InferredStrategy::NormalizeDynamicKeys { .. } => "NORMALIZE",
        InferredStrategy::Flatten { .. }              => "FLATTEN",
    }
}

/// For each position in `order`, returns true if that table is the last child
/// of its parent within the display order. Used to pick └─ vs ├─ connectors.
pub fn compute_last_child(order: &[usize], schemas: &[TableSchema]) -> Vec<bool> {
    let mut result = vec![true; order.len()];
    for pos in 0..order.len() {
        let i = order[pos];
        if let Some(ref parent) = schemas[i].parent_table {
            for later_pos in (pos + 1)..order.len() {
                if schemas[order[later_pos]].parent_table.as_deref() == Some(parent.as_str()) {
                    result[pos] = false;
                    break;
                }
            }
        }
    }
    result
}

/// Returns (`css_badge_class_suffix`, `short_label`) for the new design-system `.badge` classes.
pub const fn strategy_badge(s: &InferredStrategy) -> (&'static str, &'static str) {
    match s {
        InferredStrategy::Columns                     => ("default",   "default"),
        InferredStrategy::Jsonb                       => ("jsonb",     "jsonb"),
        InferredStrategy::JsonbFlatten                => ("jsonbi",    "flatten"),
        InferredStrategy::Pivot                       => ("pivot",     "pivot"),
        InferredStrategy::NormalizeDynamicKeys { .. } => ("normalize", "normalize"),
        InferredStrategy::Ignore                      => ("skip",      "skip"),
        InferredStrategy::Flatten { .. }              => ("flatten",   "flatten"),
        InferredStrategy::StructuredPivot(_)          => ("pivot",     "struct pivot"),
        InferredStrategy::SiblingCollapse(_)               => ("pivot",     "sibling collapse"),
        InferredStrategy::SiblingCollapseMulti(_)          => ("pivot",     "sibling collapse multi"),
        InferredStrategy::AutoSplit { .. }            => ("normalize", "auto split"),
    }
}

pub fn user_override_badge(o: &UserOverride) -> (&'static str, &'static str) {
    match o {
        UserOverride::Columns                      => ("columns",   "columns"),
        UserOverride::Pivot                        => ("pivot",     "pivot"),
        UserOverride::Jsonb                        => ("jsonb",     "jsonb"),
        UserOverride::Skip                         => ("skip",      "skip"),
        UserOverride::JsonbFlatten                 => ("jsonb",     "jsonb flatten"),
        UserOverride::Flatten { .. }               => ("flatten",   "flatten"),
        UserOverride::NormalizeDynamicKeys { .. }  => ("normalize", "normalize"),
    }
}


/// Compute a progress percentage from `done` units out of `total`.
/// Returns 0 if total is 0, clamps to 100 if done >= total.
pub fn progress_pct(done: u64, total: u64) -> u32 {
    if total == 0 { return 0; }
    ((done * 100 / total).min(100)) as u32
}

// ---------------------------------------------------------------------------
// Shared component: progress bar
// ---------------------------------------------------------------------------

/// Returns the CSS class for a progress bar track.
/// `indeterminate` (scanning animation) only when waiting to start (pct == 0 and not done).
/// Once progress begins, the bar fills deterministically via `width:{pct}%`.
pub const fn progress_bar_class(done: bool, pct: u32) -> &'static str {
    if !done && pct == 0 { "prog thick indeterminate" } else { "prog thick" }
}

/// A labeled progress bar used in `AnalysisScreen` and `ImportScreen`.
///
/// - `pct`   : 0–100
/// - `done`  : if true, bar is solid (no animation)
/// - `label` : caption line below the bar (bytes/rows/ETA)
/// - `phase` : short phase name shown as a prefix badge (e.g. "Streaming")
#[allow(clippy::derive_partial_eq_without_eq)]
#[component]
pub fn ProgressBar(pct: u32, done: bool, label: String, phase: String) -> Element {
    let cls = progress_bar_class(done, pct);
    rsx! {
        div {
            div { style: "display:flex;align-items:center;gap:8px;margin-bottom:4px;",
                span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;min-width:32px;",
                    "{pct}%"
                }
                span { style: "font-size:var(--fs-xs);color:var(--fg-2);font-weight:600;", "{phase}" }
            }
            div { class: "{cls}",
                i { style: "width:{pct}%;", "" }
            }
            span { style: "font-size:var(--fs-xs);color:var(--fg-3);font-family:'JetBrains Mono',monospace;",
                "{label}"
            }
        }
    }
}

/// Apply user strategy overrides (`Pivot | Jsonb | Skip`) to a copy of `schemas`.
///
/// Delegates to `json2sql::schema::config::apply_user_overrides` so both the GUI
/// and CLI paths produce identical output for the same overrides. `Skip` removes the
/// table and — for `AutoSplit` tables — also removes the companion `_wide` table.
/// The original slice is never mutated.
pub fn build_effective_schemas(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
) -> Vec<TableSchema> {
    let mut result = schemas.to_vec();
    json2sql::schema::config::apply_user_overrides(&mut result, strategy_overrides);
    // Defensive dedup: stale snapshots saved before the finalizer fix may contain
    // duplicate table names, which would cause add_constraints() to fail with 42P16.
    let mut seen = std::collections::HashSet::new();
    result.retain(|s| seen.insert(s.name.clone()));
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use json2sql::schema::table_schema::TableSchema;

    // --- picker helpers ---

    #[test]
    fn none_maps_to_cancelled() {
        assert!(matches!(option_to_pick_result(None), PickResult::Cancelled));
    }

    #[test]
    fn some_path_maps_to_selected() {
        let path = PathBuf::from("/tmp/test.json");
        assert!(matches!(
            option_to_pick_result(Some(path.clone())),
            PickResult::Selected(p) if p == path
        ));
    }

    #[test]
    fn parse_ext_single() {
        assert_eq!(parse_ext("*.json"), vec!["json"]);
    }

    #[test]
    fn parse_ext_multiple() {
        assert_eq!(parse_ext("*.json *.jsonl *.ndjson"), vec!["json", "jsonl", "ndjson"]);
    }

    #[test]
    fn parse_ext_no_star_prefix() {
        assert_eq!(parse_ext("json"), vec!["json"]);
    }

    #[test]
    fn parse_ext_empty_string() {
        let result = parse_ext("");
        assert!(result.is_empty());
    }

    fn make_table(name: &str, parent: Option<&str>) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.parent_table = parent.map(|s| s.to_string());
        t
    }

    fn identity_order(n: usize) -> Vec<usize> { (0..n).collect() }

    #[test]
    fn no_children_all_true() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        assert_eq!(compute_last_child(&identity_order(2), &schemas), vec![true, true]);
    }

    #[test]
    fn single_child_is_last() {
        let schemas = vec![make_table("parent", None), make_table("child", Some("parent"))];
        assert_eq!(compute_last_child(&identity_order(2), &schemas), vec![true, true]);
    }

    #[test]
    fn two_children_first_is_not_last() {
        let schemas = vec![
            make_table("parent", None),
            make_table("child1", Some("parent")),
            make_table("child2", Some("parent")),
        ];
        assert_eq!(compute_last_child(&identity_order(3), &schemas), vec![true, false, true]);
    }

    #[test]
    fn three_children_only_last_is_true() {
        let schemas = vec![
            make_table("root", None),
            make_table("c1", Some("root")),
            make_table("c2", Some("root")),
            make_table("c3", Some("root")),
        ];
        assert_eq!(compute_last_child(&identity_order(4), &schemas), vec![true, false, false, true]);
    }

    #[test]
    fn empty_slice_returns_empty() {
        let schemas: Vec<TableSchema> = vec![];
        assert_eq!(compute_last_child(&identity_order(0), &schemas), Vec::<bool>::new());
    }

    // --- tree_display_order ---

    #[test]
    fn tree_order_flat_schemas_sorted_alphabetically() {
        let schemas = vec![make_table("zebra", None), make_table("alpha", None), make_table("mango", None)];
        let order = tree_display_order(&schemas);
        let names: Vec<&str> = order.iter().map(|&i| schemas[i].name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "mango", "zebra"]);
    }

    #[test]
    fn tree_order_children_grouped_under_parent() {
        // enfants avant parent dans le slice
        let schemas = vec![
            make_table_depth("child_b", Some("parent"), 1),
            make_table_depth("child_a", Some("parent"), 1),
            make_table_depth("parent",  None,           0),
        ];
        let order = tree_display_order(&schemas);
        let names: Vec<&str> = order.iter().map(|&i| schemas[i].name.as_str()).collect();
        assert_eq!(names, vec!["parent", "child_a", "child_b"]);
        // indices originaux préservés
        assert_eq!(order[0], 2, "parent est à l'index 2 dans schemas");
        assert_eq!(order[1], 1, "child_a est à l'index 1 dans schemas");
        assert_eq!(order[2], 0, "child_b est à l'index 0 dans schemas");
    }

    #[test]
    fn tree_order_three_levels() {
        let schemas = vec![
            make_table_depth("grandchild", Some("child"), 2),
            make_table_depth("child",      Some("root"),  1),
            make_table_depth("root",       None,          0),
        ];
        let order = tree_display_order(&schemas);
        let names: Vec<&str> = order.iter().map(|&i| schemas[i].name.as_str()).collect();
        assert_eq!(names, vec!["root", "child", "grandchild"]);
    }

    #[test]
    fn tree_order_orphan_parent_at_end() {
        let schemas = vec![
            make_table_depth("alpha",  None,      0),
            make_table_depth("orphan", Some("missing"), 1),
        ];
        let order = tree_display_order(&schemas);
        let names: Vec<&str> = order.iter().map(|&i| schemas[i].name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "orphan"]);
    }

    #[test]
    fn connectors_correct_after_reorder() {
        // c2 avant c1 dans le slice, mais c1 < c2 alphabétiquement
        let schemas = vec![
            make_table_depth("parent", None,            0),
            make_table_depth("c2",     Some("parent"),  1),
            make_table_depth("c1",     Some("parent"),  1),
        ];
        let rows = empty_rows(&schemas);
        assert_eq!(rows[0].name, "parent");
        assert_eq!(rows[1].name, "c1");
        assert_eq!(rows[1].connector, "├─ ", "c1 a un frère après lui");
        assert_eq!(rows[2].name, "c2");
        assert_eq!(rows[2].connector, "└─ ", "c2 est le dernier");
    }

    // --- build_effective_schemas ---

    #[test]
    fn ignore_removes_table() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        let result = build_effective_schemas(&schemas, &overrides);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "b");
    }

    #[test]
    fn no_overrides_returns_full_clone() {
        let schemas = vec![make_table("x", None), make_table("y", None)];
        let result = build_effective_schemas(&schemas, &HashMap::new());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "x");
    }

    #[test]
    fn jsonb_override_applied() {
        use json2sql::schema::table_schema::ColumnSchema;
        use json2sql::schema::type_tracker::PgType;

        let mut t = make_table("wide", None);
        // Add a data column so apply_wide_strategy_columns has something to restructure.
        t.columns.push(ColumnSchema {
            name: "val".to_string(),
            original_name: "val".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let schemas = vec![t];
        let mut overrides = HashMap::new();
        overrides.insert("wide".to_string(), UserOverride::Jsonb);
        let result = build_effective_schemas(&schemas, &overrides);
        assert!(matches!(result[0].inferred_strategy, InferredStrategy::Jsonb));
    }

    #[test]
    fn original_schemas_not_mutated() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let original_len = schemas.len();
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        let _ = build_effective_schemas(&schemas, &overrides);
        assert_eq!(schemas.len(), original_len, "original slice must not be mutated");
    }

    #[test]
    fn duplicate_names_in_input_are_deduped() {
        // Regression: stale snapshots saved before the finalizer dedup fix can
        // contain duplicate table names → add_constraints() would fail with 42P16.
        // build_effective_schemas must dedup defensively.
        let schemas = vec![
            make_table("a", None),
            make_table("b", None),
            make_table("a", None), // duplicate
        ];
        let result = build_effective_schemas(&schemas, &HashMap::new());
        assert_eq!(result.len(), 2, "duplicate table names must be removed");
        assert!(result.iter().any(|s| s.name == "a"));
        assert!(result.iter().any(|s| s.name == "b"));
    }


    #[test]
    fn skip_autosplit_removes_companion_wide_table() {
        // AutoSplit produces two tables: the main (e.g. "events") and a companion
        // "events_wide". Applying Skip on the main must remove BOTH so the GUI
        // Preview shows the same schema as the CLI snapshot-restore path.
        let mut main = make_table("events", None);
        main.inferred_strategy = InferredStrategy::AutoSplit {
            stable_threshold: 0.8,
            rare_threshold: 0.1,
            medium_keys: std::collections::HashSet::new(),
            wide_table_name: "events_wide".to_string(),
        };
        let companion = make_table("events_wide", None);
        let other = make_table("users", None);

        let schemas = vec![main, companion, other];
        let mut overrides = HashMap::new();
        overrides.insert("events".to_string(), UserOverride::Skip);
        let result = build_effective_schemas(&schemas, &overrides);

        let names: Vec<&str> = result.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"events"),      "main table must be removed");
        assert!(!names.contains(&"events_wide"), "companion _wide table must be removed");
        assert!(names.contains(&"users"),        "unrelated table must survive");
    }

    // --- build_table_rows helpers ---

    fn make_table_depth(name: &str, parent: Option<&str>, depth: usize) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], depth);
        t.parent_table = parent.map(|s| s.to_string());
        t
    }

    fn make_routing_table(name: &str) -> TableSchema {
        use json2sql::schema::table_schema::{ColumnSchema, KeyShape, SiblingGroup, SiblingSchema};
        use json2sql::schema::type_tracker::PgType;
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.inferred_strategy = InferredStrategy::SiblingCollapseMulti(vec![SiblingGroup {
            pivot_table: format!("{name}_pivot"),
            key_is_numeric: false,
            sibling_schema: SiblingSchema {
                key_col_name: "key".to_string(),
                key_shape: KeyShape::Slug,
                array_children: false,
            },
            absorbed_names: vec![],
            path_segment: "key".to_string(),
            absorbed_path_segments: vec![],
        }]);
        t.columns.push(ColumnSchema {
            name: "j2s_id".to_string(), original_name: "j2s_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: false,
        });
        t
    }

    fn make_overflow_table(name: &str) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.inferred_strategy = InferredStrategy::Jsonb;
        t
    }

    fn make_wide_table(name: &str, col_count: usize) -> TableSchema {
        use json2sql::schema::table_schema::ColumnSchema;
        use json2sql::schema::type_tracker::PgType;
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        for i in 0..col_count {
            t.columns.push(ColumnSchema {
                name: format!("col_{i}"), original_name: format!("col_{i}"),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
        }
        t
    }

    fn empty_rows(schemas: &[TableSchema]) -> Vec<TableRowViewModel> {
        let (ov, ov_n, sel, abs, an): (HashMap<String, UserOverride>, _, _, _, _) =
            (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        build_table_rows(schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, filter: "", show_warn_only: false, anomaly_counts: &an,
        })
    }

    // --- build_table_rows tests ---

    #[test]
    fn normal_table_has_correct_badge_and_visible() {
        let schemas = vec![make_table("orders", None)];
        let rows = empty_rows(&schemas);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "orders");
        assert!(rows[0].visible);
        assert!(!rows[0].has_warn);
        assert!(!rows[0].is_routing);
    }

    #[test]
    fn overflow_without_override_sets_warn_badge() {
        let schemas = vec![make_overflow_table("big")];
        let overflow = HashSet::from(["big".to_string()]);
        let (ov, sel, abs, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &overflow, selected_indices: &sel,
            absorbed_names: &abs, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(rows[0].has_warn);
        assert_eq!(rows[0].badge_cls, "warn");
        assert_eq!(rows[0].badge_lbl, "JSONB ⚠");
    }

    #[test]
    fn overflow_with_user_override_suppresses_warn() {
        let schemas = vec![make_overflow_table("big")];
        let overflow = HashSet::from(["big".to_string()]);
        let mut overrides = HashMap::new();
        overrides.insert("big".to_string(), UserOverride::Jsonb);
        let (sel, abs, an) = (HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &overrides, overflow_names: &overflow, selected_indices: &sel,
            absorbed_names: &abs, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(!rows[0].has_warn, "user override must suppress overflow flag");
        assert_ne!(rows[0].badge_cls, "warn");
    }

    #[test]
    fn routing_container_detected() {
        let schemas = vec![make_routing_table("products_pivot")];
        let rows = empty_rows(&schemas);
        assert!(rows[0].is_routing);
        assert_eq!(rows[0].badge_cls, "muted");
        assert_eq!(rows[0].badge_lbl, "ROUTE");
        assert!(rows[0].has_warn);
    }

    #[test]
    fn filter_text_hides_non_matching() {
        let schemas = vec![make_table("orders", None), make_table("users", None)];
        let (ov, ov_n, sel, abs, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, filter: "user", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(!rows[0].visible, "orders should be hidden");
        assert!(rows[1].visible, "users should match");
    }

    #[test]
    fn empty_filter_shows_all() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let rows = empty_rows(&schemas);
        assert!(rows.iter().all(|r| r.visible));
    }

    #[test]
    fn show_warn_only_hides_clean_rows() {
        let schemas = vec![make_table("clean", None), make_overflow_table("big")];
        let overflow = HashSet::from(["big".to_string()]);
        let (ov, sel, abs, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &overflow, selected_indices: &sel,
            absorbed_names: &abs, filter: "", show_warn_only: true, anomaly_counts: &an,
        });
        // tree_display_order sorts alphabetically: "big" appears before "clean".
        let clean = rows.iter().find(|r| r.name == "clean").unwrap();
        let big   = rows.iter().find(|r| r.name == "big").unwrap();
        assert!(!clean.visible, "clean table must be hidden");
        assert!(big.visible,    "warn table must be visible");
    }

    #[test]
    fn selected_row_has_sel_class() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let selected = HashSet::from([1usize]);
        let (ov, ov_n, abs, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &selected,
            absorbed_names: &abs, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert_eq!(rows[0].row_cls, "");
        assert_eq!(rows[1].row_cls, "sel");
        assert!(rows[1].is_selected);
    }

    #[test]
    fn connector_root_table_is_empty() {
        let schemas = vec![make_table_depth("root", None, 0)];
        let rows = empty_rows(&schemas);
        assert_eq!(rows[0].connector, "");
        assert_eq!(rows[0].indent_px, 0);
    }

    #[test]
    fn connector_last_child_is_corner() {
        let schemas = vec![
            make_table_depth("parent", None, 0),
            make_table_depth("child", Some("parent"), 1),
        ];
        let rows = empty_rows(&schemas);
        assert_eq!(rows[1].connector, "└─ ");
        assert_eq!(rows[1].indent_px, 12);
    }

    #[test]
    fn connector_non_last_child_is_tee() {
        let schemas = vec![
            make_table_depth("parent", None, 0),
            make_table_depth("c1", Some("parent"), 1),
            make_table_depth("c2", Some("parent"), 1),
        ];
        let rows = empty_rows(&schemas);
        assert_eq!(rows[1].connector, "├─ ", "c1 has a sibling after it");
        assert_eq!(rows[2].connector, "└─ ", "c2 is last");
    }

    #[test]
    fn wide_table_is_flagged() {
        let schemas = vec![make_wide_table("fat", 101)];
        let rows = empty_rows(&schemas);
        assert!(rows[0].is_wide);
    }

    #[test]
    fn non_wide_table_not_flagged() {
        let schemas = vec![make_wide_table("slim", 5)];
        let rows = empty_rows(&schemas);
        assert!(!rows[0].is_wide);
    }

    #[test]
    fn index_matches_position() {
        let schemas = vec![make_table("a", None), make_table("b", None), make_table("c", None)];
        let rows = empty_rows(&schemas);
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(row.index, i);
        }
    }

    #[test]
    fn absorbed_table_shows_merged_badge() {
        let schemas = vec![
            make_table("parent", None),
            make_table("child_a", Some("parent")),
            make_table("child_b", Some("parent")),
        ];
        let absorbed = HashSet::from(["child_a".to_string(), "child_b".to_string()]);
        let (ov, ov_n, sel, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &absorbed, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(rows[0].visible,                    "parent must be visible");
        assert!(rows[1].visible,                    "absorbed child_a must remain visible");
        assert_eq!(rows[1].badge_lbl, "merged",     "absorbed must show merged badge");
        assert_eq!(rows[1].row_cls,   "muted",      "absorbed must be muted");
        assert_eq!(rows[2].badge_lbl, "merged");
    }

    #[test]
    fn non_absorbed_table_has_normal_badge() {
        let schemas = vec![make_table("standalone", None)];
        let absorbed: HashSet<String> = HashSet::new();
        let (ov, ov_n, sel, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &absorbed, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(rows[0].visible);
        assert_ne!(rows[0].badge_lbl, "merged");
    }

    #[test]
    fn anomaly_count_populated_from_map() {
        let schemas = vec![make_table("orders", None), make_table("users", None)];
        let mut anomalies = HashMap::new();
        anomalies.insert("orders".to_string(), 7u64);
        let (ov, ov_n, sel, abs) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, filter: "", show_warn_only: false, anomaly_counts: &anomalies,
        });
        assert_eq!(rows[0].anomaly_count, 7, "orders must carry anomaly count");
        assert_eq!(rows[1].anomaly_count, 0, "users has no anomaly");
    }

    // --- progress_pct ---

    #[test]
    fn progress_pct_zero_when_total_is_zero() {
        assert_eq!(progress_pct(0, 0), 0);
    }

    #[test]
    fn progress_pct_zero_at_start() {
        assert_eq!(progress_pct(0, 1000), 0);
    }

    #[test]
    fn progress_pct_half() {
        assert_eq!(progress_pct(500, 1000), 50);
    }

    #[test]
    fn progress_pct_full() {
        assert_eq!(progress_pct(1000, 1000), 100);
    }

    #[test]
    fn progress_pct_capped_at_100_when_done_exceeds_total() {
        assert_eq!(progress_pct(1500, 1000), 100);
    }

    // --- progress_bar_class ---

    #[test]
    fn bar_class_indeterminate_only_when_not_started() {
        // pct=0, not done → waiting to start → indeterminate animation
        assert_eq!(progress_bar_class(false, 0), "prog thick indeterminate");
    }

    #[test]
    fn bar_class_deterministic_once_progress_begins() {
        // pct>0, not done → filling progressively, no scanning animation
        assert_eq!(progress_bar_class(false, 1), "prog thick");
        assert_eq!(progress_bar_class(false, 50), "prog thick");
        assert_eq!(progress_bar_class(false, 99), "prog thick");
    }

    #[test]
    fn bar_class_solid_when_done() {
        assert_eq!(progress_bar_class(true, 100), "prog thick");
        // Edge: done=true but pct still 0 (e.g. empty phase) → no animation
        assert_eq!(progress_bar_class(true, 0), "prog thick");
    }

    #[test]
    fn picker_guard_resets_active_flag_on_drop() {
        use std::sync::atomic::Ordering;
        // Ensure clean state
        PICKER_ACTIVE.store(false, Ordering::SeqCst);
        // Simulate picker acquiring the flag
        PICKER_ACTIVE.store(true, Ordering::SeqCst);
        assert!(PICKER_ACTIVE.load(Ordering::SeqCst));
        // Drop guard must reset the flag
        drop(PickerGuard);
        assert!(!PICKER_ACTIVE.load(Ordering::SeqCst), "PickerGuard drop must reset PICKER_ACTIVE to false");
    }

    #[test]
    fn picker_active_swap_blocks_reentry() {
        use std::sync::atomic::Ordering;
        PICKER_ACTIVE.store(false, Ordering::SeqCst);
        // First acquire succeeds (returns false = was not active)
        let was_active = PICKER_ACTIVE.swap(true, Ordering::AcqRel);
        assert!(!was_active, "first acquire should succeed");
        // Second acquire fails (returns true = was already active)
        let was_active2 = PICKER_ACTIVE.swap(true, Ordering::AcqRel);
        assert!(was_active2, "second acquire must see flag already set");
        // Cleanup
        PICKER_ACTIVE.store(false, Ordering::SeqCst);
    }
}
