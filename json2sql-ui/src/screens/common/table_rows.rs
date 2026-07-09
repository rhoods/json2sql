//! Construction du view-model de la liste de tables : badges, arbre d'affichage
//! (ordre DFS, connecteurs └─/├─), filtre, visibilité, overflow/routing/absorbed.
//!
//! Fonctions :
//! - struct `TableRowViewModel` — données d'affichage précalculées pour une ligne de la liste de tables
//! - struct `TableRowsCtx` — contexte d'entrée partagé pour construire les lignes (overrides, filtres, sélection)
//! - struct `RowFlags` — indicateurs internes (routing/absorbed/warn/badge) calculés pour une table
//! - fn `RowFlags::compute` — calcule les indicateurs d'une table à partir du contexte
//! - fn `build_row` — construit le view-model d'une ligne à partir d'une table et de sa position
//! - fn `build_table_rows` — construit le view-model de toutes les lignes (ordre, filtre, visibilité)
//! - fn `tree_display_order` — ordre d'affichage DFS (racines triées, enfants groupés, orphelins en fin)
//! - fn `compute_last_child` — pour chaque position, vrai si dernier enfant de son parent (connecteur └─/├─)
use std::collections::{HashMap, HashSet};
use json2sql::schema::table_schema::{TableSchema, InferredStrategy, UserOverride};

use super::{strategy_badge, user_override_badge};

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
    /// Real children that will be removed by cascade from a `Skip` override on an ancestor —
    /// never set by the user directly (see `apply_user_overrides` cascade, #47).
    pub cascaded_names:   &'a HashSet<String>,
    pub filter:           &'a str,
    pub show_warn_only:   bool,
    pub anomaly_counts:   &'a HashMap<String, u64>,
}

#[allow(clippy::struct_excessive_bools)] // each flag is an independent, orthogonal row category
struct RowFlags {
    is_routing:       bool,
    is_absorbed:      bool,
    is_cascaded_skip: bool,
    has_warn:         bool,
    badge_cls:        &'static str,
    badge_lbl:        &'static str,
}

impl RowFlags {
    fn compute(table: &TableSchema, ctx: &TableRowsCtx<'_>) -> Self {
        let user_overrode = ctx.overrides.contains_key(&table.name);
        let effective = table.effective_strategy();
        let is_absorbed = ctx.absorbed_names.contains(&table.name);
        let is_cascaded_skip = ctx.cascaded_names.contains(&table.name);
        let is_overflow = !user_overrode
            && matches!(*effective, InferredStrategy::Jsonb)
            && ctx.overflow_names.contains(&table.name);
        let is_routing = !user_overrode
            && matches!(*effective, InferredStrategy::SiblingCollapseMulti(_))
            && table.columns.iter().all(|c| c.is_generated);
        let has_warn = is_overflow || is_routing;
        let (badge_cls, badge_lbl) = if is_absorbed { ("muted", "merged") }
            else if is_cascaded_skip { ("skip", "skip ↳") }
            else if is_routing { ("muted", "ROUTE") }
            else if is_overflow { ("warn", "JSONB ⚠") }
            else if let Some(ov) = ctx.overrides.get(&table.name) { user_override_badge(ov) }
            else { strategy_badge(&*effective) };
        Self { is_routing, is_absorbed, is_cascaded_skip, has_warn, badge_cls, badge_lbl }
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
        else if flags.is_routing || flags.is_absorbed || flags.is_cascaded_skip { "muted" } else { "" };
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

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use json2sql::schema::table_schema::TableSchema;

    fn make_table(name: &str, parent: Option<&str>) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.parent_table = parent.map(str::to_string);
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

    // --- build_table_rows helpers ---

    fn make_table_depth(name: &str, parent: Option<&str>, depth: usize) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], depth);
        t.parent_table = parent.map(str::to_string);
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
        let (ov, ov_n, sel, abs, casc, an): (HashMap<String, UserOverride>, _, _, _, _, _) =
            (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        build_table_rows(schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &an,
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
        let (ov, sel, abs, casc, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &overflow, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &an,
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
        let (sel, abs, casc, an) = (HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &overrides, overflow_names: &overflow, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &an,
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
        let (ov, ov_n, sel, abs, casc, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &casc, filter: "user", show_warn_only: false, anomaly_counts: &an,
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
        let (ov, sel, abs, casc, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &overflow, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &casc, filter: "", show_warn_only: true, anomaly_counts: &an,
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
        let (ov, ov_n, abs, casc, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &selected,
            absorbed_names: &abs, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &an,
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
        let (ov, ov_n, sel, casc, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &absorbed, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(rows[0].visible,                    "parent must be visible");
        assert!(rows[1].visible,                    "absorbed child_a must remain visible");
        assert_eq!(rows[1].badge_lbl, "merged",     "absorbed must show merged badge");
        assert_eq!(rows[1].row_cls,   "muted",      "absorbed must be muted");
        assert_eq!(rows[2].badge_lbl, "merged");
    }

    #[test]
    fn cascaded_child_shows_dedicated_skip_badge_and_is_muted() {
        let schemas = vec![make_table("parent", None), make_table("child", Some("parent"))];
        let cascaded = HashSet::from(["child".to_string()]);
        let (ov, ov_n, sel, abs, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &cascaded, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        let child = rows.iter().find(|r| r.name == "child").unwrap();
        assert_eq!(child.badge_cls, "skip", "cascaded child must show a skip-styled badge");
        assert_eq!(child.badge_lbl, "skip ↳", "badge must be visually distinct from a direct user Skip");
        assert_eq!(child.row_cls, "muted", "cascaded child row must be muted like an absorbed row");
    }

    #[test]
    fn non_absorbed_table_has_normal_badge() {
        let schemas = vec![make_table("standalone", None)];
        let absorbed: HashSet<String> = HashSet::new();
        let (ov, ov_n, sel, casc, an) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashMap::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &absorbed, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &an,
        });
        assert!(rows[0].visible);
        assert_ne!(rows[0].badge_lbl, "merged");
    }

    #[test]
    fn anomaly_count_populated_from_map() {
        let schemas = vec![make_table("orders", None), make_table("users", None)];
        let mut anomalies = HashMap::new();
        anomalies.insert("orders".to_string(), 7u64);
        let (ov, ov_n, sel, abs, casc) = (HashMap::new(), HashSet::new(), HashSet::new(), HashSet::new(), HashSet::new());
        let rows = build_table_rows(&schemas, &TableRowsCtx {
            overrides: &ov, overflow_names: &ov_n, selected_indices: &sel,
            absorbed_names: &abs, cascaded_names: &casc, filter: "", show_warn_only: false, anomaly_counts: &anomalies,
        });
        assert_eq!(rows[0].anomaly_count, 7, "orders must carry anomaly count");
        assert_eq!(rows[1].anomaly_count, 0, "users has no anomaly");
    }
}
