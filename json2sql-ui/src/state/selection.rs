//! Table-selection logic for `SchemaState` — click, shift-click, and visible-children lookup.
//!
//! Fonctions :
//! - fn `SchemaState::apply_shift_click` — sélection multi-table par Shift+click (plage visible)
//! - fn `SchemaState::apply_click` — sélection multi-table par click / Ctrl+click
//! - fn `select_children_visible` — indices des enfants directs d'une table visibles dans la liste

use std::collections::HashSet;

use json2sql::schema::table_schema::TableSchema;

use super::SchemaState;

impl SchemaState {
    /// Apply a Shift+click range-select.
    ///
    /// Selects all entries in `visible_indices` whose index falls in
    /// `[min(anchor, i), max(anchor, i)]`, skipping rows not in `visible_indices`.
    pub fn apply_shift_click(&mut self, i: usize, anchor: usize, visible_indices: &[usize]) {
        let lo = anchor.min(i);
        let hi = anchor.max(i);
        let range: HashSet<usize> = visible_indices.iter()
            .copied()
            .filter(|&idx| idx >= lo && idx <= hi)
            .collect();
        if !range.is_empty() {
            self.selected_table_indices = range;
            self.last_selected_idx = i;
        }
    }

    /// Apply a table-row click to the selection.
    ///
    /// `ctrl` = Ctrl or Meta modifier held.
    /// Plain click replaces the selection; Ctrl+click toggles the row in/out,
    /// refusing to deselect when it is the last selected item.
    pub fn apply_click(&mut self, i: usize, ctrl: bool) {
        if ctrl {
            if self.selected_table_indices.contains(&i) {
                if self.selected_table_indices.len() > 1 {
                    self.selected_table_indices.remove(&i);
                }
            } else {
                self.selected_table_indices.insert(i);
                self.last_selected_idx = i;
            }
        } else {
            self.selected_table_indices = HashSet::from([i]);
            self.last_selected_idx = i;
        }
    }
}

/// Return the indices of direct children of `schemas[parent_idx]` that are in `visible_indices`.
pub fn select_children_visible(
    schemas: &[TableSchema],
    parent_idx: usize,
    visible_indices: &[usize],
) -> HashSet<usize> {
    let Some(parent) = schemas.get(parent_idx) else { return HashSet::new() };
    visible_indices.iter()
        .copied()
        .filter(|&i| {
            schemas.get(i)
                .and_then(|t| t.parent_table.as_ref())
                .is_some_and(|p| p == &parent.name)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- apply_click ---

    fn schema_with_selection(selected: impl IntoIterator<Item = usize>, last: usize) -> SchemaState {
        let mut s = SchemaState::default();
        s.selected_table_indices = selected.into_iter().collect();
        s.last_selected_idx = last;
        s
    }

    #[test]
    fn plain_click_replaces_selection() {
        let mut s = schema_with_selection([0, 1, 2], 2);
        s.apply_click(5, false);
        assert_eq!(s.selected_table_indices, HashSet::from([5]));
        assert_eq!(s.last_selected_idx, 5);
    }

    #[test]
    fn ctrl_click_adds_new_item() {
        let mut s = schema_with_selection([0], 0);
        s.apply_click(3, true);
        assert!(s.selected_table_indices.contains(&0));
        assert!(s.selected_table_indices.contains(&3));
        assert_eq!(s.last_selected_idx, 3);
    }

    #[test]
    fn ctrl_click_removes_selected_item() {
        let mut s = schema_with_selection([0, 3], 3);
        s.apply_click(3, true);
        assert_eq!(s.selected_table_indices, HashSet::from([0]));
    }

    #[test]
    fn ctrl_click_cannot_deselect_last_item() {
        let mut s = schema_with_selection([0], 0);
        s.apply_click(0, true);
        assert_eq!(s.selected_table_indices, HashSet::from([0]), "last item must stay selected");
    }

    // --- apply_shift_click ---

    #[test]
    fn shift_click_selects_visible_range_forward() {
        // visible: 0, 1, 3, 5 (2 and 4 filtered out)
        let visible = vec![0, 1, 3, 5];
        let mut s = schema_with_selection([0], 0);
        s.apply_shift_click(3, 0, &visible);
        assert_eq!(s.selected_table_indices, HashSet::from([0, 1, 3]));
        assert_eq!(s.last_selected_idx, 3);
    }

    #[test]
    fn shift_click_selects_visible_range_backward() {
        let visible = vec![0, 1, 3, 5];
        let mut s = schema_with_selection([5], 5);
        s.apply_shift_click(1, 5, &visible);
        assert_eq!(s.selected_table_indices, HashSet::from([1, 3, 5]));
        assert_eq!(s.last_selected_idx, 1);
    }

    #[test]
    fn shift_click_skips_invisible_indices() {
        // indices 2 and 4 are not in visible list → not selected
        let visible = vec![0, 1, 3, 5];
        let mut s = schema_with_selection([0], 0);
        s.apply_shift_click(5, 0, &visible);
        assert!(!s.selected_table_indices.contains(&2));
        assert!(!s.selected_table_indices.contains(&4));
        assert_eq!(s.selected_table_indices, HashSet::from([0, 1, 3, 5]));
    }

    #[test]
    fn shift_click_on_same_item_keeps_selection() {
        let visible = vec![0, 1, 2];
        let mut s = schema_with_selection([0], 0);
        s.apply_shift_click(0, 0, &visible);
        assert_eq!(s.selected_table_indices, HashSet::from([0]));
    }

    // --- select_children_visible ---

    fn make_schema(name: &str, parent: Option<&str>) -> TableSchema {
        let mut s = TableSchema::new(
            name.to_string(), vec![name.to_string()], if parent.is_some() { 1 } else { 0 },
        );
        s.parent_table = parent.map(|p| p.to_string());
        s
    }

    #[test]
    fn select_children_returns_direct_children() {
        let schemas = vec![
            make_schema("product", None),          // 0
            make_schema("product_image", Some("product")),  // 1
            make_schema("product_tag", Some("product")),    // 2
            make_schema("order", None),             // 3
        ];
        let visible = vec![0, 1, 2, 3];
        let result = select_children_visible(&schemas, 0, &visible);
        assert_eq!(result, HashSet::from([1, 2]));
    }

    #[test]
    fn select_children_skips_invisible() {
        let schemas = vec![
            make_schema("product", None),
            make_schema("product_image", Some("product")),  // 1
            make_schema("product_tag", Some("product")),    // 2 — filtered out
        ];
        let visible = vec![0, 1]; // 2 not visible
        let result = select_children_visible(&schemas, 0, &visible);
        assert_eq!(result, HashSet::from([1]));
    }

    #[test]
    fn select_children_returns_empty_for_leaf() {
        let schemas = vec![
            make_schema("product", None),
            make_schema("product_image", Some("product")),
        ];
        let visible = vec![0, 1];
        let result = select_children_visible(&schemas, 1, &visible);
        assert!(result.is_empty(), "leaf node has no children");
    }
}
