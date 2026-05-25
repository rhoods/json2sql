/// Shared left-panel component: table list with strategy badges.
///
/// Purely presentational — all selection logic lives in the caller.
/// Build rows with [`crate::screens::build_table_rows`] first, then pass them here.
///
/// Two layout modes controlled by `show_checkboxes`:
///   - `true`  (Strategy screen): 4 cols — name | cols | badge | warn
///   - `false` (Preview screen):  2 cols — name | badge+warn
use dioxus::prelude::*;
use crate::screens::TableRowViewModel;

#[component]
pub fn TableListPanel(
    rows: Vec<TableRowViewModel>,
    on_select: EventHandler<(usize, Modifiers)>,
    /// Called with the parent row index when the "Select children" hover-button is clicked.
    on_select_children: EventHandler<usize>,
    /// true = interactive (col count, thead); false = read-only (tree indent)
    show_checkboxes: bool,
) -> Element {
    let mut hovered: Signal<Option<usize>> = use_signal(|| None);

    rsx! {
        table { class: "t", style: "width:100%;",
            if show_checkboxes {
                thead {
                    tr {
                        th { "name" }
                        th { class: "ta-r", "cols" }
                        th { "strategy" }
                        th { "⚠" }
                    }
                }
            }
            tbody {
                for row in rows {
                    if row.visible {
                        {
                            let i            = row.index;
                            let name         = row.name.clone();
                            let cls          = row.row_cls;
                            let indent       = row.indent_px;
                            let conn         = row.connector;
                            let tc_cls       = if conn.starts_with('└') { "tc tc-last" } else { "tc tc-mid" };
                            let is_wide      = row.is_wide;
                            let cnt          = row.col_count;
                            let bcls         = row.badge_cls;
                            let blbl         = row.badge_lbl;
                            let warn         = row.has_warn;
                            let has_children = row.has_children;
                            let is_hovered   = *hovered.read() == Some(i);
                            rsx! {
                                tr {
                                    key: "{name}",
                                    class: "{cls}",
                                    style: "cursor:pointer;",
                                    onclick: move |evt: MouseEvent| on_select.call((i, evt.modifiers())),
                                    onmouseenter: move |_| hovered.set(Some(i)),
                                    onmouseleave: move |_| hovered.set(None),
                                    if show_checkboxes {
                                        // Interactive layout: 4 columns
                                        td { style: "padding-left:{indent}px;",
                                            div { class: "row gap-xs",
                                                if !conn.is_empty() {
                                                    span { class: "{tc_cls}" }
                                                }
                                                span { class: "mono", "{name}" }
                                                if has_children && is_hovered {
                                                    button {
                                                        class: "btn ghost sq",
                                                        style: "font-size:10px;padding:1px 5px;height:18px;line-height:1;",
                                                        title: "Select children",
                                                        onclick: move |evt| {
                                                            evt.stop_propagation();
                                                            on_select_children.call(i);
                                                        },
                                                        "⊕ children"
                                                    }
                                                }
                                            }
                                        }
                                        td {
                                            class: "ta-r mono",
                                            style: if is_wide { "color:var(--warning);font-weight:600;" } else { "color:var(--fg-2);" },
                                            "{cnt}"
                                        }
                                        td { span { class: "badge {bcls}", "{blbl}" } }
                                        td {
                                            if warn {
                                                span { class: "badge warn sq", style: "height:16px;font-size:10px;", "⚠" }
                                            }
                                        }
                                    } else {
                                        // Read-only layout: 2 columns
                                        td {
                                            style: "padding-left:{indent}px;display:flex;align-items:center;gap:4px;font-family:var(--font-code);font-size:var(--fs-xs);white-space:nowrap;",
                                            if !conn.is_empty() {
                                                span { class: "{tc_cls}" }
                                            }
                                            span { "{name}" }
                                        }
                                        td { style: "white-space:nowrap;text-align:right;",
                                            span { class: "badge {bcls}", "{blbl}" }
                                            if warn {
                                                span { class: "badge warn", style: "margin-left:4px;", "⚠" }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
