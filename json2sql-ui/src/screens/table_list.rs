/// Shared left-panel component: hierarchical table list with strategy badges.
///
/// Each caller pre-computes `Vec<TableRowEntry>` from its own data (schemas,
/// strategy overrides, overflow warnings, etc.) and passes it here for rendering.
/// The component is display-only — selection logic lives in the caller's `on_select`.
use dioxus::prelude::*;
use crate::theme;

/// Display-ready description of one row in the table list.
#[derive(Clone, Debug, PartialEq)]
pub struct TableRowEntry {
    /// Index into the original schemas slice — used as the key and returned via on_select.
    pub index: usize,
    pub name: String,
    pub depth: usize,
    pub is_last_child: bool,
    pub is_selected: bool,
    pub badge_label: &'static str,
    pub badge_color: &'static str,
    /// True for routing containers (MultiKeyedPivot with no data columns) — renders dimmed.
    pub dim: bool,
}

#[component]
pub fn TableListPanel(entries: Vec<TableRowEntry>, on_select: EventHandler<usize>) -> Element {
    rsx! {
        div {
            style: "flex:0 1 25%;min-width:0;box-sizing:border-box;background:{theme::BG_SIDEBAR};overflow-y:auto;padding:4px 0;",
            for entry in entries {
                {
                    let idx = entry.index;
                    let row_bg = if entry.is_selected {
                        format!("background:{};", theme::SELECTED_BG)
                    } else {
                        "background:transparent;".to_string()
                    };
                    let accent = if entry.is_selected {
                        format!("border-left:2px solid {};", theme::SECONDARY)
                    } else {
                        "border-left:2px solid transparent;".to_string()
                    };
                    let parent_indent = entry.depth.saturating_sub(1) * 14 + 8;
                    let connector = if entry.depth == 0 {
                        ""
                    } else if entry.is_last_child {
                        "└─"
                    } else {
                        "├─"
                    };
                    let opacity = if entry.dim { "opacity:0.6;" } else { "" };
                    let name_color = if entry.dim { theme::ON_SURFACE_DIM } else { theme::ON_SURFACE };
                    rsx! {
                        div {
                            key: "{idx}",
                            style: "display:flex;align-items:center;gap:4px;padding:5px 8px 5px {parent_indent}px;cursor:pointer;{row_bg}{accent}{opacity}",
                            onclick: move |_| on_select.call(idx),
                            if entry.depth > 0 {
                                span {
                                    style: "font-size:0.6875rem;color:{theme::ON_SURFACE_DIM};flex-shrink:0;line-height:1;",
                                    "{connector}"
                                }
                            }
                            span {
                                style: "font-family:{theme::FONT_CODE};font-size:0.75rem;color:{name_color};flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;",
                                "{entry.name}"
                            }
                            span {
                                style: "font-size:0.5625rem;font-weight:700;letter-spacing:0.04em;color:{theme::ON_PRIMARY};background:{entry.badge_color};padding:1px 4px;border-radius:2px;flex-shrink:0;",
                                "{entry.badge_label}"
                            }
                        }
                    }
                }
            }
        }
    }
}
