use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use json2sql::schema::table_schema::{TableSchema, WideStrategy};

use crate::state::AppState;

#[derive(Serialize)]
pub struct TableSummary {
    pub name: String,
    pub path: Vec<String>,
    pub depth: usize,
    pub parent_table: Option<String>,
    pub strategy: String,
    pub column_count: usize,
    pub is_wide: bool,
    /// true si l'utilisateur a explicitement overridé la stratégie
    pub is_overridden: bool,
    /// true si cette table est absorbée (enfant d'une table Jsonb)
    pub is_absorbed: bool,
}

impl TableSummary {
    fn from_table(t: &TableSchema, override_strategy: Option<&str>, is_absorbed: bool) -> Self {
        Self {
            name: t.name.clone(),
            path: t.path.clone(),
            depth: t.depth,
            parent_table: t.parent_table.clone(),
            strategy: override_strategy
                .unwrap_or_else(|| strategy_label(&t.wide_strategy))
                .to_string(),
            column_count: t.columns.len(),
            is_wide: t.wide_strategy.is_wide(),
            is_overridden: override_strategy.is_some(),
            is_absorbed,
        }
    }
}

fn strategy_label(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns => "Columns",
        WideStrategy::Pivot => "Pivot",
        WideStrategy::Jsonb => "Jsonb",
        WideStrategy::StructuredPivot(_) => "StructuredPivot",
        WideStrategy::KeyedPivot(_) => "KeyedPivot",
        WideStrategy::AutoSplit { .. } => "AutoSplit",
        WideStrategy::Ignore => "Ignore",
    }
}

#[derive(Deserialize, Default)]
pub struct TableFilter {
    /// Recherche textuelle sur le nom de la table
    pub search: Option<String>,
    /// Filtre : "wide" | "alerts" | "overridden" | "auto"
    pub filter: Option<String>,
}

/// GET /api/schema/tables — liste avec filtres optionnels.
pub async fn list_tables(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TableFilter>,
) -> Json<Vec<TableSummary>> {
    let overrides = state.strategy_overrides.read().unwrap();

    let absorbed = state.absorbed_table_names();

    let summaries: Vec<TableSummary> = state
        .tables()
        .iter()
        .filter(|t| {
            // Filtre recherche textuelle
            if let Some(ref search) = params.search {
                if !t.name.contains(search.as_str()) {
                    return false;
                }
            }
            // Filtre par catégorie
            match params.filter.as_deref() {
                Some("wide") => t.wide_strategy.is_wide(),
                Some("overridden") => overrides.contains_key(&t.name),
                Some("auto") => !overrides.contains_key(&t.name),
                _ => true,
            }
        })
        .map(|t| {
            let is_absorbed = absorbed.contains(&t.name);
            TableSummary::from_table(t, overrides.get(&t.name).map(|s| s.as_str()), is_absorbed)
        })
        .collect();

    Json(summaries)
}

/// GET /api/schema/tables/:name — détail complet d'une table.
pub async fn get_table(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<TableSchema>, StatusCode> {
    state
        .table_by_name(&name)
        .map(|t| Json(t.clone()))
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Deserialize)]
pub struct StrategyBody {
    /// Nouvelle stratégie ("Jsonb", "Pivot", "Columns", …) ou null/absent pour reset.
    pub strategy: Option<String>,
}

/// POST /api/schema/tables/:name/strategy — appliquer ou réinitialiser un override de stratégie.
pub async fn set_strategy(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(body): Json<StrategyBody>,
) -> StatusCode {
    if state.table_by_name(&name).is_none() {
        return StatusCode::NOT_FOUND;
    }
    let mut overrides = state.strategy_overrides.write().unwrap();
    match body.strategy {
        Some(s) if !s.is_empty() => {
            overrides.insert(name, s);
        }
        _ => {
            overrides.remove(&name);
        }
    }
    StatusCode::NO_CONTENT
}
