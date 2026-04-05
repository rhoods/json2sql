use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;

use crate::state::{AppState, Group, GroupKind, GroupOrigin};

/// GET /api/groups — liste tous les groupes.
pub async fn list_groups(State(state): State<Arc<AppState>>) -> Json<Vec<Group>> {
    let groups = state.groups.read().unwrap();
    Json(groups.values().cloned().collect())
}

#[derive(Deserialize)]
pub struct CreateGroupRequest {
    pub name: String,
    pub kind: GroupKind,
    pub table_members: Vec<String>,
    pub strategy: Option<String>,
}

/// POST /api/groups — créer un groupe manuel.
pub async fn create_group(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateGroupRequest>,
) -> Result<Json<Group>, StatusCode> {
    let id = format!("manual_{}", uuid::Uuid::new_v4().simple());
    let group = Group {
        id: id.clone(),
        name: req.name,
        kind: req.kind,
        table_members: req.table_members,
        strategy: req.strategy,
        origin: GroupOrigin::Manual,
    };
    state.groups.write().unwrap().insert(id, group.clone());

    // Propager l'override de stratégie aux tables membres
    if let Some(ref strategy) = group.strategy {
        let mut overrides = state.strategy_overrides.write().unwrap();
        for table in &group.table_members {
            overrides.insert(table.clone(), strategy.clone());
        }
    }

    Ok(Json(group))
}

#[derive(Deserialize)]
pub struct UpdateGroupRequest {
    pub name: Option<String>,
    pub table_members: Option<Vec<String>>,
    pub strategy: Option<String>,
}

/// PUT /api/groups/:id — modifier nom, membres, ou stratégie.
pub async fn update_group(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(req): Json<UpdateGroupRequest>,
) -> Result<Json<Group>, StatusCode> {
    let mut groups = state.groups.write().unwrap();
    let group = groups.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;

    if let Some(name) = req.name {
        group.name = name;
    }
    if let Some(members) = req.table_members {
        group.table_members = members;
    }
    if req.strategy.is_some() {
        group.strategy = req.strategy;
    }
    // Marquer comme modifié si c'était auto-détecté
    if group.origin == GroupOrigin::AutoDetected {
        group.origin = GroupOrigin::Modified;
    }

    // Propager l'override de stratégie à toutes les tables membres
    if let Some(ref strategy) = group.strategy {
        let mut overrides = state.strategy_overrides.write().unwrap();
        for table in &group.table_members {
            overrides.insert(table.clone(), strategy.clone());
        }
    }

    Ok(Json(group.clone()))
}

/// DELETE /api/groups/:id — supprimer un groupe et retirer les overrides de ses membres.
pub async fn delete_group(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> StatusCode {
    let removed = state.groups.write().unwrap().remove(&id);
    match removed {
        None => StatusCode::NOT_FOUND,
        Some(group) => {
            // Retirer les overrides des tables membres — sauf si une autre groupe
            // les référence encore (on recalcule depuis les groupes restants).
            let remaining: Vec<_> = {
                let groups = state.groups.read().unwrap();
                groups
                    .values()
                    .flat_map(|g| g.table_members.iter().cloned())
                    .collect()
            };
            let still_covered: std::collections::HashSet<&str> =
                remaining.iter().map(|s| s.as_str()).collect();

            let mut overrides = state.strategy_overrides.write().unwrap();
            for table in &group.table_members {
                if !still_covered.contains(table.as_str()) {
                    overrides.remove(table);
                }
            }
            StatusCode::NO_CONTENT
        }
    }
}
