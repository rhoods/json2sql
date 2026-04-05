use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Serialize)]
pub struct ConfigResponse {
    can_migrate: bool,
    has_db_url: bool,
    has_input: bool,
    pg_schema: String,
}

/// GET /api/config — état de la configuration (connexion, fichier source).
pub async fn get_config(State(state): State<Arc<AppState>>) -> Json<ConfigResponse> {
    Json(ConfigResponse {
        can_migrate: state.can_migrate(),
        has_db_url: state.read_db_url().is_some(),
        has_input: state.read_input_file().is_some(),
        pg_schema: state.pg_schema.clone(),
    })
}

#[derive(Deserialize)]
pub struct SetConfigRequest {
    pub db_url: Option<String>,
    pub input_file: Option<String>,
}

#[derive(Serialize)]
pub struct ConfigError {
    error: String,
}

/// POST /api/config — mettre à jour db_url et/ou input_file au runtime.
pub async fn set_config(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SetConfigRequest>,
) -> Result<StatusCode, (StatusCode, Json<ConfigError>)> {
    if let Some(url) = req.db_url {
        let url = url.trim().to_string();
        *state.db_url.write().unwrap_or_else(|e| e.into_inner()) =
            if url.is_empty() { None } else { Some(url) };
    }
    if let Some(path) = req.input_file {
        let path = path.trim().to_string();
        if path.is_empty() {
            *state.input_file.write().unwrap_or_else(|e| e.into_inner()) = None;
        } else {
            let pb = PathBuf::from(&path);
            if !pb.exists() {
                return Err((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(ConfigError {
                        error: format!("Fichier introuvable : {path}"),
                    }),
                ));
            }
            *state.input_file.write().unwrap_or_else(|e| e.into_inner()) = Some(pb);
        }
    }
    Ok(StatusCode::NO_CONTENT)
}
