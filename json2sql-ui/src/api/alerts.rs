use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::alerts::{compute_alerts, Alert, AlertSeverity};
use crate::state::AppState;

#[derive(Serialize)]
pub struct AlertsResponse {
    pub blocking: Vec<Alert>,
    pub warnings: Vec<Alert>,
    pub blocking_count: usize,
    pub warning_count: usize,
}

/// GET /api/alerts — toutes les alertes regroupées par sévérité.
pub async fn list_alerts(State(state): State<Arc<AppState>>) -> Json<AlertsResponse> {
    let all = compute_alerts(&state);
    let blocking: Vec<_> = all.iter().filter(|a| a.severity == AlertSeverity::Blocking).cloned().collect();
    let warnings: Vec<_> = all.iter().filter(|a| a.severity == AlertSeverity::Warning).cloned().collect();
    let blocking_count = blocking.len();
    let warning_count = warnings.len();
    Json(AlertsResponse { blocking, warnings, blocking_count, warning_count })
}
