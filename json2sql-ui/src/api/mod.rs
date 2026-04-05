mod alerts;
mod config;
mod export;
mod groups;
mod migration;
mod presets;
mod schema;

use std::sync::Arc;
use axum::Router;
use axum::routing::{get, post, put};

use crate::state::AppState;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        // Config
        .route("/config", get(config::get_config))
        .route("/config", post(config::set_config))
        // Schema
        .route("/schema/tables", get(schema::list_tables))
        .route("/schema/tables/:name", get(schema::get_table))
        .route("/schema/tables/:name/strategy", post(schema::set_strategy))
        // Alerts
        .route("/alerts", get(alerts::list_alerts))
        // Groups
        .route("/groups", get(groups::list_groups))
        .route("/groups", post(groups::create_group))
        .route("/groups/:id", put(groups::update_group))
        .route("/groups/:id", axum::routing::delete(groups::delete_group))
        // Export
        .route("/export/ddl/:name", get(export::get_ddl))
        .route("/export/ddl-group/:id", get(export::get_group_ddl))
        .route("/export/toml", get(export::get_toml))
        // Presets
        .route("/presets", get(presets::list_presets))
        .route("/presets/:id", get(presets::get_preset))
        // Migration
        .route("/migration/start", post(migration::start_migration))
        .route("/migration/progress", get(migration::migration_progress))
        .route("/migration/status", get(migration::migration_status))
}
