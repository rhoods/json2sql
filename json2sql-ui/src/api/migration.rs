use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, Sse};
use axum::Json;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;

use json2sql::db::connection::connect;
use json2sql::db::ddl::create_tables;
use json2sql::pass2::runner;

use crate::alerts::{compute_alerts, AlertSeverity};
use crate::migration::MigrationStatus;
use crate::state::AppState;

#[derive(Deserialize)]
pub struct StartRequest {
    pub drop_existing: Option<bool>,
}

#[derive(Serialize)]
pub struct StartError {
    error: String,
    detail: Option<Vec<String>>,
}

/// POST /api/migration/start — lancer la Pass 2.
pub async fn start_migration(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartRequest>,
) -> Result<StatusCode, (StatusCode, Json<StartError>)> {
    // Vérifier les prérequis
    if !state.can_migrate() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(StartError {
                error: "json2sql-ui doit être lancé avec --input et --db-url pour migrer".into(),
                detail: None,
            }),
        ));
    }

    if state.migration.is_running() {
        return Err((
            StatusCode::CONFLICT,
            Json(StartError {
                error: "Une migration est déjà en cours".into(),
                detail: None,
            }),
        ));
    }

    // Bloquer si alertes critiques
    let blocking: Vec<_> = compute_alerts(&state)
        .into_iter()
        .filter(|a| a.severity == AlertSeverity::Blocking)
        .map(|a| format!("{}: {}", a.table, a.message))
        .collect();
    if !blocking.is_empty() {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(StartError {
                error: "Alertes bloquantes non résolues".into(),
                detail: Some(blocking),
            }),
        ));
    }

    let drop_existing = req.drop_existing.unwrap_or(false);
    let input_file = state.read_input_file().unwrap();
    let db_url = state.read_db_url().unwrap();
    let pg_schema = state.pg_schema.clone();
    let schemas = state.snapshot.schemas.clone();
    // pass2 looks up root by path.join("."), not by sanitized name
    let root_table = state
        .snapshot
        .schemas
        .first()
        .map(|s| s.path.join("."))
        .unwrap_or_default();

    // Lancer la migration en background
    let migration = Arc::clone(&state);
    tokio::spawn(async move {
        let (started, started_at_secs) = migration.migration.set_running();

        // Ticker : met à jour elapsed_secs toutes les secondes
        let ticker_migration = Arc::clone(&migration);
        let ticker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                if !ticker_migration.migration.is_running() {
                    break;
                }
                ticker_migration
                    .migration
                    .update_elapsed(started_at_secs, started.elapsed().as_secs());
            }
        });

        let result = async {
            let client = connect(&db_url).await?;
            create_tables(&client, &schemas, &pg_schema, drop_existing).await?;
            runner::run(
                &input_file,
                &root_table,
                &schemas,
                &client,
                &pg_schema,
                100_000, // flush_threshold
                false,   // use_transaction
                Some(&db_url),
                1, // parallel
            )
            .await
        }
        .await;

        ticker.abort();
        let elapsed = started.elapsed();
        match result {
            Ok(pass2) => migration.migration.set_done(elapsed, pass2.rows_per_table),
            Err(e) => migration.migration.set_failed(e.to_string(), elapsed),
        }
    });

    Ok(StatusCode::ACCEPTED)
}

/// GET /api/migration/progress — SSE : état de la migration en temps réel.
pub async fn migration_progress(
    State(state): State<Arc<AppState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.migration.receiver.clone();
    let stream = WatchStream::new(rx).map(|status| {
        let data = serde_json::to_string(&status).unwrap_or_default();
        Ok(Event::default().data(data))
    });
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

/// GET /api/migration/status — état courant (polling alternatif au SSE).
pub async fn migration_status(
    State(state): State<Arc<AppState>>,
) -> Json<MigrationStatus> {
    Json(state.migration.receiver.borrow().clone())
}
