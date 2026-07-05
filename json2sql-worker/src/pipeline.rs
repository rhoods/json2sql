//! Pipeline d'import exécuté dans le process worker : connexion PG → DDL → Pass 2 → résultat.
//!
//! Fonctions :
//! - fn `build_pg_url` — construit l'URL `postgres://` (composants percent-encodés, host IPv6 entre crochets).
//! - fn `run_pipeline` — orchestre connexion (timeout 10s) → `create_tables_no_constraints` → Pass 2, vérifie `cancel` entre chaque étape, relaie les `ProgressEvent` vers `ImportSummary` via un channel, retourne un `WorkerResult` (jamais de panic).

use std::sync::Arc;

use tokio::sync::Mutex;

use json2sql::ipc::{WorkerConfig, WorkerResult};
use json2sql::io::progress_event::ProgressEvent;

use crate::cancel::CancelToken;
use crate::summary::ImportSummary;

/// Build a `postgres://` URL from `WorkerConfig` fields and a clear-text password.
/// All components are percent-encoded so special characters do not corrupt the URL.
/// IPv6 host addresses are bracketed.
pub fn build_pg_url(cfg: &WorkerConfig, password: &str) -> String {
    use urlencoding::encode;
    let host = if cfg.pg_host.contains(':') && !cfg.pg_host.starts_with('[') {
        format!("[{}]", encode(&cfg.pg_host))
    } else {
        encode(&cfg.pg_host).into_owned()
    };
    format!(
        "postgres://{}:{}@{}:{}/{}",
        encode(&cfg.pg_user),
        encode(password),
        host,
        cfg.pg_port,
        encode(&cfg.pg_database)
    )
}

/// Run the full import pipeline inside the worker process:
/// connect to PG → DDL → pass2 → aggregate result.
///
/// Events pushed during the run are forwarded to `summary` via an internal channel.
/// Returns a `WorkerResult` describing success or error; never panics.
pub async fn run_pipeline(
    cfg: &WorkerConfig,
    summary: Arc<Mutex<ImportSummary>>,
    cancel: CancelToken,
) -> WorkerResult {
    let password = std::env::var("J2S_PG_PASSWORD").unwrap_or_default();
    let pg_url = build_pg_url(cfg, &password);

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

    // Bridge channel → ImportSummary so connection handlers can stream events.
    let sum2 = Arc::clone(&summary);
    let forwarder = tokio::spawn(async move {
        let mut rx = rx;
        while let Some(event) = rx.recv().await {
            sum2.lock().await.push(event);
        }
    });

    let outcome: Result<json2sql::pass2::runner::Pass2Result, String> = async {
        if cancel.is_cancelled() {
            return Err("cancelled".to_string());
        }

        let (client, connection) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio_postgres::connect(&pg_url, tokio_postgres::NoTls),
        )
        .await
        .map_err(|_| "PostgreSQL connection timed out (10s)".to_string())?
        .map_err(|e| e.to_string())?;

        tokio::spawn(async move { let _ = connection.await; });

        if cancel.is_cancelled() {
            return Err("cancelled".to_string());
        }

        json2sql::db::ddl::create_tables_no_constraints(
            &client,
            &cfg.schemas,
            &cfg.pg_schema,
            cfg.drop_existing,
            Some(&tx),
        )
        .await
        .map_err(|e| e.to_string())?;

        if cancel.is_cancelled() {
            return Err("cancelled".to_string());
        }

        let pass2_cfg = cfg.clone().into_pass2_config();
        json2sql::pass2::runner::run(
            &cfg.source_file,
            &cfg.schemas,
            &client,
            &pg_url,
            &pass2_cfg,
            Some(tx),
        )
        .await
        .map_err(|e| e.to_string())
    }
    .await;

    // Wait for the forwarder to drain all remaining events (including Pass2Done).
    forwarder.await.ok();

    match outcome {
        Ok(r) => WorkerResult {
            status: "success".to_string(),
            total_rows: r.rows_per_table.values().sum(),
            anomaly_count: r.anomaly_collector.total_anomalies(),
            constraint_warning_count: r.constraint_warnings.len() as u64,
            message: None,
        },
        Err(msg) => WorkerResult {
            status: "error".to_string(),
            total_rows: 0,
            anomaly_count: 0,
            constraint_warning_count: 0,
            message: Some(msg),
        },
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn base_cfg() -> WorkerConfig {
        WorkerConfig {
            source_file: PathBuf::from("/tmp/data.json"),
            root_table: "root".to_string(),
            pg_host: "localhost".to_string(),
            pg_port: 5432,
            pg_database: "mydb".to_string(),
            pg_user: "alice".to_string(),
            pg_schema: "public".to_string(),
            schemas: vec![],
            drop_existing: false,
            anomaly_dir: None,
            pass2_parallel: 4,
            import_limit: None,
            verbose_logs: false,
            hint_format: None,
            skip_constraints: false,
            socket_path: PathBuf::from("/tmp/test.sock"),
            result_file: PathBuf::from("/tmp/result.json"),
        }
    }

    #[test]
    fn build_pg_url_basic() {
        let cfg = base_cfg();
        let url = build_pg_url(&cfg, "secret");
        assert_eq!(url, "postgres://alice:secret@localhost:5432/mydb");
    }

    #[test]
    fn build_pg_url_special_chars_in_password() {
        let cfg = base_cfg();
        let url = build_pg_url(&cfg, "p@ss:word");
        assert!(url.contains("p%40ss%3Aword"), "@ and : must be percent-encoded");
        assert!(!url.contains("p@ss"), "raw @ must not appear in URL");
    }

    #[test]
    fn build_pg_url_ipv6_host() {
        let cfg = WorkerConfig {
            pg_host: "::1".to_string(),
            ..base_cfg()
        };
        let url = build_pg_url(&cfg, "pw");
        assert!(url.contains("[%3A%3A1]"), "IPv6 host must be bracketed and encoded");
    }
}
