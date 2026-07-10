//! Pass 2 configuration — parameters, validation and per-worker threshold derivation.
//!
//! Fonctions :
//! - struct `Pass2Config` — all parameters controlling a Pass 2 run.
//! - fn `effective_worker_threshold` — splits the flush threshold across workers.
//! - fn `validate_run_params` — validates input parameters (e.g. parallel > 0).
//! - fn `validate_watermarks` — validates RAM high/low watermarks and the flush threshold.
//! - fn `try_set_synchronous_commit_off` — disables `synchronous_commit` (best-effort).

use std::path::PathBuf;

use tokio_postgres::Client;

use crate::error::{J2sError, Result};

/// All parameters controlling a Pass 2 run.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Pass2Config {
    pub root_table: String,
    pub pg_schema: String,
    pub parallel: usize,
    pub anomaly_dir: Option<PathBuf>,
    /// Stop pass 2 after inserting this many root objects. None = full import.
    /// Some(0) = create tables with no rows.
    pub limit: Option<u64>,
    /// Per-table buffer size before the diskless worker sends a batch to the flusher.
    /// None = 64 MiB.
    pub mem_flush_threshold_bytes: Option<u64>,
    /// Flusher pauses workers above this RAM usage ratio. None = 0.70.
    pub ram_high_watermark: Option<f64>,
    /// Flusher unpauses workers below this RAM usage ratio. None = 0.50.
    pub ram_low_watermark: Option<f64>,
    /// Emit verbose logs (RAM tick every second, DISPATCH progress every 10k rows).
    /// Default false — high-frequency logs are noisy in normal use.
    pub verbose: bool,
    /// JSON format detected during pass1 and persisted in the schema snapshot.
    /// When `Some`, skips format re-detection. When `None` (old snapshot or direct call),
    /// pass2 detects the format by scanning the source file.
    pub hint_format: Option<crate::io::reader::JsonFormat>,
    /// When `true`, skips the `add_constraints` phase entirely (Phase D).
    /// Useful for dev/exploration runs or pipelines that apply constraints in a separate step.
    pub skip_constraints: bool,
}

/// Per-worker flush threshold: divides the global threshold by worker count so that
/// with many workers and a wide schema, each worker's per-table buffer stays proportionally
/// small and flushes reach the flusher before RAM is exhausted.
pub(super) fn effective_worker_threshold(mem_flush_threshold: u64, parallel: usize) -> u64 {
    (mem_flush_threshold / parallel as u64).max(1)
}

/// Default RAM high-watermark: log a warning when system RAM exceeds this ratio.
/// Used for observability only — the actual pause trigger is `DEFAULT_HIGH_FLUSHER_BYTES`.
pub(super) const DEFAULT_RAM_HIGH_WATERMARK: f64 = 0.70;

/// Default RAM low-watermark: kept for config validation and backward compatibility.
pub(super) const DEFAULT_RAM_LOW_WATERMARK: f64 = 0.50;

pub(super) fn validate_run_params(parallel: usize) -> Result<()> {
    if parallel == 0 {
        return Err(J2sError::InvalidInput(
            "parallel must be >= 1 (0 would produce an empty connection pool)".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_watermarks(ram_high: f64, ram_low: f64, mem_flush_threshold: u64) -> Result<()> {
    if !ram_high.is_finite() || ram_high <= 0.0 || ram_high > 1.0 {
        return Err(J2sError::InvalidInput(format!(
            "ram_high_watermark must be in (0.0, 1.0], got {ram_high}"
        )));
    }
    if !ram_low.is_finite() || ram_low <= 0.0 || ram_low >= 1.0 {
        return Err(J2sError::InvalidInput(format!(
            "ram_low_watermark must be in (0.0, 1.0), got {ram_low}"
        )));
    }
    if ram_low >= ram_high {
        return Err(J2sError::InvalidInput(format!(
            "ram_low_watermark ({ram_low}) must be < ram_high_watermark ({ram_high})"
        )));
    }
    if mem_flush_threshold == 0 {
        return Err(J2sError::InvalidInput(
            "mem_flush_threshold_bytes must be > 0".to_string(),
        ));
    }
    Ok(())
}

/// Attempt to disable synchronous commit for faster bulk-load. Non-fatal if the server
/// denies the privilege (RDS, Supabase, restricted PG) — any other error is propagated.
pub(super) async fn try_set_synchronous_commit_off(conn: &Client) -> Result<()> {
    match conn.execute("SET synchronous_commit = off", &[]).await {
        Ok(_) => Ok(()),
        Err(e) if e.as_db_error().is_some_and(|db| {
            db.code() == &tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE
        }) => {
            eprintln!("WARNING: SET synchronous_commit = off not permitted — continuing without it");
            Ok(())
        }
        Err(e) => Err(J2sError::Db(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::{Pass2Config, validate_run_params, validate_watermarks, effective_worker_threshold};
    use super::{DEFAULT_RAM_HIGH_WATERMARK, DEFAULT_RAM_LOW_WATERMARK};

    // -------------------------------------------------------------------------
    // validate_watermarks tests
    // -------------------------------------------------------------------------

    #[test]
    fn watermarks_valid_defaults_pass() {
        assert!(validate_watermarks(
            DEFAULT_RAM_HIGH_WATERMARK,
            DEFAULT_RAM_LOW_WATERMARK,
            64 * 1024 * 1024,
        ).is_ok());
    }

    #[test]
    #[allow(clippy::float_cmp)] // exact const value check, not an approximate computation
    fn default_ram_high_watermark_is_logging_threshold() {
        // RAM watermarks are now informational only — actual pause uses total_buffered.
        assert_eq!(DEFAULT_RAM_HIGH_WATERMARK, 0.70);
    }

    #[test]
    #[allow(clippy::float_cmp)] // exact const value check, not an approximate computation
    fn default_ram_low_watermark_kept_for_config_compat() {
        assert_eq!(DEFAULT_RAM_LOW_WATERMARK, 0.50);
    }

    #[test]
    fn watermarks_high_above_one_is_invalid() {
        assert!(validate_watermarks(1.01, 0.70, 1024).is_err());
    }

    #[test]
    fn watermarks_high_at_one_is_valid() {
        assert!(validate_watermarks(1.0, 0.70, 1024).is_ok());
    }

    #[test]
    fn watermarks_high_zero_is_invalid() {
        assert!(validate_watermarks(0.0, 0.0, 1024).is_err());
    }

    #[test]
    fn watermarks_high_nan_is_invalid() {
        assert!(validate_watermarks(f64::NAN, 0.70, 1024).is_err());
    }

    #[test]
    fn watermarks_high_inf_is_invalid() {
        assert!(validate_watermarks(f64::INFINITY, 0.70, 1024).is_err());
    }

    #[test]
    fn watermarks_low_geq_high_is_invalid() {
        assert!(validate_watermarks(0.80, 0.80, 1024).is_err());
        assert!(validate_watermarks(0.80, 0.90, 1024).is_err());
    }

    #[test]
    fn watermarks_threshold_zero_is_invalid() {
        assert!(validate_watermarks(0.85, 0.70, 0).is_err());
    }

    #[test]
    fn watermarks_threshold_one_is_valid() {
        assert!(validate_watermarks(0.85, 0.70, 1).is_ok());
    }

    // -------------------------------------------------------------------------
    // validate_run_params tests
    // -------------------------------------------------------------------------

    #[test]
    fn run_params_parallel_zero_is_invalid() {
        assert!(matches!(
            validate_run_params(0),
            Err(crate::error::J2sError::InvalidInput(_))
        ));
    }

    #[test]
    fn run_params_parallel_one_is_valid() {
        assert!(validate_run_params(1).is_ok());
    }

    // -------------------------------------------------------------------------
    // Pass2Config tests
    // -------------------------------------------------------------------------

    #[test]
    fn pass2_config_limit_none_means_full_import() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            limit: None,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: None,
            skip_constraints: false,
        };
        assert!(cfg.limit.is_none());
    }

    #[test]
    fn pass2_config_limit_zero_means_ddl_only() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            limit: Some(0),
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: None,
            skip_constraints: false,
        };
        assert_eq!(cfg.limit, Some(0));
    }

    #[test]
    fn pass2_config_hint_format_skips_detection() {
        use crate::io::reader::JsonFormat;
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            limit: None,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: Some(JsonFormat::Array),
            skip_constraints: false,
        };
        assert_eq!(cfg.hint_format, Some(JsonFormat::Array));
    }

    #[test]
    fn pass2_config_skip_constraints_false_means_run_constraints() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            limit: None,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: None,
            skip_constraints: false,
        };
        assert!(!cfg.skip_constraints, "skip_constraints: false means constraints must run");
    }

    #[test]
    fn pass2_config_skip_constraints_true_means_skip() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            limit: None,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: None,
            skip_constraints: true,
        };
        assert!(cfg.skip_constraints, "skip_constraints: true means constraints must be skipped");
    }

    #[test]
    fn pass2_config_serde_round_trip() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 4,
            anomaly_dir: None,
            limit: Some(1000),
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: None,
            skip_constraints: true,
        };
        let json = serde_json::to_string(&cfg).expect("serialize Pass2Config");
        let back: Pass2Config = serde_json::from_str(&json).expect("deserialize Pass2Config");
        assert_eq!(back.root_table, "root");
        assert_eq!(back.pg_schema, "public");
        assert_eq!(back.parallel, 4);
        assert_eq!(back.limit, Some(1000));
        assert!(back.skip_constraints);
    }

    // -------------------------------------------------------------------------
    // effective_worker_threshold tests
    // -------------------------------------------------------------------------

    #[test]
    fn effective_worker_threshold_divides_by_parallel() {
        assert_eq!(effective_worker_threshold(64 * 1024 * 1024, 8), 8 * 1024 * 1024);
    }

    #[test]
    fn effective_worker_threshold_single_worker_unchanged() {
        assert_eq!(effective_worker_threshold(64 * 1024 * 1024, 1), 64 * 1024 * 1024);
    }

    #[test]
    fn effective_worker_threshold_16_workers_reproduces_fix() {
        // 64 MB / 16 workers = 4 MB per worker — tables flush before deadlock
        assert_eq!(effective_worker_threshold(64 * 1024 * 1024, 16), 4 * 1024 * 1024);
    }

    #[test]
    fn effective_worker_threshold_minimum_is_one() {
        // threshold=1 / parallel=100 → 0, clamped to 1
        assert_eq!(effective_worker_threshold(1, 100), 1);
    }
}
