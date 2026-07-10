//! CLI-independent import pipeline.
//!
//! [`PipelineConfig`] holds all resolved parameters (no `clap` types). [`run_pipeline`]
//! orchestrates Pass 1 → overrides → Pass 2 and can be called directly from tests or
//! other callers without going through the CLI.
//!
//! Fonctions :
//! - struct `PipelineConfig` — tous les paramètres résolus d'un import complet json2sql.
//! - fn `run_pipeline` — enchaîne stdin → Pass 1 → overrides → dry-run ou DB → Pass 2 → rapport.
//!
//! Pass 2 :
//! - fn `connect_and_create_tables` — connecte PG et crée les tables (DDL sans contraintes).
//! - fn `run_pass2` — lance l'insertion des données.
//! - fn `finalize_pass2` — rapport final + vérification du taux d'anomalie max.
//!
//! Résolution d'entrée :
//! - fn `resolve_input` — bufferise stdin dans un fichier temporaire si aucun chemin n'est fourni.
//!
//! Pass 1 :
//! - fn `load_or_infer_schema` — lance Pass 1 ou restaure un snapshot, affiche le résumé.
//! - fn `restore_from_snapshot` — charge un `SchemaSnapshot` et ré-applique les overrides persistés.
//! - fn `run_pass1_workers` — lance Pass 1 séquentiel ou parallèle selon `num_workers`.
//! - fn `print_schema_summary` — affiche les tables inférées.
//!
//! Warnings Pass 1 :
//! - fn `report_pass1_warnings` — dispatch vers les 5 warns ci-dessous.
//! - fn `warn_truncated_names` — avertit des noms de table tronqués.
//! - fn `warn_column_collisions` — avertit des collisions de noms de colonnes.
//! - fn `warn_overflow` — avertit du dépassement du plafond de 1600 colonnes.
//! - fn `warn_skip_cascade` — avertit des vrais enfants cascadés par un `Skip` (#47).
//! - fn `warn_depth` — avertit d'une profondeur de nesting excessive.
//!
//! Post-traitement schéma :
//! - fn `post_process_schema` — dispatch overrides → snapshot → rapport.
//! - fn `apply_schema_config` — applique le TOML de surcharges.
//! - fn `save_schema_snapshot` — sauvegarde le snapshot si demandé.
//! - fn `emit_schema_report` — écrit le rapport de stats (stderr ou fichier).
//!
//! Dry-run & rapports :
//! - fn `print_dry_run_ddl` — affiche le DDL sans connexion DB.
//! - fn `aggregate_anomaly_stats` — agrège les anomalies par table (triées par nombre décroissant).
//! - fn `report_pass2_results` — affiche le résumé d'import (lignes/anomalies par table).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::anomaly::reporter::AnomalyFormat;
use crate::error::{J2sError, Result};
use crate::pass1::runner::{Pass1Config, Pass1Result};
use crate::schema::config::SkipCascadeWarning;
use crate::schema::finalizer::OverflowWarning;
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::strategies::StrategyName;
use crate::schema::table_schema::TableSchema;

/// All parameters for a full json2sql import, resolved from CLI args or built directly.
#[allow(clippy::struct_excessive_bools)] // CLI flags require individual bools; grouping would obscure semantics
pub struct PipelineConfig {
    /// Input JSON file. `None` → read from stdin (buffered to a temp file).
    pub input: Option<PathBuf>,
    /// Root table name.
    pub root_table: String,
    /// `PostgreSQL` connection URL. Required unless `dry_run` is true.
    pub db_url: Option<String>,
    /// Target `PostgreSQL` schema (default: `"public"`).
    pub pg_schema: String,
    /// Drop existing tables before import.
    pub drop_existing: bool,
    /// Print DDL to stdout and return without connecting to any database.
    pub dry_run: bool,
    /// Minimum string length (bytes) to use `TEXT` instead of `VARCHAR`.
    pub text_threshold: u32,
    /// Store scalar arrays as `PostgreSQL` array columns instead of junction tables.
    pub array_as_pg_array: bool,
    /// Warn when nesting depth exceeds this value. `None` = disabled.
    pub depth_limit: Option<usize>,
    /// Wide-table column threshold for automatic `Pivot`/`Jsonb` strategy assignment.
    pub wide_column_threshold: usize,
    /// Minimum sibling count required for automatic `SiblingCollapse` merging.
    pub sibling_threshold: usize,
    /// Minimum Jaccard similarity for sibling merging.
    pub sibling_jaccard: f64,
    /// Minimum key frequency to keep a column in the main table (`AutoSplit`).
    pub stable_threshold: f64,
    /// Keys below this frequency are dropped entirely.
    pub rare_threshold: f64,
    /// Number of Pass 1 worker threads (1 = sequential).
    pub num_workers: usize,
    /// Strategies to disable during Pass 1.
    pub disabled_strategies: HashSet<StrategyName>,
    /// Number of parallel `PostgreSQL` connections for Pass 2 COPY.
    pub parallel: usize,
    /// Directory for per-table NDJSON anomaly streaming files.
    pub anomaly_dir: Option<PathBuf>,
    /// Stop Pass 2 after N root objects. `None` = full import.
    pub limit: Option<u64>,
    /// Format for the anomaly summary report.
    pub anomaly_format: AnomalyFormat,
    /// Write the anomaly summary report to this file (stdout if `None`).
    pub anomaly_output: Option<PathBuf>,
    /// Abort if the anomaly rate exceeds this threshold (0.0–1.0). `None` = disabled.
    pub max_anomaly_rate: Option<f64>,
    /// TOML file with manual type overrides applied after Pass 1.
    pub schema_config: Option<PathBuf>,
    /// Save the Pass 1 schema snapshot to this file for later reuse.
    pub schema_output: Option<PathBuf>,
    /// Load a previously saved Pass 1 snapshot and skip Pass 1 entirely.
    pub schema_input: Option<PathBuf>,
    /// Print Pass 1 schema statistics to stderr.
    pub schema_report: bool,
    /// Write Pass 1 schema statistics to this file instead of stderr.
    pub schema_report_output: Option<PathBuf>,
    /// Skip the `add_constraints` phase at the end of Pass 2.
    pub skip_constraints: bool,
}

/// Run the full json2sql import pipeline.
///
/// Orchestrates stdin buffering → Pass 1 (schema inference or snapshot load) →
/// schema override application → DDL creation → Pass 2 (data insertion) → reporting.
pub async fn run_pipeline(mut cfg: PipelineConfig) -> Result<()> {
    let (_stdin_temp, input_path) = resolve_input(std::mem::take(&mut cfg.input))?;
    let mut pass1 = load_or_infer_schema(&cfg, &input_path)?;
    report_pass1_warnings(&pass1, cfg.depth_limit);
    let config_warnings = post_process_schema(&mut pass1, &cfg)?;
    for w in &config_warnings {
        eprintln!("WARNING: {}", w.to_message());
    }

    if cfg.dry_run {
        print_dry_run_ddl(&pass1.schemas, &cfg.pg_schema, cfg.drop_existing);
        return Ok(());
    }

    let db_url = cfg.db_url.as_deref().ok_or_else(|| {
        J2sError::InvalidInput(
            "No database URL provided. Use --db-url or set DATABASE_URL, or pass dry_run=true."
                .to_string(),
        )
    })?;
    let client = connect_and_create_tables(db_url, &pass1.schemas, &cfg.pg_schema, cfg.drop_existing).await?;
    let pass2 = run_pass2(&input_path, &pass1, &client, db_url, &cfg).await?;
    finalize_pass2(&pass2, &cfg)
}

async fn connect_and_create_tables(
    db_url: &str,
    schemas: &[TableSchema],
    pg_schema: &str,
    drop_existing: bool,
) -> Result<tokio_postgres::Client> {
    eprintln!("\nConnecting to PostgreSQL...");
    let client = crate::db::connection::connect(db_url).await?;
    eprintln!("Connected.");
    eprintln!("\nCreating tables in schema '{pg_schema}'...");
    crate::db::ddl::create_tables_no_constraints(&client, schemas, pg_schema, drop_existing, None)
        .await?;
    Ok(client)
}

async fn run_pass2(
    input_path: &Path,
    pass1: &Pass1Result,
    client: &tokio_postgres::Client,
    db_url: &str,
    cfg: &PipelineConfig,
) -> Result<crate::pass2::runner::Pass2Result> {
    eprintln!("\nPass 2: inserting data...");
    crate::pass2::runner::run(
        input_path,
        &pass1.schemas,
        client,
        db_url,
        &crate::pass2::Pass2Config {
            root_table: cfg.root_table.clone(),
            pg_schema: cfg.pg_schema.clone(),
            parallel: cfg.parallel,
            anomaly_dir: cfg.anomaly_dir.clone(),
            limit: cfg.limit,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: pass1.detected_format.clone(),
            skip_constraints: cfg.skip_constraints,
        },
        None,
    )
    .await
}

fn finalize_pass2(
    pass2: &crate::pass2::runner::Pass2Result,
    cfg: &PipelineConfig,
) -> Result<()> {
    report_pass2_results(pass2, cfg.anomaly_dir.as_deref());

    let total_anomalies = pass2.anomaly_collector.total_anomalies();
    if total_anomalies > 0 || cfg.anomaly_output.is_some() {
        crate::anomaly::reporter::write_report(
            &pass2.anomaly_collector,
            &cfg.anomaly_format,
            cfg.anomaly_output.as_deref(),
        )?;
    }

    if let Some(max_rate) = cfg.max_anomaly_rate {
        let actual_rate = pass2.anomaly_collector.overall_anomaly_rate();
        if actual_rate > max_rate {
            return Err(J2sError::InvalidInput(format!(
                "Anomaly rate {:.4}% exceeds threshold {:.4}%",
                actual_rate * 100.0,
                max_rate * 100.0
            )));
        }
    }

    eprintln!("\nDone.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Buffer stdin to a named temp file when no explicit input path is given.
/// Returns `(temp_file_guard, resolved_path)`. The guard must stay alive for
/// the duration of the pipeline so the temp file is not deleted prematurely.
fn resolve_input(
    input: Option<PathBuf>,
) -> Result<(Option<tempfile::NamedTempFile>, PathBuf)> {
    if let Some(path) = input { Ok((None, path)) } else {
        eprintln!("No input specified, reading from stdin...");
        let mut temp = tempfile::NamedTempFile::new().map_err(J2sError::Io)?;
        std::io::copy(&mut std::io::stdin(), &mut temp).map_err(J2sError::Io)?;
        let path = temp.path().to_path_buf();
        eprintln!(
            "Buffered stdin to temp file ({} bytes).",
            temp.as_file().metadata().map_or(0, |m| m.len())
        );
        Ok((Some(temp), path))
    }
}

/// Run Pass 1 or restore from snapshot, then print the inferred schema summary.
fn load_or_infer_schema(cfg: &PipelineConfig, input_path: &Path) -> Result<Pass1Result> {
    let pass1 = if let Some(ref schema_path) = cfg.schema_input {
        restore_from_snapshot(schema_path)?
    } else {
        run_pass1_workers(input_path, cfg)?
    };
    print_schema_summary(&pass1);
    Ok(pass1)
}

fn restore_from_snapshot(schema_path: &Path) -> Result<Pass1Result> {
    eprintln!("Loading schema snapshot from '{}'...", schema_path.display());
    let snap = crate::schema::persistence::load(schema_path)?;
    let mut schemas = snap.schemas;
    let skip_cascade_warnings = if snap.strategy_overrides.is_empty() {
        Vec::new()
    } else {
        crate::schema::config::apply_user_overrides(&mut schemas, &snap.strategy_overrides)
    };
    eprintln!("Snapshot loaded: {} tables, {} rows originally scanned.", schemas.len(), snap.total_rows);
    Ok(Pass1Result {
        schemas,
        total_rows: snap.total_rows,
        stats: snap.stats,
        truncated_names: snap.truncated_names,
        column_collisions: snap.column_collisions,
        overflow_warnings: snap.overflow_warnings,
        skip_cascade_warnings,
        detected_format: snap.detected_format,
    })
}

#[allow(clippy::too_many_lines)] // verbose config struct construction, not multiple responsibilities
fn run_pass1_workers(input_path: &Path, cfg: &PipelineConfig) -> Result<Pass1Result> {
    eprintln!("Pass 1: inferring schema from '{}'...", input_path.display());
    let base_cfg = Pass1Config {
        root_table: cfg.root_table.clone(),
        registry: crate::schema::registry::RegistryConfig {
            text_threshold: cfg.text_threshold,
            array_as_pg_array: cfg.array_as_pg_array,
            wide_column_threshold: cfg.wide_column_threshold,
            sibling_threshold: cfg.sibling_threshold,
            sibling_jaccard: cfg.sibling_jaccard,
            stable_threshold: cfg.stable_threshold,
            rare_threshold: cfg.rare_threshold,
            disabled_strategies: cfg.disabled_strategies.clone(),
        },
        num_workers: None,
    };
    if cfg.num_workers > 1 {
        let (workers, capped) = crate::pass1::runner::effective_workers(cfg.num_workers);
        if let Some(cap) = capped {
            eprintln!(
                "WARNING: num_workers {} exceeds available CPUs ({}); clamped to {}.",
                cfg.num_workers, cap, cap
            );
        }
        eprintln!("Using {workers} parallel workers for schema inference.");
        crate::pass1::runner::run_parallel(
            input_path,
            &Pass1Config { num_workers: Some(workers), ..base_cfg },
            None,
        )
    } else {
        crate::pass1::runner::run(input_path, &base_cfg, None)
    }
}

fn print_schema_summary(pass1: &Pass1Result) {
    eprintln!("\nInferred schema ({} tables):", pass1.schemas.len());
    for schema in &pass1.schemas {
        eprintln!(
            "  {} ({} columns){}",
            schema.name,
            schema.columns.len(),
            schema.parent_table.as_deref().map(|p| format!(" → parent: {p}")).unwrap_or_default()
        );
    }
}

/// Emit Pass 1 warnings: truncated names, column collisions, overflow, depth limit.
fn report_pass1_warnings(pass1: &Pass1Result, depth_limit: Option<usize>) {
    warn_truncated_names(&pass1.truncated_names);
    warn_column_collisions(&pass1.column_collisions);
    warn_overflow(&pass1.overflow_warnings);
    warn_skip_cascade(&pass1.skip_cascade_warnings);
    warn_depth(&pass1.schemas, depth_limit);
}

fn warn_truncated_names(names: &[TruncatedName]) {
    if names.is_empty() {
        return;
    }
    eprintln!("\nWARNING: {} table name(s) exceeded 63 chars and were truncated:", names.len());
    for t in names {
        eprintln!("  {} → {} (original: {})", t.full_name, t.pg_name, t.original_path);
    }
}

fn warn_column_collisions(collisions: &[ColumnCollision]) {
    if collisions.is_empty() {
        return;
    }
    let total: usize = collisions.iter().map(|c| c.original_names.len()).sum();
    eprintln!(
        "\nWARNING: {} column name collision(s) resolved by hash suffix ({} fields affected):",
        collisions.len(),
        total
    );
    for collision in collisions {
        eprintln!(
            "  table '{}': {} fields all sanitize to '{}' →",
            collision.table_name, collision.original_names.len(), collision.sanitized_name
        );
        for (orig, resolved) in collision.original_names.iter().zip(&collision.resolved_names) {
            eprintln!("    '{orig}' → '{resolved}'");
        }
    }
}

fn warn_overflow(warnings: &[OverflowWarning]) {
    if warnings.is_empty() {
        return;
    }
    eprintln!(
        "\nWARNING: {} table(s) exceeded PostgreSQL's 1600-column limit and were converted to JSONB:",
        warnings.len()
    );
    for w in warnings {
        eprintln!(
            "  {} ({} data columns → single 'data JSONB' column)",
            w.table_name, w.original_column_count
        );
    }
    eprintln!("  Their child tables are preserved and will still receive data.");
}

fn warn_skip_cascade(warnings: &[SkipCascadeWarning]) {
    if warnings.is_empty() {
        return;
    }
    eprintln!(
        "\nWARNING: {} table(s) removed by Skip cascaded onto real children:",
        warnings.len()
    );
    for w in warnings {
        eprintln!("  {} → {}", w.removed_table, w.cascaded_children.join(", "));
    }
}

fn warn_depth(schemas: &[TableSchema], depth_limit: Option<usize>) {
    let Some(limit) = depth_limit else { return };
    let deep: Vec<_> = schemas.iter().filter(|s| s.depth > limit).collect();
    if deep.is_empty() {
        return;
    }
    eprintln!("\nWARNING: {} table(s) exceed depth limit of {}:", deep.len(), limit);
    for s in &deep {
        eprintln!("  {} (depth {})", s.name, s.depth);
    }
}

/// Apply schema config overrides, save snapshot, and emit stats report.
fn post_process_schema(pass1: &mut Pass1Result, cfg: &PipelineConfig) -> Result<Vec<crate::schema::config::ConfigWarning>> {
    let warnings = apply_schema_config(pass1, cfg)?;
    save_schema_snapshot(pass1, cfg)?;
    emit_schema_report(pass1, cfg)?;
    Ok(warnings)
}

fn apply_schema_config(pass1: &mut Pass1Result, cfg: &PipelineConfig) -> Result<Vec<crate::schema::config::ConfigWarning>> {
    let Some(ref config_path) = cfg.schema_config else { return Ok(vec![]) };
    eprintln!("\nApplying schema overrides from '{}'...", config_path.display());
    let config = crate::schema::config::SchemaConfig::from_file(config_path)?;
    let warnings = crate::schema::config::apply_overrides_complete(&mut pass1.schemas, &config)?;
    eprintln!("Schema after overrides: {} tables", pass1.schemas.len());
    Ok(warnings)
}

fn save_schema_snapshot(pass1: &Pass1Result, cfg: &PipelineConfig) -> Result<()> {
    let Some(ref out_path) = cfg.schema_output else { return Ok(()) };
    let snapshot = crate::schema::persistence::SchemaSnapshot {
        version: crate::schema::persistence::SCHEMA_FORMAT_VERSION,
        total_rows: pass1.total_rows,
        schemas: pass1.schemas.clone(),
        truncated_names: pass1.truncated_names.clone(),
        column_collisions: pass1.column_collisions.clone(),
        stats: pass1.stats.clone(),
        strategy_overrides: std::collections::HashMap::new(),
        overflow_warnings: pass1.overflow_warnings.clone(),
        detected_format: pass1.detected_format.clone(),
    };
    crate::schema::persistence::write_snapshot(&snapshot, out_path, false)?;
    eprintln!("Schema snapshot saved to '{}'.", out_path.display());
    Ok(())
}

fn emit_schema_report(pass1: &Pass1Result, cfg: &PipelineConfig) -> Result<()> {
    if !cfg.schema_report && cfg.schema_report_output.is_none() {
        return Ok(());
    }
    if let Some(ref path) = cfg.schema_report_output {
        let mut file = std::fs::File::create(path).map_err(J2sError::Io)?;
        crate::schema::stats::write_text_report(&pass1.stats, pass1.total_rows, &mut file)
            .map_err(J2sError::Io)?;
    } else {
        crate::schema::stats::write_text_report(
            &pass1.stats,
            pass1.total_rows,
            &mut std::io::stderr(),
        )
        .map_err(J2sError::Io)?;
    }
    Ok(())
}

/// Print CREATE TABLE + ALTER TABLE FK DDL to stdout and return.
fn print_dry_run_ddl(
    schemas: &[crate::schema::table_schema::TableSchema],
    pg_schema: &str,
    drop_existing: bool,
) {
    println!("-- DDL generated by json2sql (dry-run, no database connection)\n");
    for schema in schemas {
        println!("{};", crate::db::ddl::generate_create_table(schema, pg_schema, drop_existing));
        println!();
    }
    for schema in schemas {
        if let Some(ref parent_name) = schema.parent_table {
            let fk_col = schema
                .columns
                .iter()
                .find(|c| c.is_parent_fk)
                .map_or("j2s_parent_id", |c| c.name.as_str());
            println!(
                "ALTER TABLE {schema_q}.{table_q}\n    ADD CONSTRAINT {constraint}\n    FOREIGN KEY ({fk_col_q})\n    REFERENCES {schema_q}.{parent_q} (j2s_id);",
                schema_q = crate::db::ddl::quote_ident(pg_schema),
                table_q  = crate::db::ddl::quote_ident(&schema.name),
                constraint = crate::db::ddl::quote_ident(&format!("fk_{}_parent", schema.name)),
                fk_col_q = crate::db::ddl::quote_ident(fk_col),
                parent_q = crate::db::ddl::quote_ident(parent_name),
            );
            println!();
        }
    }
}

/// Aggregate anomaly summaries by table: returns `(table, anomaly_count, total_rows)` sorted
/// by anomaly count descending, ties broken alphabetically.
fn aggregate_anomaly_stats(
    summaries: &[crate::anomaly::collector::AnomalySummary],
) -> Vec<(String, u64, u64)> {
    let mut by_table: std::collections::HashMap<String, (u64, u64)> =
        std::collections::HashMap::new();
    for s in summaries {
        let e = by_table.entry(s.table.clone()).or_insert((0, s.total_rows));
        e.0 += s.anomaly_count;
    }
    let mut stats: Vec<(String, u64, u64)> =
        by_table.into_iter().map(|(t, (anom, rows))| (t, anom, rows)).collect();
    stats.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    stats
}

/// Print the import summary (rows per table + top anomaly tables).
fn report_pass2_results(
    pass2: &crate::pass2::runner::Pass2Result,
    anomaly_dir: Option<&Path>,
) {
    eprintln!("\n=== Import Summary ===");
    let mut table_names: Vec<&String> = pass2.rows_per_table.keys().collect();
    table_names.sort();
    for name in &table_names {
        eprintln!("  {name}: {} rows", pass2.rows_per_table[*name]);
    }

    let total_rows: u64 = pass2.rows_per_table.values().sum();
    let total_anomalies = pass2.anomaly_collector.total_anomalies();
    eprintln!("\nTotal rows inserted: {total_rows}");
    eprintln!("Total anomalies:     {total_anomalies}");

    if total_anomalies > 0 {
        let summaries = pass2.anomaly_collector.summaries();
        let table_stats = aggregate_anomaly_stats(&summaries);

        eprintln!("\nAnomalies by table (top 10):");
        for (table, anom, rows) in table_stats.iter().take(10) {
            #[allow(clippy::cast_precision_loss)]
            let rate = if *rows > 0 { *anom as f64 / *rows as f64 * 100.0 } else { 0.0 };
            eprintln!("  {table:40} {anom:>8} anomalies / {rows:>10} rows ({rate:.2}%)");
        }
        if table_stats.len() > 10 {
            eprintln!("  ... and {} more tables with anomalies", table_stats.len() - 10);
        }
        if let Some(dir) = anomaly_dir {
            eprintln!("\nAnomaly files written to: {}", dir.display());
        }
    }

}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use crate::anomaly::collector::AnomalySummary;
    use crate::anomaly::reporter::AnomalyFormat;
    use crate::pass1::runner::Pass1Result;
    use crate::schema::config::ConfigWarning;
    use crate::schema::table_schema::TableSchema;

    fn minimal_pass1(schemas: Vec<TableSchema>) -> Pass1Result {
        Pass1Result {
            schemas,
            total_rows: 0,
            stats: vec![],
            truncated_names: vec![],
            column_collisions: vec![],
            overflow_warnings: vec![],
            skip_cascade_warnings: vec![],
            detected_format: None,
        }
    }

    fn minimal_pipeline_cfg(schema_config: Option<std::path::PathBuf>) -> PipelineConfig {
        PipelineConfig {
            input: None,
            root_table: "root".to_string(),
            db_url: None,
            pg_schema: "public".to_string(),
            drop_existing: false,
            dry_run: false,
            text_threshold: 256,
            array_as_pg_array: false,
            depth_limit: None,
            wide_column_threshold: 50,
            sibling_threshold: 3,
            sibling_jaccard: 0.8,
            stable_threshold: 0.8,
            rare_threshold: 0.01,
            num_workers: 1,
            disabled_strategies: std::collections::HashSet::new(),
            parallel: 1,
            anomaly_dir: None,
            limit: None,
            anomaly_format: AnomalyFormat::Json,
            anomaly_output: None,
            max_anomaly_rate: None,
            schema_config,
            schema_output: None,
            schema_input: None,
            schema_report: false,
            schema_report_output: None,
            skip_constraints: false,
        }
    }

    #[test]
    fn pipeline_config_skip_constraints_false_means_run_constraints() {
        let cfg = minimal_pipeline_cfg(None);
        assert!(!cfg.skip_constraints, "default skip_constraints must be false");
    }

    #[test]
    fn pipeline_config_skip_constraints_true_propagates() {
        let cfg = PipelineConfig { skip_constraints: true, ..minimal_pipeline_cfg(None) };
        assert!(cfg.skip_constraints, "skip_constraints must be settable to true");
    }

    #[test]
    fn apply_schema_config_none_returns_empty() {
        let mut pass1 = minimal_pass1(vec![]);
        let cfg = minimal_pipeline_cfg(None);
        let warnings = apply_schema_config(&mut pass1, &cfg).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn apply_schema_config_propagates_unknown_table_warning() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), "[ghost_table]\nage = \"INTEGER\"\n").unwrap();

        let mut pass1 = minimal_pass1(vec![
            TableSchema::new("real_table".to_string(), vec!["real_table".to_string()], 0),
        ]);
        let cfg = minimal_pipeline_cfg(Some(tmp.path().to_path_buf()));
        let warnings = apply_schema_config(&mut pass1, &cfg).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownTable("ghost_table".to_string())]);
    }

    fn summary(table: &str, anomaly_count: u64, total_rows: u64) -> AnomalySummary {
        AnomalySummary {
            table: table.to_string(),
            column: String::new(),
            expected_type: String::new(),
            anomaly_count,
            total_rows,
            anomaly_rate: 0.0,
            examples: vec![],
        }
    }

    #[test]
    fn test_aggregate_anomaly_stats_sorts_by_count_desc() {
        let summaries = vec![
            summary("a", 5, 100),
            summary("b", 20, 200),
            summary("c", 1, 50),
        ];
        let stats = aggregate_anomaly_stats(&summaries);
        assert_eq!(stats[0].0, "b");
        assert_eq!(stats[1].0, "a");
        assert_eq!(stats[2].0, "c");
    }

    #[test]
    fn test_aggregate_anomaly_stats_merges_same_table() {
        let summaries = vec![
            summary("t", 3, 10),
            summary("t", 7, 10),
        ];
        let stats = aggregate_anomaly_stats(&summaries);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0], ("t".to_string(), 10, 10));
    }

    #[test]
    fn test_aggregate_anomaly_stats_tie_broken_by_name() {
        let summaries = vec![
            summary("z", 5, 100),
            summary("a", 5, 100),
        ];
        let stats = aggregate_anomaly_stats(&summaries);
        assert_eq!(stats[0].0, "a");
        assert_eq!(stats[1].0, "z");
    }

    #[test]
    fn test_aggregate_anomaly_stats_empty() {
        assert!(aggregate_anomaly_stats(&[]).is_empty());
    }
}
