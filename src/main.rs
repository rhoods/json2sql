//! CLI entry point for json2sql.
//!
//! Parses arguments via [`cli`], converts to [`pipeline::PipelineConfig`], and dispatches
//! to [`pipeline::run_pipeline`]. The `inspect` subcommand is handled inline here.
//!
//! Fonctions :
//! - fn `main` — parse la CLI, dispatch vers `inspect` ou vers le pipeline d'import complet.
//! - fn `run_inspect` — exécute Pass 1 seule (sans stratégies ni overrides) et affiche un résumé.
//! - fn `write_inspect_outputs` — écrit le schéma inféré (stdout ou fichier) et le résumé d'anomalies.
//! - fn `write_sample_file` — écrit les objets JSON échantillonnés en NDJSON.
#![forbid(unsafe_code)]
#![deny(dead_code)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(clippy::nursery)]

mod anomaly;
mod cli;
mod db;
mod error;
mod io;
mod pass1;
mod pass2;
mod pipeline;
mod schema;

use clap::Parser;
use cli::{Cli, Commands};
use error::Result;
use schema::strategies::parse_disabled_strategies;

#[tokio::main]
#[allow(clippy::too_many_lines)] // CLI dispatch: each branch handles a distinct subcommand
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Inspect { ref input, limit, ref table, text_threshold, ref sample_output, ref output }) => {
            let root = table.clone().unwrap_or_else(|| {
                input.file_stem().and_then(|s| s.to_str()).unwrap_or("root").to_string()
            });
            run_inspect(input, &root, text_threshold, limit, sample_output.as_deref(), output.as_deref())
                .map_err(|e| anyhow::anyhow!("{e}"))
        }
        None => {
            // Validate --disable-strategy and --array-as-pg-array before any file I/O.
            reject_array_as_pg_array(cli.array_as_pg_array)?;
            let disabled_strategies = parse_disabled_strategies(&cli.disable_strategy)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let root_table = cli.root_table_name();
            let cfg = pipeline::PipelineConfig {
                input: cli.input,
                root_table,
                db_url: cli.db_url,
                pg_schema: cli.schema,
                drop_existing: cli.drop_existing,
                dry_run: cli.dry_run,
                text_threshold: cli.text_threshold,
                array_as_pg_array: cli.array_as_pg_array,
                depth_limit: cli.depth_limit,
                wide_column_threshold: cli.wide_column_threshold,
                sibling_threshold: cli.sibling_threshold,
                sibling_jaccard: cli.sibling_jaccard,
                stable_threshold: cli.stable_threshold,
                rare_threshold: cli.rare_threshold,
                num_workers: cli.workers,
                disabled_strategies,
                parallel: cli.parallel,
                anomaly_dir: cli.anomaly_dir,
                limit: cli.limit,
                anomaly_format: cli.anomaly_format,
                anomaly_output: cli.anomaly_output,
                max_anomaly_rate: cli.max_anomaly_rate,
                schema_config: cli.schema_config,
                schema_output: cli.schema_output,
                schema_input: cli.schema_input,
                schema_report: cli.schema_report,
                schema_report_output: cli.schema_report_output,
                skip_constraints: cli.no_constraints,
            };
            pipeline::run_pipeline(cfg).await.map_err(|e| anyhow::anyhow!("{e}"))
        }
    }
}

/// `--array-as-pg-array` is temporarily disabled: it silently drops data (`AutoSplit`
/// companion tables) or crashes DDL (flat tables) when a normally-scalar field has an
/// occasional array occurrence. See issue #48 for the root-cause fix.
fn reject_array_as_pg_array(enabled: bool) -> anyhow::Result<()> {
    if enabled {
        anyhow::bail!(
            "--array-as-pg-array est temporairement désactivé (voir issue #48 : \
             perte de données silencieuse ou crash DDL sur hétérogénéité scalaire/array)"
        );
    }
    Ok(())
}

fn run_inspect(
    path: &std::path::Path,
    root_table: &str,
    text_threshold: u32,
    limit: usize,
    sample_output: Option<&std::path::Path>,
    output: Option<&std::path::Path>,
) -> Result<()> {
    eprintln!("Inspecting '{}' (limit: {} objects)...", path.display(), limit);
    let result = pass1::runner::run_inspect(
        path,
        &pass1::runner::Pass1Config {
            root_table: root_table.to_string(),
            registry: schema::registry::RegistryConfig {
                text_threshold,
                array_as_pg_array: false,
                wide_column_threshold: usize::MAX,
                sibling_threshold: usize::MAX,
                sibling_jaccard: 1.0,
                stable_threshold: 0.0,
                rare_threshold: 0.0,
                disabled_strategies: std::collections::HashSet::new(),
            },
            num_workers: None,
        },
        limit,
    )?;

    eprintln!("\nScanned {} object(s) → {} table(s) detected\n", result.rows_scanned, result.schemas.len());
    write_inspect_outputs(&result, output, sample_output)
}

fn write_inspect_outputs(
    result: &pass1::runner::InspectResult,
    output: Option<&std::path::Path>,
    sample_output: Option<&std::path::Path>,
) -> Result<()> {
    use std::io::Write;
    let mut schema_out: Box<dyn Write> = match output {
        Some(p) => Box::new(std::io::BufWriter::new(
            std::fs::File::create(p).map_err(error::J2sError::Io)?
        )),
        None => Box::new(std::io::stdout()),
    };
    for schema in &result.schemas {
        let data_cols: Vec<_> = schema.data_columns().collect();
        writeln!(schema_out, "┌─ {} ({} columns)", schema.name, data_cols.len()).map_err(error::J2sError::Io)?;
        if let Some(ref parent) = schema.parent_table {
            writeln!(schema_out, "│  parent: {parent}").map_err(error::J2sError::Io)?;
        }
        for col in &data_cols {
            writeln!(schema_out, "│  {:30} {}", col.name, col.pg_type.as_sql()).map_err(error::J2sError::Io)?;
        }
        writeln!(schema_out).map_err(error::J2sError::Io)?;
    }
    if let Some(p) = output { eprintln!("Schema written → {}", p.display()); }
    if result.anomaly_count > 0 {
        eprintln!("⚠ {} column(s) with mixed types detected (re-run with --schema-report for details)", result.anomaly_count);
    } else {
        eprintln!("✓ No type anomalies detected");
    }
    if let Some(out_path) = sample_output {
        write_sample_file(&result.sampled_objects, out_path)?;
    }
    Ok(())
}

fn write_sample_file(objects: &[serde_json::Value], out_path: &std::path::Path) -> Result<()> {
    use std::io::Write;
    let file = std::fs::File::create(out_path).map_err(error::J2sError::Io)?;
    let mut writer = std::io::BufWriter::new(file);
    for obj in objects {
        serde_json::to_writer(&mut writer, obj).map_err(|e| error::J2sError::Json { source: e, position: 0 })?;
        writeln!(writer).map_err(error::J2sError::Io)?;
    }
    eprintln!("Sample written: {} objects → {}", objects.len(), out_path.display());
    Ok(())
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    #[test]
    fn reject_array_as_pg_array_errors_when_enabled() {
        let err = reject_array_as_pg_array(true).unwrap_err();
        assert!(err.to_string().contains("array-as-pg-array"));
        assert!(err.to_string().contains("#48"));
    }

    #[test]
    fn reject_array_as_pg_array_ok_when_disabled() {
        assert!(reject_array_as_pg_array(false).is_ok());
    }
}
