mod anomaly;
mod cli;
mod db;
mod error;
mod io;
mod pass1;
mod pass2;
mod schema;

use clap::Parser;
use cli::Cli;
use error::Result;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run(cli).await.map_err(|e| anyhow::anyhow!("{}", e))
}

async fn run(cli: Cli) -> Result<()> {
    let root_table = cli.root_table_name();

    // -------------------------------------------------------------------------
    // Resolve input: buffer stdin to a temp file if --input is omitted.
    // The temp file must stay alive for the duration of run() (both passes).
    // -------------------------------------------------------------------------
    let (_stdin_temp, input_path) = match cli.input {
        Some(ref path) => (None, path.clone()),
        None => {
            eprintln!("No --input specified, reading from stdin...");
            let mut temp = tempfile::NamedTempFile::new().map_err(error::J2sError::Io)?;
            std::io::copy(&mut std::io::stdin(), &mut temp)
                .map_err(error::J2sError::Io)?;
            let path = temp.path().to_path_buf();
            eprintln!("Buffered stdin to temp file ({} bytes).", temp.as_file().metadata().map(|m| m.len()).unwrap_or(0));
            (Some(temp), path)
        }
    };

    // -------------------------------------------------------------------------
    // Pass 1 — Schema inference (or load from snapshot)
    // -------------------------------------------------------------------------
    let mut pass1 = if let Some(ref schema_path) = cli.schema_input {
        eprintln!("Loading schema snapshot from '{}'...", schema_path.display());
        let snap = schema::persistence::load(schema_path)?;
        eprintln!(
            "Snapshot loaded: {} tables, {} rows originally scanned.",
            snap.schemas.len(),
            snap.total_rows
        );
        pass1::runner::Pass1Result {
            schemas: snap.schemas,
            total_rows: snap.total_rows,
            stats: snap.stats,
            truncated_names: snap.truncated_names,
            column_collisions: snap.column_collisions,
        }
    } else {
        eprintln!("Pass 1: inferring schema from '{}'...", input_path.display());
        pass1::runner::run(
            &input_path,
            &root_table,
            cli.text_threshold,
            cli.array_as_pg_array,
            cli.wide_column_threshold,
            cli.sibling_threshold,
            cli.sibling_jaccard,
            cli.stable_threshold,
            cli.rare_threshold,
        )?
    };

    eprintln!("\nInferred schema ({} tables):", pass1.schemas.len());
    for schema in &pass1.schemas {
        eprintln!(
            "  {} ({} columns){}",
            schema.name,
            schema.columns.len(),
            schema
                .parent_table
                .as_deref()
                .map(|p| format!(" → parent: {}", p))
                .unwrap_or_default()
        );
    }

    // -------------------------------------------------------------------------
    // Truncated name warning
    // -------------------------------------------------------------------------
    if !pass1.truncated_names.is_empty() {
        eprintln!(
            "\nWARNING: {} table name(s) exceeded 63 chars and were truncated:",
            pass1.truncated_names.len()
        );
        for t in &pass1.truncated_names {
            eprintln!(
                "  {} → {} (original: {})",
                t.full_name, t.pg_name, t.original_path
            );
        }
    }

    // -------------------------------------------------------------------------
    // Column name collision warning
    // -------------------------------------------------------------------------
    if !pass1.column_collisions.is_empty() {
        let total: usize = pass1.column_collisions.iter().map(|c| c.original_names.len()).sum();
        eprintln!(
            "\nWARNING: {} column name collision(s) resolved by hash suffix ({} fields affected):",
            pass1.column_collisions.len(),
            total
        );
        for collision in &pass1.column_collisions {
            eprintln!(
                "  table '{}': {} fields all sanitize to '{}' →",
                collision.table_name, collision.original_names.len(), collision.sanitized_name
            );
            for (orig, resolved) in collision.original_names.iter().zip(&collision.resolved_names) {
                eprintln!("    '{}' → '{}'", orig, resolved);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Depth limit warning
    // -------------------------------------------------------------------------
    if let Some(limit) = cli.depth_limit {
        let deep: Vec<_> = pass1
            .schemas
            .iter()
            .filter(|s| s.depth > limit)
            .collect();
        if !deep.is_empty() {
            eprintln!(
                "\nWARNING: {} table(s) exceed depth limit of {}:",
                deep.len(),
                limit
            );
            for s in &deep {
                eprintln!("  {} (depth {})", s.name, s.depth);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Schema config — apply manual type overrides
    // -------------------------------------------------------------------------
    if let Some(ref config_path) = cli.schema_config {
        eprintln!("\nApplying schema overrides from '{}'...", config_path.display());
        let config = schema::config::SchemaConfig::from_file(config_path)?;
        schema::config::apply_overrides(&mut pass1.schemas, &config);
        schema::config::apply_group_overrides(&mut pass1.schemas, &config);
        // Re-run exclusion: strategy overrides may have changed Columns → Jsonb/Pivot on a parent,
        // which should now suppress all its child tables (they'd receive no data anyway).
        schema::registry::exclude_absorbed_children(&mut pass1.schemas);
        eprintln!(
            "Schema after overrides: {} tables",
            pass1.schemas.len()
        );
    }

    // -------------------------------------------------------------------------
    // Schema snapshot — save after overrides so the snapshot is Pass-2-ready
    // -------------------------------------------------------------------------
    if let Some(ref out_path) = cli.schema_output {
        schema::persistence::save(
            &pass1.schemas,
            pass1.total_rows,
            &pass1.truncated_names,
            &pass1.column_collisions,
            &pass1.stats,
            out_path,
        )?;
        eprintln!("Schema snapshot saved to '{}'.", out_path.display());
    }

    // -------------------------------------------------------------------------
    // Schema statistics report
    // -------------------------------------------------------------------------
    if cli.schema_report || cli.schema_report_output.is_some() {
        if let Some(ref path) = cli.schema_report_output {
            let mut file = std::fs::File::create(path).map_err(error::J2sError::Io)?;
            schema::stats::write_text_report(&pass1.stats, pass1.total_rows, &mut file)
                .map_err(error::J2sError::Io)?;
        } else {
            schema::stats::write_text_report(
                &pass1.stats,
                pass1.total_rows,
                &mut std::io::stderr(),
            )
            .map_err(error::J2sError::Io)?;
        }
    }

    // -------------------------------------------------------------------------
    // Dry-run — print DDL and exit
    // -------------------------------------------------------------------------
    if cli.dry_run {
        println!("-- DDL generated by json2sql (dry-run, no database connection)\n");
        for schema in &pass1.schemas {
            println!("{};", db::ddl::generate_create_table(schema, &cli.schema, cli.drop_existing));
            println!();
        }
        // FK constraints
        for schema in &pass1.schemas {
            if let Some(ref parent_name) = schema.parent_table {
                let fk_col = schema
                    .columns
                    .iter()
                    .find(|c| c.is_parent_fk)
                    .map(|c| c.name.as_str())
                    .unwrap_or("j2s_parent_id");
                println!(
                    "ALTER TABLE {schema_q}.{table_q}\n    ADD CONSTRAINT {constraint}\n    FOREIGN KEY ({fk_col_q})\n    REFERENCES {schema_q}.{parent_q} (j2s_id);",
                    schema_q = db::ddl::quote_ident(&cli.schema),
                    table_q = db::ddl::quote_ident(&schema.name),
                    constraint = db::ddl::quote_ident(&format!("fk_{}_parent", schema.name)),
                    fk_col_q = db::ddl::quote_ident(fk_col),
                    parent_q = db::ddl::quote_ident(parent_name),
                );
                println!();
            }
        }
        return Ok(());
    }

    // -------------------------------------------------------------------------
    // Connect to PostgreSQL
    // -------------------------------------------------------------------------
    let db_url = cli.db_url.as_deref().ok_or_else(|| {
        error::J2sError::InvalidInput(
            "No database URL provided. Use --db-url or set DATABASE_URL, or pass --dry-run."
                .to_string(),
        )
    })?;
    eprintln!("\nConnecting to PostgreSQL...");
    let client = db::connection::connect(db_url).await?;
    eprintln!("Connected.");

    // -------------------------------------------------------------------------
    // Create tables
    // -------------------------------------------------------------------------
    eprintln!("\nCreating tables in schema '{}'...", cli.schema);
    db::ddl::create_tables(&client, &pass1.schemas, &cli.schema, cli.drop_existing).await?;

    // -------------------------------------------------------------------------
    // Pass 2 — Data insertion
    // -------------------------------------------------------------------------
    eprintln!("\nPass 2: inserting data...");
    let pass2 = pass2::runner::run(
        &input_path,
        &root_table,
        &pass1.schemas,
        &client,
        &cli.schema,
        cli.batch_size,
        cli.transaction,
        cli.db_url.as_deref(),
        cli.parallel,
        cli.anomaly_dir.clone(),
    )
    .await?;

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------
    eprintln!("\n=== Import Summary ===");
    let mut table_names: Vec<&String> = pass2.rows_per_table.keys().collect();
    table_names.sort();
    for name in &table_names {
        let count = pass2.rows_per_table[*name];
        eprintln!("  {}: {} rows", name, count);
    }

    let total_rows: u64 = pass2.rows_per_table.values().sum();
    let total_anomalies = pass2.anomaly_collector.total_anomalies();
    eprintln!("\nTotal rows inserted: {}", total_rows);
    eprintln!("Total anomalies:     {}", total_anomalies);

    if total_anomalies > 0 {
        eprintln!(
            "Anomaly rate:        {:.4}%",
            pass2.anomaly_collector.overall_anomaly_rate() * 100.0
        );
        // Per-table breakdown, sorted by anomaly count desc
        let mut summaries = pass2.anomaly_collector.summaries();
        summaries.sort_by(|a, b| b.anomaly_count.cmp(&a.anomaly_count).then(a.table.cmp(&b.table)));
        eprintln!("\nAnomalies by table (top 10):");
        for s in summaries.iter().take(10) {
            eprintln!(
                "  {:40} {:>8} anomalies / {:>10} rows ({:.2}%)",
                s.table,
                s.anomaly_count,
                s.total_rows,
                s.anomaly_rate * 100.0,
            );
        }
        if summaries.len() > 10 {
            eprintln!("  ... and {} more tables with anomalies", summaries.len() - 10);
        }
        if let Some(ref dir) = cli.anomaly_dir {
            eprintln!("\nAnomaly files written to: {}", dir.display());
        }
    }

    // -------------------------------------------------------------------------
    // Anomaly report
    // -------------------------------------------------------------------------
    if total_anomalies > 0 || cli.anomaly_output.is_some() {
        anomaly::reporter::write_report(
            &pass2.anomaly_collector,
            &cli.anomaly_format,
            cli.anomaly_output.as_deref(),
        )?;
    }

    // -------------------------------------------------------------------------
    // Check anomaly rate threshold
    // -------------------------------------------------------------------------
    if let Some(max_rate) = cli.max_anomaly_rate {
        let actual_rate = pass2.anomaly_collector.overall_anomaly_rate();
        if actual_rate > max_rate {
            return Err(error::J2sError::InvalidInput(format!(
                "Anomaly rate {:.4}% exceeds threshold {:.4}%",
                actual_rate * 100.0,
                max_rate * 100.0
            )));
        }
    }

    eprintln!("\nDone.");
    Ok(())
}
