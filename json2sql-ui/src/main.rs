mod alerts;
mod api;
mod migration;
mod state;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use clap::Parser;
use axum::response::Html;
use axum::routing::get as get_route;

use json2sql::schema::persistence;
use json2sql::schema::persistence::SchemaSnapshot;

use state::AppState;

#[derive(Parser, Debug)]
#[command(name = "json2sql-ui", about = "IHM de configuration des stratégies de migration json2sql")]
struct Cli {
    /// Snapshot JSON pré-calculé (--schema-output de json2sql).
    /// Si absent, Pass 1 est lancé automatiquement depuis --input.
    #[arg(long, short = 's')]
    snapshot: Option<PathBuf>,

    /// Fichier JSON/NDJSON source.
    /// Requis si --snapshot est absent ; facultatif sinon (mais requis pour migrer).
    #[arg(long, short = 'i', value_name = "FILE")]
    input: Option<PathBuf>,

    /// Fichier JSON/NDJSON source (argument positionnel, équivalent à --input).
    #[arg(value_name = "INPUT_FILE")]
    input_positional: Option<PathBuf>,

    /// Sauvegarder le snapshot généré (ou chargé) dans ce fichier pour réutilisation future.
    #[arg(long)]
    snapshot_output: Option<PathBuf>,

    /// Fichier TOML d'overrides manuels (types, groupes…) appliqué après Pass 1.
    #[arg(long)]
    schema_config: Option<PathBuf>,

    /// Seuil de longueur pour TEXT vs VARCHAR (défaut : 256)
    #[arg(long, default_value_t = 256)]
    text_threshold: u32,

    /// Nombre minimum de tables sœurs pour le regroupement automatique KeyedPivot (défaut : 3)
    #[arg(long, default_value_t = 3)]
    sibling_threshold: usize,

    /// Similarité Jaccard minimale entre tables sœurs pour fusion (défaut : 0.5)
    #[arg(long, default_value_t = 0.5)]
    sibling_jaccard: f64,

    /// Seuil de colonnes larges avant stratégie automatique Pivot/Jsonb (défaut : 100)
    #[arg(long, default_value_t = 100)]
    wide_column_threshold: usize,

    /// URL de connexion PostgreSQL (requis pour lancer la migration)
    #[arg(long, env = "DATABASE_URL")]
    db_url: Option<String>,

    /// Schéma PostgreSQL cible
    #[arg(long, default_value = "public")]
    pg_schema: String,

    /// Adresse d'écoute
    #[arg(long, default_value = "127.0.0.1:3000")]
    bind: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cli = Cli::parse();

    // Fusionner l'argument positionnel avec --input
    if cli.input.is_none() {
        cli.input = cli.input_positional.take();
    }

    // ─── Charger ou générer le snapshot ────────────────────────────────────────
    let mut snapshot: SchemaSnapshot = match &cli.snapshot {
        Some(path) => {
            println!("Chargement du snapshot : {}", path.display());
            let s = persistence::load(path)
                .with_context(|| format!("Impossible de charger {}", path.display()))?;
            println!(
                "✓ {} tables chargées ({} lignes analysées)",
                s.schemas.len(),
                s.total_rows
            );
            s
        }
        None => {
            let input = cli.input.as_ref().with_context(|| {
                "Fournir --snapshot (snapshot existant) ou --input (JSON source) pour lancer Pass 1"
            })?;
            println!("Pas de snapshot — lancement de Pass 1 sur '{}'…", input.display());

            let root_table = input
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("root")
                .to_string();

            let pass1 = json2sql::pass1::runner::run(
                input,
                &root_table,
                cli.text_threshold,
                false,  // array_as_pg_array
                cli.wide_column_threshold,
                cli.sibling_threshold,
                cli.sibling_jaccard,
                0.10,   // stable_threshold
                0.001,  // rare_threshold
            )?;

            println!("✓ Pass 1 terminée : {} tables inférées", pass1.schemas.len());

            SchemaSnapshot {
                version: 1,
                schemas: pass1.schemas,
                total_rows: pass1.total_rows,
                stats: pass1.stats,
                truncated_names: pass1.truncated_names,
                column_collisions: pass1.column_collisions,
            }
        }
    };

    // ─── Appliquer les overrides TOML si fournis ───────────────────────────────
    if let Some(ref config_path) = cli.schema_config {
        println!("Application des overrides depuis '{}'…", config_path.display());
        let config = json2sql::schema::config::SchemaConfig::from_file(config_path)?;
        json2sql::schema::config::apply_overrides(&mut snapshot.schemas, &config);
        json2sql::schema::config::apply_group_overrides(&mut snapshot.schemas, &config);
        json2sql::schema::registry::exclude_absorbed_children(&mut snapshot.schemas);
        println!("✓ {} tables après overrides", snapshot.schemas.len());
    }

    // ─── Sauvegarder le snapshot si demandé ───────────────────────────────────
    if let Some(ref out_path) = cli.snapshot_output {
        persistence::save(
            &snapshot.schemas,
            snapshot.total_rows,
            &snapshot.truncated_names,
            &snapshot.column_collisions,
            &snapshot.stats,
            out_path,
        )?;
        println!("Snapshot sauvegardé dans '{}'.", out_path.display());
    }

    if cli.input.is_none() || cli.db_url.is_none() {
        println!("ℹ  --input et/ou --db-url non fournis — lancement de migration désactivé");
    }

    let state = Arc::new(AppState::new(
        snapshot,
        cli.input,
        cli.db_url,
        cli.pg_schema,
    ));

    let app = Router::new()
        .route("/", get_route(|| async {
            Html(include_str!("../static/index.html"))
        }))
        .nest("/api", api::router())
        .with_state(state);

    println!("IHM disponible sur http://{}", cli.bind);
    let listener = tokio::net::TcpListener::bind(&cli.bind).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
