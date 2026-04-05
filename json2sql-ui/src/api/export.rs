use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use json2sql::db::ddl::{generate_create_table, quote_ident};
use json2sql::schema::registry::build_union_columns;
use json2sql::schema::table_schema::{ColumnSchema, TableSchema};
use json2sql::schema::type_tracker::PgType;

use crate::alerts::{compute_alerts, AlertSeverity};
use crate::state::AppState;

/// GET /api/export/ddl-group/:id — DDL du schéma fusionné d'un groupe.
pub async fn get_group_ddl(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<String, StatusCode> {
    let groups = state.groups.read().unwrap();
    let group = groups.get(&id).ok_or(StatusCode::NOT_FOUND)?;

    let member_schemas: Vec<&TableSchema> = group
        .table_members
        .iter()
        .filter_map(|name| state.table_by_name(name))
        .collect();

    if member_schemas.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    let strategy = group.strategy.as_deref().unwrap_or("Columns");
    let pg_schema = &state.pg_schema;

    match strategy {
        "KeyedPivot" | "StructuredPivot" => {
            let first = member_schemas[0];
            let mut virtual_schema =
                TableSchema::new(group.name.clone(), vec![group.name.clone()], first.depth);
            virtual_schema.parent_table = first.parent_table.clone();

            // j2s_id PK
            virtual_schema
                .columns
                .push(ColumnSchema::generated("j2s_id", PgType::Uuid));

            // FK vers le parent si nécessaire
            if let Some(ref parent) = first.parent_table {
                virtual_schema.columns.push(ColumnSchema::parent_fk(parent));
            }

            // Colonne clé (le suffixe d'origine — ex: "1", "2", "fr"…)
            virtual_schema.columns.push(ColumnSchema {
                name: "key_id".to_string(),
                original_name: "key_id".to_string(),
                pg_type: PgType::Text,
                not_null: true,
                is_generated: false,
                is_parent_fk: false,
            });

            // Union des colonnes de données de tous les membres
            for col in build_union_columns(&member_schemas) {
                virtual_schema.columns.push(col);
            }

            // DDL CREATE TABLE + contrainte FK
            let mut out = generate_create_table(&virtual_schema, pg_schema, true);
            if let Some(ref parent) = virtual_schema.parent_table {
                let fk_col = virtual_schema
                    .columns
                    .iter()
                    .find(|c| c.is_parent_fk)
                    .map(|c| c.name.as_str())
                    .unwrap_or("j2s_parent_id");
                out.push_str(&format!(
                    "\n\nALTER TABLE {schema}.{table}\n    ADD CONSTRAINT {constraint}\n    FOREIGN KEY ({fk}) REFERENCES {schema}.{parent} (j2s_id);",
                    schema = quote_ident(pg_schema),
                    table  = quote_ident(&group.name),
                    constraint = quote_ident(&format!("fk_{}_parent", group.name)),
                    fk     = quote_ident(fk_col),
                    parent = quote_ident(parent),
                ));
            }
            out.push_str(&format!(
                "\n\n-- {} tables fusionnées en 1 via {}\n",
                member_schemas.len(),
                strategy
            ));
            Ok(out)
        }
        _ => {
            // Stratégies non-fusionnantes : DDL de chaque table membre
            let ddls: Vec<String> = member_schemas
                .iter()
                .map(|t| generate_create_table(t, pg_schema, true))
                .collect();
            Ok(format!(
                "-- Groupe « {} » — stratégie {} (tables conservées séparément)\n\n{}",
                group.name,
                strategy,
                ddls.join("\n\n")
            ))
        }
    }
}

/// GET /api/export/ddl/:name — DDL SQL d'une table spécifique.
pub async fn get_ddl(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<String, StatusCode> {
    state
        .table_by_name(&name)
        .map(|t| generate_create_table(t, "public", true))
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Serialize)]
struct ExportError {
    error: String,
    blocking_count: usize,
    blocking: Vec<String>,
}

/// GET /api/export/toml — génère le TOML ou 422 si alertes bloquantes.
pub async fn get_toml(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let all_alerts = compute_alerts(&state);
    let blocking: Vec<_> = all_alerts
        .iter()
        .filter(|a| a.severity == AlertSeverity::Blocking)
        .collect();

    if !blocking.is_empty() {
        let body = ExportError {
            error: "Export bloqué — alertes critiques non résolues".to_string(),
            blocking_count: blocking.len(),
            blocking: blocking.iter().map(|a| format!("{}: {}", a.table, a.message)).collect(),
        };
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(body).into_response());
    }

    let overrides = state.strategy_overrides.read().unwrap();
    let groups = state.groups.read().unwrap();
    let mut toml = String::from("# Généré par json2sql-ui\n\n");

    // Tables absorbées par un groupe de fusion → ne pas émettre d'override individuel
    let merge_strats = ["KeyedPivot", "StructuredPivot"];
    let in_merge_group: std::collections::HashSet<&str> = groups
        .values()
        .filter(|g| g.strategy.as_deref().map_or(false, |s| merge_strats.contains(&s)))
        .flat_map(|g| g.table_members.iter().map(|s| s.as_str()))
        .collect();

    // Sections [group.xxx] pour les groupes de fusion
    let mut has_groups = false;
    for g in groups.values() {
        let strategy_key = match g.strategy.as_deref() {
            Some("KeyedPivot") => "keyed_pivot",
            Some("StructuredPivot") => "structured_pivot",
            _ => continue,
        };
        has_groups = true;
        let members = g
            .table_members
            .iter()
            .map(|m| format!("  {:?}", m))
            .collect::<Vec<_>>()
            .join(",\n");
        toml.push_str(&format!(
            "[group.{}]\nstrategy = \"{}\"\nmembers = [\n{},\n]\n\n",
            g.name, strategy_key, members
        ));
    }

    // Overrides individuels (hors groupes de fusion)
    let individual: Vec<_> = overrides
        .iter()
        .filter(|(table, _)| !in_merge_group.contains(table.as_str()))
        .collect();

    if !has_groups && individual.is_empty() {
        toml.push_str("# Aucun override défini\n");
    } else {
        for (table, strategy) in &individual {
            toml.push_str(&format!(
                "[{}]\nstrategy = \"{}\"\n\n",
                table,
                strategy.to_lowercase()
            ));
        }
    }

    (StatusCode::OK, toml.into_response())
}
