use std::collections::{HashMap, HashSet};

use serde::Serialize;

use json2sql::schema::table_schema::{TableSchema, WideStrategy};

use crate::state::AppState;

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    /// Empêche l'export TOML
    Blocking,
    /// Avertissement — export possible avec confirmation
    Warning,
}

/// Action suggérée pour une alerte — affichée comme bouton dans l'UI.
#[derive(Debug, Clone, Serialize)]
pub struct AlertAction {
    /// Libellé du bouton
    pub label: String,
    /// Stratégie à appliquer via POST /api/schema/tables/:name/strategy
    pub strategy: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub kind: String,
    pub table: String,
    pub message: String,
    /// Action suggérée (optionnelle) — permet à l'UI d'afficher un bouton d'action directe.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<AlertAction>,
}

/// Calculer toutes les alertes depuis l'état courant.
pub fn compute_alerts(state: &AppState) -> Vec<Alert> {
    let overrides = state.strategy_overrides.read().unwrap();
    let mut alerts = Vec::new();

    for table in state.tables() {
        // Table large sans décision explicite de l'utilisateur
        if table.wide_strategy.is_wide()
            && !matches!(table.wide_strategy, WideStrategy::AutoSplit { .. })
            && !overrides.contains_key(&table.name)
        {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                kind: "wide_not_confirmed".to_string(),
                table: table.name.clone(),
                message: format!(
                    "Stratégie '{}' auto-détectée non confirmée",
                    strategy_label(&table.wide_strategy)
                ),
                action: None,
            });
        }

        // Table AutoSplit sans override : informationnel seulement si racine
        // (déjà géré par la Pass 1, pas d'alerte)

        // Table avec colonnes mais stratégie Columns et > 100 colonnes sans override
        if wide_column_count(table) > 100 && !overrides.contains_key(&table.name) {
            alerts.push(Alert {
                severity: AlertSeverity::Blocking,
                kind: "very_wide_unconfigured".to_string(),
                table: table.name.clone(),
                message: format!(
                    "{} colonnes — stratégie non configurée",
                    wide_column_count(table)
                ),
                action: None,
            });
        }
    }

    // ── Validations des groupes de fusion ────────────────────────────────────
    let groups = state.groups.read().unwrap();
    const MERGE_STRATS: &[&str] = &["KeyedPivot", "StructuredPivot"];

    // Table présente dans plusieurs groupes de fusion → bloquant
    let mut table_merge_count: HashMap<&str, usize> = HashMap::new();
    for g in groups.values() {
        if g.strategy.as_deref().map_or(false, |s| MERGE_STRATS.contains(&s)) {
            for t in &g.table_members {
                *table_merge_count.entry(t.as_str()).or_insert(0) += 1;
            }
        }
    }
    for (table_name, count) in &table_merge_count {
        if *count > 1 {
            alerts.push(Alert {
                severity: AlertSeverity::Blocking,
                kind: "table_multi_group".to_string(),
                table: table_name.to_string(),
                message: format!("Présente dans {} groupes de fusion simultanément", count),
                action: None,
            });
        }
    }

    for g in groups.values() {
        let strategy = g.strategy.as_deref().unwrap_or("Columns");
        if !MERGE_STRATS.contains(&strategy) {
            continue;
        }

        // Tables introuvables dans le snapshot
        let missing: Vec<_> = g
            .table_members
            .iter()
            .filter(|name| state.table_by_name(name).is_none())
            .collect();
        if !missing.is_empty() {
            alerts.push(Alert {
                severity: AlertSeverity::Blocking,
                kind: "group_missing_tables".to_string(),
                table: g.name.clone(),
                message: format!(
                    "{} membre(s) introuvable(s) : {}",
                    missing.len(),
                    missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ")
                ),
                action: None,
            });
            continue; // pas de sens de continuer les vérifications
        }

        let members: Vec<&TableSchema> = g
            .table_members
            .iter()
            .filter_map(|name| state.table_by_name(name))
            .collect();

        // Groupe avec un seul membre → inutile
        if members.len() < 2 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                kind: "group_single_member".to_string(),
                table: g.name.clone(),
                message: "Groupe de fusion avec un seul membre — fusion inutile".to_string(),
                action: None,
            });
            continue;
        }

        // Parents différents → bloquant (FK vers des tables différentes)
        let parents: HashSet<Option<&str>> =
            members.iter().map(|t| t.parent_table.as_deref()).collect();
        if parents.len() > 1 {
            let parent_names: Vec<&str> = parents
                .iter()
                .map(|p| p.unwrap_or("<racine>"))
                .collect();
            alerts.push(Alert {
                severity: AlertSeverity::Blocking,
                kind: "group_mixed_parents".to_string(),
                table: g.name.clone(),
                message: format!(
                    "Parents différents ({}) — les FK pointent vers des tables distinctes",
                    parent_names.join(", ")
                ),
                action: None,
            });
        }

        // Jaccard minimal < 30 % → avertissement (table résultante très creuse)
        let jaccard = min_pairwise_jaccard(&members);
        if jaccard < 0.30 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                kind: "group_low_jaccard".to_string(),
                table: g.name.clone(),
                message: format!(
                    "Schémas peu similaires (Jaccard min {:.0}%) — table fusionnée très creuse",
                    jaccard * 100.0
                ),
                action: None,
            });
        }
    }

    // ── Détection d'explosion de tables enfants ───────────────────────────────
    // Pré-calculer l'ensemble des tables absorbées (ancêtre avec override Jsonb).
    // BFS depuis chaque table Jsonb pour couvrir tous les descendants quelle que
    // soit l'ordre de la liste.
    let jsonb_absorbers: HashSet<&str> = overrides
        .iter()
        .filter(|(_, v)| v.as_str() == "Jsonb")
        .map(|(k, _)| k.as_str())
        .collect();
    let mut absorbed_set: HashSet<&str> = HashSet::new();
    if !jsonb_absorbers.is_empty() {
        let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
        for t in state.tables() {
            if let Some(ref parent) = t.parent_table {
                children.entry(parent.as_str()).or_default().push(t.name.as_str());
            }
        }
        let mut queue: std::collections::VecDeque<&str> = std::collections::VecDeque::new();
        for &name in &jsonb_absorbers {
            if let Some(kids) = children.get(name) {
                queue.extend(kids.iter().copied());
            }
        }
        while let Some(name) = queue.pop_front() {
            if absorbed_set.insert(name) {
                if let Some(kids) = children.get(name) {
                    queue.extend(kids.iter().copied());
                }
            }
        }
    }

    const CHILD_EXPLOSION_THRESHOLD: usize = 50;
    let mut child_count: HashMap<&str, usize> = HashMap::new();
    for t in state.tables() {
        if let Some(ref parent) = t.parent_table {
            *child_count.entry(parent.as_str()).or_insert(0) += 1;
        }
    }
    for (parent_name, count) in &child_count {
        if *count < CHILD_EXPLOSION_THRESHOLD
            || overrides.contains_key(*parent_name)
            || absorbed_set.contains(*parent_name)
        {
            continue;
        }
        // Ne pas alerter sur les tables racines (depth 0, pas de parent_table) :
        // elles ont naturellement de nombreux enfants et les convertir en JSONB
        // détruirait toute la structure relationnelle.
        let is_root = state
            .table_by_name(parent_name)
            .map_or(true, |t| t.parent_table.is_none());
        if is_root {
            continue;
        }
        alerts.push(Alert {
            severity: AlertSeverity::Warning,
            kind: "child_explosion".to_string(),
            table: parent_name.to_string(),
            message: format!(
                "{} tables enfants détectées — conversion en JSONB recommandée",
                count
            ),
            action: Some(AlertAction {
                label: "Convertir en JSONB".to_string(),
                strategy: "Jsonb".to_string(),
            }),
        });
    }

    // Dédoublonner : si une table a déjà un blocking, supprimer le warning associé
    dedup_alerts(alerts)
}

fn wide_column_count(t: &TableSchema) -> usize {
    matches!(t.wide_strategy, WideStrategy::Columns)
        .then(|| t.data_columns().count())
        .unwrap_or(0)
}

fn strategy_label(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns => "Columns",
        WideStrategy::Pivot => "Pivot",
        WideStrategy::Jsonb => "Jsonb",
        WideStrategy::StructuredPivot(_) => "StructuredPivot",
        WideStrategy::KeyedPivot(_) => "KeyedPivot",
        WideStrategy::AutoSplit { .. } => "AutoSplit",
        WideStrategy::Ignore => "Ignore",
    }
}

/// Jaccard minimal sur toutes les paires de tables (noms de colonnes de données).
fn min_pairwise_jaccard(schemas: &[&TableSchema]) -> f64 {
    if schemas.len() < 2 {
        return 1.0;
    }
    let mut min = 1.0_f64;
    for i in 0..schemas.len() {
        let a: HashSet<&str> = schemas[i].data_columns().map(|c| c.original_name.as_str()).collect();
        for j in (i + 1)..schemas.len() {
            let b: HashSet<&str> = schemas[j].data_columns().map(|c| c.original_name.as_str()).collect();
            let inter = a.iter().filter(|&&c| b.contains(c)).count();
            let union = a.len() + b.len() - inter;
            let j_val = if union == 0 { 1.0 } else { inter as f64 / union as f64 };
            if j_val < min {
                min = j_val;
            }
        }
    }
    min
}

fn dedup_alerts(mut alerts: Vec<Alert>) -> Vec<Alert> {
    // Si une table a un Blocking, supprimer ses Warnings
    let blocking_tables: std::collections::HashSet<_> = alerts
        .iter()
        .filter(|a| a.severity == AlertSeverity::Blocking)
        .map(|a| a.table.clone())
        .collect();

    alerts.retain(|a| {
        !(a.severity == AlertSeverity::Warning && blocking_tables.contains(&a.table))
    });
    alerts
}
