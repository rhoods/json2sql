use axum::Json;
use serde::Serialize;

#[derive(Serialize, Clone)]
pub struct PassParams {
    pub wide_column_threshold: u32,
    pub sibling_threshold: usize,
    pub sibling_jaccard: f64,
    pub stable_threshold: f64,
    pub rare_threshold: f64,
}

#[derive(Serialize)]
pub struct Preset {
    pub id: String,
    pub name: String,
    pub description: String,
    pub params: PassParams,
}

fn presets() -> Vec<Preset> {
    vec![
        Preset {
            id: "conservative".to_string(),
            name: "Conservateur".to_string(),
            description: "Préserve la structure JSON au maximum, peu de fusions automatiques.".to_string(),
            params: PassParams {
                wide_column_threshold: 500,
                sibling_threshold: 5,
                sibling_jaccard: 0.8,
                stable_threshold: 0.05,
                rare_threshold: 0.0001,
            },
        },
        Preset {
            id: "default".to_string(),
            name: "Défaut".to_string(),
            description: "Paramètres par défaut de json2sql.".to_string(),
            params: PassParams {
                wide_column_threshold: 1000,
                sibling_threshold: 3,
                sibling_jaccard: 0.5,
                stable_threshold: 0.10,
                rare_threshold: 0.001,
            },
        },
        Preset {
            id: "aggressive".to_string(),
            name: "Agressif".to_string(),
            description: "Normalisation maximale, réduction du nombre de tables.".to_string(),
            params: PassParams {
                wide_column_threshold: 20,
                sibling_threshold: 2,
                sibling_jaccard: 0.3,
                stable_threshold: 0.20,
                rare_threshold: 0.005,
            },
        },
        Preset {
            id: "openfoodfacts".to_string(),
            name: "OpenFoodFacts".to_string(),
            description: "Paramètres optimisés pour le dataset OpenFoodFacts (4.4M produits, 70GB).".to_string(),
            params: PassParams {
                wide_column_threshold: 50,
                sibling_threshold: 3,
                sibling_jaccard: 0.5,
                stable_threshold: 0.10,
                rare_threshold: 0.001,
            },
        },
    ]
}

/// GET /api/presets — liste des presets de paramètres Pass 1.
pub async fn list_presets() -> Json<Vec<Preset>> {
    Json(presets())
}

/// GET /api/presets/:id — un preset spécifique.
pub async fn get_preset(
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<Preset>, axum::http::StatusCode> {
    presets()
        .into_iter()
        .find(|p| p.id == id)
        .map(Json)
        .ok_or(axum::http::StatusCode::NOT_FOUND)
}
