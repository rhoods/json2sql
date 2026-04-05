use thiserror::Error;

#[derive(Debug, Error)]
pub enum J2sError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error at byte {position}: {source}")]
    Json {
        source: serde_json::Error,
        position: u64,
    },

    #[error("Database error: {0}")]
    Db(#[from] tokio_postgres::Error),

    #[error("Database error: {0}")]
    DbContext(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Anomaly report error: {0}")]
    AnomalyReport(String),
}

impl From<serde_json::Error> for J2sError {
    fn from(e: serde_json::Error) -> Self {
        J2sError::Json {
            position: e.column() as u64,
            source: e,
        }
    }
}

pub type Result<T> = std::result::Result<T, J2sError>;
