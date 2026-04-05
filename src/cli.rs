use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "json2sql",
    about = "Convert large nested JSON files to PostgreSQL databases",
    version
)]
pub struct Cli {
    /// Input JSON file (JSON array or JSON-Lines format); reads from stdin if omitted
    #[arg(short, long, value_name = "FILE")]
    pub input: Option<PathBuf>,

    /// PostgreSQL connection string (e.g. postgres://user:pass@localhost/dbname)
    #[arg(short, long, value_name = "DSN", env = "DATABASE_URL")]
    pub db_url: Option<String>,

    /// Target PostgreSQL schema (default: public)
    #[arg(long, default_value = "public")]
    pub schema: String,

    /// Root table name (defaults to input filename without extension)
    #[arg(long, value_name = "NAME")]
    pub table: Option<String>,

    /// Drop existing tables before import
    #[arg(long, default_value_t = false)]
    pub drop_existing: bool,

    /// Output file for anomaly report (stdout if omitted)
    #[arg(long, value_name = "FILE")]
    pub anomaly_output: Option<PathBuf>,

    /// Anomaly report format
    #[arg(long, default_value = "json")]
    pub anomaly_format: AnomalyFormat,

    /// Abort if anomaly rate exceeds this threshold (0.0–1.0, disabled by default)
    #[arg(long, value_name = "RATE")]
    pub max_anomaly_rate: Option<f64>,

    /// Minimum string length to use TEXT instead of VARCHAR (default: 256)
    #[arg(long, default_value_t = 256)]
    pub text_threshold: u32,

    /// Flush a table's buffer to PostgreSQL every N rows during Pass 2, keeping
    /// temp-file disk usage bounded. Use 0 to disable (buffer everything first).
    /// (default: 100000)
    #[arg(long, default_value_t = 100_000)]
    pub batch_size: usize,

    /// Wrap all COPY operations in a single transaction; rollback on error
    #[arg(long, default_value_t = false)]
    pub transaction: bool,

    /// Print inferred DDL to stdout without connecting to any database
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Store scalar arrays as PostgreSQL array columns (TEXT[], INTEGER[]…) instead of junction tables
    #[arg(long, default_value_t = false)]
    pub array_as_pg_array: bool,

    /// Warn when nesting depth exceeds N levels (disabled by default)
    #[arg(long, value_name = "N")]
    pub depth_limit: Option<usize>,

    /// Print Pass 1 schema statistics (type distribution per column) to stderr
    #[arg(long, default_value_t = false)]
    pub schema_report: bool,

    /// Write Pass 1 schema statistics to FILE instead of stderr (implies --schema-report)
    #[arg(long, value_name = "FILE")]
    pub schema_report_output: Option<PathBuf>,

    /// TOML file with manual type overrides applied after schema inference
    #[arg(long, value_name = "FILE")]
    pub schema_config: Option<PathBuf>,

    /// Save the Pass 1 schema snapshot to FILE (JSON) for later reuse with --schema-input
    #[arg(long, value_name = "FILE")]
    pub schema_output: Option<PathBuf>,

    /// Load a previously saved Pass 1 schema snapshot from FILE and skip Pass 1 entirely
    #[arg(long, value_name = "FILE")]
    pub schema_input: Option<PathBuf>,

    /// Number of parallel PostgreSQL connections for COPY (default: 1, sequential)
    #[arg(long, default_value_t = 1, value_name = "N")]
    pub parallel: usize,

    /// Tables with more data columns than this threshold are automatically assigned a WideStrategy
    /// (Pivot or Jsonb). Override per table via --schema-config with `strategy = "pivot"|"jsonb"`.
    /// Set to 0 to disable automatic wide-table detection.
    #[arg(long, default_value_t = 100, value_name = "N")]
    pub wide_column_threshold: usize,

    /// Minimum number of sibling child tables required to trigger automatic KeyedPivot merging.
    /// Sibling tables share the same parent and have similar column schemas (see --sibling-jaccard).
    /// Set to 0 to disable automatic sibling detection.
    #[arg(long, default_value_t = 3, value_name = "N")]
    pub sibling_threshold: usize,

    /// Minimum Jaccard similarity (0.0–1.0) between sibling table column sets required for merging.
    /// Lower values allow merging tables with more schema divergence.
    #[arg(long, default_value_t = 0.5, value_name = "F")]
    pub sibling_jaccard: f64,

    /// Fraction of rows a key must appear in to be kept as a stable column in the main table
    /// (AutoSplit strategy). Keys below this threshold but above --rare-threshold go to
    /// the companion `{table}_wide` EAV table.
    #[arg(long, default_value_t = 0.10, value_name = "F")]
    pub stable_threshold: f64,

    /// Fraction of rows below which a key is dropped entirely (AutoSplit + Ignore strategy).
    /// Keys appearing in fewer rows than this fraction are excluded from all schemas and data.
    #[arg(long, default_value_t = 0.001, value_name = "F")]
    pub rare_threshold: f64,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum AnomalyFormat {
    Json,
    Csv,
}

impl Cli {
    /// Derive root table name from input file or explicit --table flag
    pub fn root_table_name(&self) -> String {
        if let Some(ref name) = self.table {
            return name.clone();
        }
        self.input
            .as_ref()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("root")
            .to_string()
    }
}
