use std::path::PathBuf;

use json2sql::io::reader::JsonFormat;

mod summary;
use json2sql::pass2::Pass2Config;
use json2sql::schema::table_schema::TableSchema;

// ---------------------------------------------------------------------------
// IPC types — exchanged between UI and worker over stdin / socket
// ---------------------------------------------------------------------------

/// Configuration sent by the UI to the worker via stdin (JSON).
/// PostgreSQL password is NOT included here — passed via J2S_PG_PASSWORD env var.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig {
    pub source_file: PathBuf,
    pub root_table: String,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_database: String,
    pub pg_user: String,
    pub pg_schema: String,
    pub schemas: Vec<TableSchema>,
    pub drop_existing: bool,
    pub anomaly_dir: Option<PathBuf>,
    pub pass2_parallel: usize,
    pub import_limit: Option<u64>,
    pub verbose_logs: bool,
    pub hint_format: Option<JsonFormat>,
    pub skip_constraints: bool,
    /// Full path to the Unix socket the worker will bind.
    pub socket_path: PathBuf,
    /// Full path where the worker writes the JSON result file.
    pub result_file: PathBuf,
}

impl WorkerConfig {
    /// Build a `Pass2Config` from this config (password resolved separately).
    #[must_use]
    pub fn into_pass2_config(self) -> Pass2Config {
        Pass2Config {
            root_table: self.root_table,
            pg_schema: self.pg_schema,
            parallel: self.pass2_parallel,
            anomaly_dir: self.anomaly_dir,
            limit: self.import_limit,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: self.verbose_logs,
            hint_format: self.hint_format,
            skip_constraints: self.skip_constraints,
        }
    }
}

/// Command sent by the UI to the worker on the socket (one JSON line).
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkerCommand {
    pub cmd: String,
}

/// Final result written atomically to `result_file` before the socket is closed.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct WorkerResult {
    /// "success", "error", or "cancelled"
    pub status: String,
    pub total_rows: u64,
    pub anomaly_count: u64,
    pub constraint_warning_count: u64,
    pub message: Option<String>,
}

// ---------------------------------------------------------------------------
// Socket path helpers
// ---------------------------------------------------------------------------

/// Generate a fresh socket path under `temp_dir()` using a UUID v7 session ID.
#[must_use]
pub fn new_socket_path() -> PathBuf {
    let id = uuid::Uuid::now_v7();
    std::env::temp_dir().join(format!("json2sql-{id}.sock"))
}

/// Path to the exclusive lockfile preventing concurrent workers.
#[must_use]
pub fn lockfile_path() -> PathBuf {
    std::env::temp_dir().join("json2sql-worker.lock")
}

// ---------------------------------------------------------------------------
// Lockfile guard
// ---------------------------------------------------------------------------

/// RAII guard that holds an exclusive lock on a lockfile.
/// Dropped when the worker exits, releasing the lock automatically.
pub struct LockfileGuard {
    _file: std::fs::File,
}

impl LockfileGuard {
    /// Try to acquire an exclusive lock on `path`. Returns `None` if already held.
    pub fn try_acquire_at(path: &std::path::Path) -> std::io::Result<Option<Self>> {
        use fs2::FileExt as _;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Try to acquire the default worker lockfile.
    pub fn try_acquire() -> std::io::Result<Option<Self>> {
        Self::try_acquire_at(&lockfile_path())
    }
}

// ---------------------------------------------------------------------------
// Socket binding
// ---------------------------------------------------------------------------

/// Bind a `UnixListener` at `path`, removing any stale socket file first.
/// Returns the bound listener ready to accept connections.
#[cfg(unix)]
pub fn bind_socket(path: &std::path::Path) -> std::io::Result<tokio::net::UnixListener> {
    // Remove stale socket file so bind doesn't fail with AddrInUse.
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    tokio::net::UnixListener::bind(path)
}

#[cfg(not(unix))]
pub fn bind_socket(_path: &std::path::Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "json2sql-worker requires Unix sockets (Linux/macOS only)",
    ))
}

// ---------------------------------------------------------------------------
// Result file
// ---------------------------------------------------------------------------

/// Write `result` atomically to `dest` using a sibling temp file + rename.
/// Prevents the UI from reading a partial file if the worker is killed mid-write.
pub fn write_result(dest: &std::path::Path, result: &WorkerResult) -> std::io::Result<()> {
    let tmp = dest.with_extension("tmp");
    let json = serde_json::to_vec(result)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, dest)
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cfg: WorkerConfig = {
        let stdin = std::io::stdin();
        match serde_json::from_reader(stdin) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("json2sql-worker: failed to parse config from stdin: {e}");
                std::process::exit(2);
            }
        }
    };

    let _lock = match LockfileGuard::try_acquire() {
        Ok(Some(g)) => g,
        Ok(None) => {
            eprintln!("json2sql-worker: another worker is already running");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("json2sql-worker: could not acquire lockfile: {e}");
            std::process::exit(2);
        }
    };

    eprintln!("json2sql-worker: started, socket={}", cfg.socket_path.display());
    let _listener = match bind_socket(&cfg.socket_path) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("json2sql-worker: failed to bind socket {}: {e}", cfg.socket_path.display());
            std::process::exit(2);
        }
    };
    eprintln!("json2sql-worker: socket ready, waiting for connections");
    // TODO (tâche 3): accept connections, run pipeline, stream events
    std::process::exit(0);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_config(socket_path: PathBuf, result_file: PathBuf) -> WorkerConfig {
        WorkerConfig {
            source_file: PathBuf::from("/tmp/data.json"),
            root_table: "root".to_string(),
            pg_host: "localhost".to_string(),
            pg_port: 5432,
            pg_database: "mydb".to_string(),
            pg_user: "user".to_string(),
            pg_schema: "public".to_string(),
            schemas: vec![],
            drop_existing: false,
            anomaly_dir: None,
            pass2_parallel: 4,
            import_limit: None,
            verbose_logs: false,
            hint_format: None,
            skip_constraints: false,
            socket_path,
            result_file,
        }
    }

    #[test]
    fn worker_config_serde_round_trip() {
        let socket = new_socket_path();
        let result = std::env::temp_dir().join("result.json");
        let cfg = minimal_config(socket.clone(), result.clone());

        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: WorkerConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(back.root_table, "root");
        assert_eq!(back.pg_host, "localhost");
        assert_eq!(back.pg_port, 5432);
        assert_eq!(back.socket_path, socket);
        assert_eq!(back.result_file, result);
        assert!(!back.skip_constraints);
    }

    #[test]
    fn worker_result_serde_round_trip() {
        let r = WorkerResult {
            status: "success".to_string(),
            total_rows: 12345,
            anomaly_count: 2,
            constraint_warning_count: 0,
            message: None,
        };
        let json = serde_json::to_string(&r).expect("serialize");
        let back: WorkerResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.status, "success");
        assert_eq!(back.total_rows, 12345);
        assert!(back.message.is_none());
    }

    #[test]
    fn worker_command_serde_round_trip() {
        let cmd = WorkerCommand { cmd: "cancel".to_string() };
        let json = serde_json::to_string(&cmd).expect("serialize");
        let back: WorkerCommand = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.cmd, "cancel");
    }

    #[test]
    fn new_socket_path_uses_temp_dir_and_has_uuid() {
        let p = new_socket_path();
        assert!(p.starts_with(std::env::temp_dir()));
        let name = p.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("json2sql-"), "must start with json2sql-");
        assert!(name.ends_with(".sock"), "must end with .sock");
    }

    #[test]
    fn lockfile_path_uses_temp_dir() {
        let p = lockfile_path();
        assert!(p.starts_with(std::env::temp_dir()));
        assert_eq!(p.file_name().unwrap(), "json2sql-worker.lock");
    }

    #[test]
    fn lockfile_guard_acquires_and_releases() {
        let path = std::env::temp_dir().join("json2sql-test-lock-acquire.lock");
        let guard = LockfileGuard::try_acquire_at(&path).expect("no IO error");
        assert!(guard.is_some(), "must acquire lock when not held");
        drop(guard);
        // After release, can acquire again
        let guard2 = LockfileGuard::try_acquire_at(&path).expect("no IO error");
        assert!(guard2.is_some(), "must re-acquire after release");
    }

    #[test]
    fn lockfile_guard_blocks_second_acquisition() {
        let path = std::env::temp_dir().join("json2sql-test-lock-block.lock");
        let guard1 = LockfileGuard::try_acquire_at(&path)
            .expect("no IO error")
            .expect("must acquire first lock");
        let guard2 = LockfileGuard::try_acquire_at(&path).expect("no IO error");
        assert!(guard2.is_none(), "must fail to acquire when already held");
        drop(guard1);
    }

    #[test]
    fn write_result_creates_file_with_correct_content() {
        let dir = std::env::temp_dir();
        let dest = dir.join(format!("json2sql-result-{}.json", uuid::Uuid::now_v7()));
        let result = WorkerResult {
            status: "success".to_string(),
            total_rows: 42,
            anomaly_count: 1,
            constraint_warning_count: 0,
            message: None,
        };
        write_result(&dest, &result).expect("write must succeed");
        assert!(dest.exists(), "result file must exist");
        let content = std::fs::read_to_string(&dest).expect("read back");
        let back: WorkerResult = serde_json::from_str(&content).expect("deserialize");
        assert_eq!(back.status, "success");
        assert_eq!(back.total_rows, 42);
        let _ = std::fs::remove_file(&dest);
    }

    #[test]
    fn write_result_is_atomic_no_tmp_left_on_success() {
        let dir = std::env::temp_dir();
        let dest = dir.join(format!("json2sql-result-atomic-{}.json", uuid::Uuid::now_v7()));
        let tmp = dest.with_extension("tmp");
        let result = WorkerResult {
            status: "error".to_string(),
            total_rows: 0,
            anomaly_count: 0,
            constraint_warning_count: 0,
            message: Some("pg error".to_string()),
        };
        write_result(&dest, &result).expect("write must succeed");
        assert!(dest.exists(), "final result file must exist");
        assert!(!tmp.exists(), "tmp file must be renamed away");
        let content = std::fs::read_to_string(&dest).expect("read back");
        let back: WorkerResult = serde_json::from_str(&content).expect("deserialize");
        assert_eq!(back.message.as_deref(), Some("pg error"));
        let _ = std::fs::remove_file(&dest);
    }

    #[test]
    fn write_result_overwrites_existing_file() {
        let dir = std::env::temp_dir();
        let dest = dir.join(format!("json2sql-result-overwrite-{}.json", uuid::Uuid::now_v7()));
        // Write a first result
        let r1 = WorkerResult { status: "error".to_string(), total_rows: 0, anomaly_count: 0, constraint_warning_count: 0, message: None };
        write_result(&dest, &r1).expect("first write");
        // Overwrite with a second result
        let r2 = WorkerResult { status: "success".to_string(), total_rows: 99, anomaly_count: 0, constraint_warning_count: 0, message: None };
        write_result(&dest, &r2).expect("second write");
        let content = std::fs::read_to_string(&dest).expect("read back");
        let back: WorkerResult = serde_json::from_str(&content).expect("deserialize");
        assert_eq!(back.status, "success");
        assert_eq!(back.total_rows, 99);
        let _ = std::fs::remove_file(&dest);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_socket_creates_socket_file() {
        let path = std::env::temp_dir().join(format!("json2sql-test-{}.sock", uuid::Uuid::now_v7()));
        let _listener = bind_socket(&path).expect("bind must succeed");
        assert!(path.exists(), "socket file must exist after bind");
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_socket_removes_stale_socket() {
        let path = std::env::temp_dir().join(format!("json2sql-test-stale-{}.sock", uuid::Uuid::now_v7()));
        // Create a stale socket file
        std::fs::write(&path, b"stale").expect("write stale file");
        let _listener = bind_socket(&path).expect("bind must succeed even with stale file");
        assert!(path.exists(), "socket file must exist after bind");
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bind_socket_accepts_connection() {
        use tokio::net::UnixStream;
        let path = std::env::temp_dir().join(format!("json2sql-test-conn-{}.sock", uuid::Uuid::now_v7()));
        let _listener = bind_socket(&path).expect("bind must succeed");
        // Client can connect
        UnixStream::connect(&path).await.expect("client connect must succeed");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn into_pass2_config_maps_fields_correctly() {
        let socket = new_socket_path();
        let result = std::env::temp_dir().join("result.json");
        let cfg = WorkerConfig {
            pass2_parallel: 8,
            import_limit: Some(500),
            skip_constraints: true,
            verbose_logs: true,
            pg_schema: "myschema".to_string(),
            root_table: "myroot".to_string(),
            ..minimal_config(socket, result)
        };
        let p2 = cfg.into_pass2_config();
        assert_eq!(p2.parallel, 8);
        assert_eq!(p2.limit, Some(500));
        assert!(p2.skip_constraints);
        assert!(p2.verbose);
        assert_eq!(p2.pg_schema, "myschema");
        assert_eq!(p2.root_table, "myroot");
    }
}
