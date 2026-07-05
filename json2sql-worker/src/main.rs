//! Point d'entrée du process worker : lit la config sur stdin, verrouille le lockfile exclusif,
//! sert le socket Unix (`serve`) en parallèle du pipeline d'import (`pipeline`), écrit le résultat.
//!
//! Fonctions :
//! - struct `LockfileGuard` — verrou exclusif RAII sur le lockfile (libéré au `Drop`).
//! - fn `LockfileGuard::try_acquire_at` — tente le verrou exclusif sur un chemin donné.
//! - fn `LockfileGuard::try_acquire` — tente le verrou exclusif sur le lockfile par défaut du worker.
//! - fn `bind_socket` — lie un `UnixListener` (supprime un socket obsolète existant) ; variante non-Unix retourne une erreur `Unsupported`.
//! - fn `write_result` — écrit le `WorkerResult` de façon atomique (fichier temporaire + rename).
//! - fn `main` — parse la config, acquiert le lock, lance `serve::serve_connections` et `pipeline::run_pipeline` en parallèle, écrit le résultat, annule proprement à la fin.

use std::sync::Arc;

mod cancel;
mod pipeline;
mod serve;
mod summary;

use json2sql::ipc::{WorkerConfig, WorkerResult, lockfile_path};

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

    #[cfg(unix)]
    {
        let listener = match bind_socket(&cfg.socket_path) {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "json2sql-worker: failed to bind socket {}: {e}",
                    cfg.socket_path.display()
                );
                std::process::exit(2);
            }
        };
        eprintln!("json2sql-worker: socket ready, accepting connections");

        let summary = Arc::new(tokio::sync::Mutex::new(summary::ImportSummary::new()));
        let cancel = cancel::CancelToken::new();

        let serve_handle = tokio::spawn(serve::serve_connections(
            listener,
            Arc::clone(&summary),
            cancel.clone(),
        ));

        let result = pipeline::run_pipeline(&cfg, Arc::clone(&summary), cancel.clone()).await;

        if let Err(e) = write_result(&cfg.result_file, &result) {
            eprintln!("json2sql-worker: failed to write result file: {e}");
        }

        cancel.cancel(); // stop serve loop (and any remaining connection handlers)
        serve_handle.await.ok();
    }
    #[cfg(not(unix))]
    {
        eprintln!("json2sql-worker: requires Unix (Linux/macOS)");
        std::process::exit(2);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use json2sql::ipc::new_socket_path;

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
        let r1 = WorkerResult { status: "error".to_string(), total_rows: 0, anomaly_count: 0, constraint_warning_count: 0, message: None };
        write_result(&dest, &r1).expect("first write");
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
