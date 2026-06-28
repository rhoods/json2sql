#![allow(dead_code)] // wired up in task 8 (import screen integration)

use std::path::Path;
use std::time::Duration;

use json2sql::ipc::WorkerConfig;

/// Handle to a running worker subprocess.
pub struct WorkerHandle {
    pub child: tokio::process::Child,
    pub stream: tokio::net::UnixStream,
}

/// Spawn `json2sql-worker` with `cfg` sent via stdin, then connect to its socket.
///
/// The PostgreSQL password must be passed via the `J2S_PG_PASSWORD` environment
/// variable — it is NOT serialised into `WorkerConfig`.
#[cfg(unix)]
pub async fn spawn_worker(
    worker_bin: &Path,
    cfg: &WorkerConfig,
    pg_password: &str,
) -> std::io::Result<WorkerHandle> {
    use tokio::io::AsyncWriteExt as _;
    use tokio::process::Command;

    let cfg_json = serde_json::to_vec(cfg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut child = Command::new(worker_bin)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .env("J2S_PG_PASSWORD", pg_password)
        .spawn()?;

    // Write config JSON to worker stdin, then close it so the worker can read EOF.
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&cfg_json).await?;
        // stdin drops here, sending EOF
    }

    let stream = connect_with_retry(&cfg.socket_path, 20, Duration::from_millis(100)).await?;

    Ok(WorkerHandle { child, stream })
}

/// Try to connect to a Unix socket, retrying up to `max_attempts` times with `delay` between.
///
/// Returns `Err` if the socket is still unreachable after all attempts.
#[cfg(unix)]
pub async fn connect_with_retry(
    socket_path: &Path,
    max_attempts: u32,
    delay: Duration,
) -> std::io::Result<tokio::net::UnixStream> {
    for attempt in 0..max_attempts {
        match tokio::net::UnixStream::connect(socket_path).await {
            Ok(stream) => return Ok(stream),
            Err(_) if attempt + 1 < max_attempts => {
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_with_retry_fails_if_no_socket() {
        let path = std::env::temp_dir()
            .join(format!("json2sql-ui-test-missing-{}.sock", uuid::Uuid::now_v7()));
        let result = connect_with_retry(&path, 2, Duration::from_millis(5)).await;
        assert!(result.is_err(), "must fail when socket never appears");
    }

    #[tokio::test]
    async fn connect_with_retry_succeeds_when_socket_ready() {
        use tokio::net::UnixListener;
        let path = std::env::temp_dir()
            .join(format!("json2sql-ui-test-ready-{}.sock", uuid::Uuid::now_v7()));
        let _listener = UnixListener::bind(&path).expect("bind");
        let result = connect_with_retry(&path, 3, Duration::from_millis(10)).await;
        assert!(result.is_ok(), "must succeed when socket is bound");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn connect_with_retry_waits_for_late_socket() {
        use tokio::net::UnixListener;
        let path = std::env::temp_dir()
            .join(format!("json2sql-ui-test-late-{}.sock", uuid::Uuid::now_v7()));
        let path2 = path.clone();

        // Bind the socket after a short delay in a background task
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _l = UnixListener::bind(&path2).expect("bind");
            tokio::time::sleep(Duration::from_millis(500)).await; // keep alive
        });

        // connect_with_retry should eventually succeed
        let result = connect_with_retry(&path, 20, Duration::from_millis(20)).await;
        assert!(result.is_ok(), "must succeed once socket is ready");
        let _ = std::fs::remove_file(&path);
    }
}
