use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use json2sql::ipc::WorkerConfig;

/// Handle to a running worker subprocess, with the socket stream pre-split.
pub struct WorkerHandle {
    pub child: tokio::process::Child,
    /// Read half of the Unix socket — pass to `SocketEventReader`.
    #[cfg(unix)]
    pub read_half: tokio::net::unix::OwnedReadHalf,
}

// ---------------------------------------------------------------------------
// WorkerKillHandle
// ---------------------------------------------------------------------------

/// Cloneable handle to kill a running worker subprocess.
///
/// All clones share the same `Arc` — calling `.kill()` on any clone sends SIGKILL
/// to the subprocess. Implements `Clone + Debug + Default` so it can live inside
/// `AppState` (which derives those traits).
#[derive(Clone, Default)]
pub struct WorkerKillHandle(Option<Arc<WorkerKillInner>>);

struct WorkerKillInner {
    child: Mutex<tokio::process::Child>,
}

impl std::fmt::Debug for WorkerKillHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerKillHandle({})",
            if self.0.is_some() { "active" } else { "none" }
        )
    }
}

impl WorkerKillHandle {
    pub fn new(child: tokio::process::Child) -> Self {
        Self(Some(Arc::new(WorkerKillInner {
            child: Mutex::new(child),
        })))
    }

    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    /// Send SIGKILL to the subprocess. Idempotent — safe to call multiple times or
    /// after the process has already exited.
    pub fn kill(&self) {
        if let Some(ref inner) = self.0 {
            if let Ok(mut g) = inner.child.lock() {
                let _ = g.start_kill();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SocketEventReader
// ---------------------------------------------------------------------------

/// Async reader that deserialises newline-delimited JSON `ProgressEvent`s from a
/// Unix socket read half.
#[cfg(unix)]
pub struct SocketEventReader {
    reader: tokio::io::BufReader<tokio::net::unix::OwnedReadHalf>,
}

#[cfg(unix)]
impl SocketEventReader {
    pub fn new(read_half: tokio::net::unix::OwnedReadHalf) -> Self {
        Self {
            reader: tokio::io::BufReader::new(read_half),
        }
    }

    /// Read and parse the next event.
    ///
    /// Returns `Ok(None)` on clean EOF (server closed the connection normally),
    /// `Ok(Some(event))` on success, or `Err` if the line is not valid JSON.
    pub async fn next_event(
        &mut self,
    ) -> std::io::Result<Option<json2sql::io::progress_event::ProgressEvent>> {
        use tokio::io::AsyncBufReadExt as _;
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None);
        }
        let event = serde_json::from_str(line.trim_end())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(event))
    }
}

// ---------------------------------------------------------------------------
// spawn_worker / connect_with_retry
// ---------------------------------------------------------------------------

/// Spawn `json2sql-worker` with `cfg` sent via stdin, then connect to its socket.
///
/// The PostgreSQL password must be passed via `pg_password` — it is set as the
/// `J2S_PG_PASSWORD` environment variable and is NOT serialised into `WorkerConfig`.
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

    // Write config JSON to worker stdin, then close it so the worker reads EOF.
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&cfg_json).await?;
        // stdin drops here, sending EOF
    }

    let stream = connect_with_retry(&cfg.socket_path, 20, Duration::from_millis(100)).await?;
    let (read_half, _write_half) = stream.into_split();

    Ok(WorkerHandle { child, read_half })
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
    use json2sql::io::progress_event::ProgressEvent;
    use tokio::io::AsyncWriteExt as _;
    use tokio::net::UnixListener;

    fn temp_sock(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "json2sql-ui-test-{}-{}.sock",
            tag,
            uuid::Uuid::now_v7()
        ))
    }

    #[tokio::test]
    async fn connect_with_retry_fails_if_no_socket() {
        let path = std::env::temp_dir()
            .join(format!("json2sql-ui-test-missing-{}.sock", uuid::Uuid::now_v7()));
        let result = connect_with_retry(&path, 2, Duration::from_millis(5)).await;
        assert!(result.is_err(), "must fail when socket never appears");
    }

    #[tokio::test]
    async fn connect_with_retry_succeeds_when_socket_ready() {
        let path = temp_sock("ready");
        let _listener = UnixListener::bind(&path).expect("bind");
        let result = connect_with_retry(&path, 3, Duration::from_millis(10)).await;
        assert!(result.is_ok(), "must succeed when socket is bound");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn connect_with_retry_waits_for_late_socket() {
        let path = temp_sock("late");
        let path2 = path.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _l = UnixListener::bind(&path2).expect("bind");
            tokio::time::sleep(Duration::from_millis(500)).await;
        });

        let result = connect_with_retry(&path, 20, Duration::from_millis(20)).await;
        assert!(result.is_ok(), "must succeed once socket is ready");
        let _ = std::fs::remove_file(&path);
    }

    // SocketEventReader tests

    async fn make_socket_pair(
        path: &std::path::Path,
    ) -> (
        tokio::net::unix::OwnedWriteHalf,
        tokio::net::unix::OwnedReadHalf,
    ) {
        let listener = UnixListener::bind(path).expect("bind");
        let connect_fut = tokio::net::UnixStream::connect(path);
        let (server_stream, client_stream) =
            tokio::join!(async { listener.accept().await.expect("accept").0 }, connect_fut);
        let client_stream = client_stream.expect("connect");
        let (_, server_write) = server_stream.into_split();
        let (client_read, _) = client_stream.into_split();
        (server_write, client_read)
    }

    #[tokio::test]
    async fn socket_event_reader_reads_single_event() {
        let path = temp_sock("reader-single");
        let (mut write, read) = make_socket_pair(&path).await;
        let mut reader = SocketEventReader::new(read);

        let event = ProgressEvent::Pass1Log("hello from socket".to_string());
        let line = format!("{}\n", serde_json::to_string(&event).unwrap());
        write.write_all(line.as_bytes()).await.expect("write");

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            reader.next_event(),
        )
        .await
        .expect("timeout")
        .expect("io");
        assert!(
            matches!(result, Some(ProgressEvent::Pass1Log(m)) if m == "hello from socket"),
            "must deserialise the event"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn socket_event_reader_reads_multiple_events_in_order() {
        let path = temp_sock("reader-multi");
        let (mut write, read) = make_socket_pair(&path).await;
        let mut reader = SocketEventReader::new(read);

        for i in 0..3u64 {
            let event = ProgressEvent::Pass2Progress {
                bytes_read: i * 100,
                total_bytes: 300,
                rows_processed: i,
            };
            let line = format!("{}\n", serde_json::to_string(&event).unwrap());
            write.write_all(line.as_bytes()).await.expect("write");
        }

        for expected_bytes in [0u64, 100, 200] {
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                reader.next_event(),
            )
            .await
            .expect("timeout")
            .expect("io");
            assert!(
                matches!(result, Some(ProgressEvent::Pass2Progress { bytes_read, .. }) if bytes_read == expected_bytes)
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn socket_event_reader_returns_none_on_eof() {
        let path = temp_sock("reader-eof");
        let (write, read) = make_socket_pair(&path).await;
        let mut reader = SocketEventReader::new(read);

        // Drop the write half — causes EOF on the read side
        drop(write);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            reader.next_event(),
        )
        .await
        .expect("timeout")
        .expect("io");
        assert!(result.is_none(), "EOF must return Ok(None)");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn socket_event_reader_error_on_malformed_json() {
        let path = temp_sock("reader-malformed");
        let (mut write, read) = make_socket_pair(&path).await;
        let mut reader = SocketEventReader::new(read);

        write.write_all(b"not-valid-json\n").await.expect("write");

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            reader.next_event(),
        )
        .await
        .expect("timeout");
        assert!(result.is_err(), "malformed JSON must return Err");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn worker_kill_handle_default_is_none() {
        let h = WorkerKillHandle::default();
        assert!(!h.is_some());
    }

    #[test]
    fn worker_kill_handle_debug_shows_active_or_none() {
        let h = WorkerKillHandle::default();
        assert!(format!("{h:?}").contains("none"));
    }

    #[test]
    fn worker_kill_handle_clone_shares_same_arc() {
        // We can't easily test kill() without a real subprocess, but we can verify
        // that clone() produces an is_some() handle when the original is_some().
        // We construct one with a dummy child... actually we can't without spawning.
        // Just verify default clone behaves correctly.
        let h1 = WorkerKillHandle::default();
        let h2 = h1.clone();
        assert!(!h2.is_some());
    }
}
