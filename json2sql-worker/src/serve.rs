//! Serveur du socket Unix : diffuse l'historique + les événements en direct de l'import à
//! chaque connexion client, et traduit la commande `{"cmd":"cancel"}` en annulation.
//!
//! Fonctions :
//! - `serve_connections` — boucle d'acceptation jusqu'à `cancel` ; une tâche par connexion.
//! - `handle_connection` — envoie le snapshot puis les deltas (attend `Notify` avant chaque
//!   relecture) ; lit les commandes clientes en parallèle ; ferme après `Pass2Done`/annulation
//!   (avec un drain final pour ne pas perdre le dernier batch en cas de course cancel/notify).
//! - `write_line` — écrit une ligne NDJSON sur le socket.

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

use json2sql::ipc::WorkerCommand;

use crate::cancel::CancelToken;
use crate::summary::ImportSummary;

/// Accept connections on `listener` until `cancel` is triggered.
///
/// Each connection receives the full `ImportSummary` snapshot (all events so far),
/// then streams delta events as they are pushed. Commands received from clients
/// (e.g. `{"cmd":"cancel"}`) are translated into `cancel.cancel()`.
#[cfg(unix)]
pub async fn serve_connections(
    listener: tokio::net::UnixListener,
    summary: Arc<Mutex<ImportSummary>>,
    cancel: CancelToken,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        let sum2 = Arc::clone(&summary);
                        let cancel2 = cancel.clone();
                        tokio::spawn(handle_connection(stream, sum2, cancel2));
                    }
                    Err(e) => {
                        eprintln!("json2sql-worker: accept error: {e}");
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

/// Handle one client connection: stream snapshot + deltas, read cancel commands.
#[cfg(unix)]
async fn handle_connection(
    stream: tokio::net::UnixStream,
    summary: Arc<Mutex<ImportSummary>>,
    cancel: CancelToken,
) {
    let (read_half, write_half) = stream.into_split();

    // Spawn command reader — translates `{"cmd":"cancel"}` into CancelToken signal.
    {
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(read_half).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(cmd) = serde_json::from_str::<WorkerCommand>(&line) {
                    if cmd.cmd == "cancel" {
                        cancel2.cancel();
                        return;
                    }
                }
            }
        });
    }

    let mut writer = write_half;

    // Lock briefly to clone snapshot and get the shared notifier; release before any IO.
    let (snapshot_lines, notifier, mut cursor) = {
        let s = summary.lock().await;
        let lines: Vec<String> = s
            .snapshot()
            .iter()
            .filter_map(|e| serde_json::to_string(e).ok())
            .collect();
        let cursor = s.len();
        (lines, s.notifier(), cursor)
    };

    for line in &snapshot_lines {
        if write_line(&mut writer, line).await.is_err() {
            return;
        }
    }

    // Delta loop: register the `Notified` future BEFORE locking to avoid missing events
    // that are pushed between the lock release and the wait.
    loop {
        let notified = notifier.notified();

        let (delta_lines, is_done) = {
            let s = summary.lock().await;
            let lines: Vec<String> = s.snapshot()[cursor..]
                .iter()
                .filter_map(|e| serde_json::to_string(e).ok())
                .collect();
            let is_done = s.is_done() || cancel.is_cancelled();
            cursor = s.len();
            (lines, is_done)
        };

        for line in &delta_lines {
            if write_line(&mut writer, line).await.is_err() {
                return;
            }
        }

        if is_done {
            break;
        }

        tokio::select! {
            _ = notified => {}
            _ = cancel.cancelled() => {
                // Final drain: emit any events (including Pass2Done) pushed before the
                // cancel signal arrived — prevents the client from missing the last batch
                // when cancel fires simultaneously with a notification.
                let drain: Vec<String> = {
                    let s = summary.lock().await;
                    s.snapshot()[cursor..]
                        .iter()
                        .filter_map(|e| serde_json::to_string(e).ok())
                        .collect()
                };
                for line in &drain {
                    if write_line(&mut writer, line).await.is_err() {
                        return;
                    }
                }
                break;
            }
        }
    }
}

#[cfg(unix)]
async fn write_line(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    line: &str,
) -> std::io::Result<()> {
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use json2sql::io::progress_event::ProgressEvent;
    use std::time::Duration;
    use tokio::io::AsyncBufReadExt;
    use tokio::net::UnixStream;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "json2sql-serve-{}-{}.sock",
            tag,
            uuid::Uuid::now_v7()
        ))
    }

    async fn make_listener(path: &std::path::Path) -> tokio::net::UnixListener {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        tokio::net::UnixListener::bind(path).expect("bind test socket")
    }

    #[tokio::test]
    async fn snapshot_sent_to_new_client() {
        let path = temp_path("snap");
        let listener = make_listener(&path).await;
        let summary = Arc::new(Mutex::new(ImportSummary::new()));
        {
            let mut s = summary.lock().await;
            s.push(ProgressEvent::Pass1Log("hello".to_string()));
            s.push(ProgressEvent::Pass1Log("world".to_string()));
        }
        let cancel = CancelToken::new();
        tokio::spawn(serve_connections(
            listener,
            Arc::clone(&summary),
            cancel.clone(),
        ));

        let stream = UnixStream::connect(&path).await.expect("connect");
        let mut lines = BufReader::new(stream).lines();

        let l1 = tokio::time::timeout(Duration::from_millis(200), lines.next_line())
            .await
            .expect("timeout l1")
            .expect("io l1")
            .expect("eof l1");
        let l2 = tokio::time::timeout(Duration::from_millis(200), lines.next_line())
            .await
            .expect("timeout l2")
            .expect("io l2")
            .expect("eof l2");

        let e1: ProgressEvent = serde_json::from_str(&l1).expect("parse e1");
        let e2: ProgressEvent = serde_json::from_str(&l2).expect("parse e2");
        assert!(matches!(e1, ProgressEvent::Pass1Log(m) if m == "hello"));
        assert!(matches!(e2, ProgressEvent::Pass1Log(m) if m == "world"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn delta_events_streamed_to_connected_client() {
        let path = temp_path("delta");
        let listener = make_listener(&path).await;
        let summary = Arc::new(Mutex::new(ImportSummary::new()));
        let cancel = CancelToken::new();
        tokio::spawn(serve_connections(
            listener,
            Arc::clone(&summary),
            cancel.clone(),
        ));

        let stream = UnixStream::connect(&path).await.expect("connect");
        let mut lines = BufReader::new(stream).lines();

        // Push a delta event after connecting
        let sum2 = Arc::clone(&summary);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            sum2.lock()
                .await
                .push(ProgressEvent::Pass1Log("delta".to_string()));
        });

        let line = tokio::time::timeout(Duration::from_millis(500), lines.next_line())
            .await
            .expect("timeout")
            .expect("io")
            .expect("eof");
        let e: ProgressEvent = serde_json::from_str(&line).expect("parse");
        assert!(matches!(e, ProgressEvent::Pass1Log(m) if m == "delta"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn cancel_command_triggers_cancel_token() {
        let path = temp_path("cancel-cmd");
        let listener = make_listener(&path).await;
        let summary = Arc::new(Mutex::new(ImportSummary::new()));
        let cancel = CancelToken::new();
        tokio::spawn(serve_connections(
            listener,
            Arc::clone(&summary),
            cancel.clone(),
        ));

        let mut stream = UnixStream::connect(&path).await.expect("connect");
        tokio::io::AsyncWriteExt::write_all(&mut stream, b"{\"cmd\":\"cancel\"}\n")
            .await
            .expect("write cancel cmd");

        tokio::time::timeout(Duration::from_millis(200), cancel.cancelled())
            .await
            .expect("cancel must fire within 200ms");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn connection_closes_after_pass2done() {
        let path = temp_path("done");
        let listener = make_listener(&path).await;
        let summary = Arc::new(Mutex::new(ImportSummary::new()));
        let cancel = CancelToken::new();
        tokio::spawn(serve_connections(
            listener,
            Arc::clone(&summary),
            cancel.clone(),
        ));

        let stream = UnixStream::connect(&path).await.expect("connect");
        let mut lines = BufReader::new(stream).lines();

        summary.lock().await.push(ProgressEvent::Pass2Done {
            total_rows: 10,
            anomaly_count: 0,
            constraint_warning_count: 0,
        });

        let line = tokio::time::timeout(Duration::from_millis(300), lines.next_line())
            .await
            .expect("timeout")
            .expect("io")
            .expect("eof");
        let e: ProgressEvent = serde_json::from_str(&line).expect("parse");
        assert!(matches!(e, ProgressEvent::Pass2Done { total_rows: 10, .. }));

        // Connection closes after Pass2Done — next read returns EOF
        let eof = tokio::time::timeout(Duration::from_millis(300), lines.next_line())
            .await
            .expect("timeout")
            .expect("io");
        assert!(eof.is_none(), "connection must close (EOF) after Pass2Done");
        let _ = std::fs::remove_file(&path);
    }
}
