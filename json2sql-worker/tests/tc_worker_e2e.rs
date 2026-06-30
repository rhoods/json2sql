//! End-to-end test: spawn the real `json2sql-worker` binary against a PostgreSQL container,
//! verify that the socket streams `DdlStart` … `Pass2Done` and that the result file is correct.
#![cfg(feature = "integration-tests")]

use std::collections::HashSet;
use std::time::Duration;

use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::io::AsyncBufReadExt as _;

use json2sql::ipc::{new_socket_path, WorkerConfig, WorkerResult};
use json2sql::io::progress_event::ProgressEvent;
use json2sql::pass1::runner::{Pass1Config, run as run_pass1};
use json2sql::schema::registry::RegistryConfig;

// Path to the compiled worker binary, injected by Cargo at test build time.
// Falls back to looking for the binary adjacent to the test executable.
fn worker_bin_path() -> std::path::PathBuf {
    // CARGO_BIN_EXE_json2sql-worker is set by Cargo for integration tests in the same package.
    let from_env = option_env!("CARGO_BIN_EXE_json2sql-worker");
    if let Some(path) = from_env {
        let p = std::path::PathBuf::from(path);
        if p.exists() {
            return p;
        }
    }
    // Fallback: resolve relative to CARGO_MANIFEST_DIR (works when running via `cargo test`).
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // target/{profile}/json2sql-worker — walk from manifest up to workspace root.
    let workspace = std::path::Path::new(manifest_dir).parent().unwrap();
    let candidates = [
        workspace.join("target/debug/json2sql-worker"),
        workspace.join("target/release/json2sql-worker"),
    ];
    for c in &candidates {
        if c.exists() {
            return c.clone();
        }
    }
    panic!("Cannot locate json2sql-worker binary. Run `cargo build -p json2sql-worker` first.");
}

/// Write a tiny 3-row JSON fixture and return its path.
fn write_fixture() -> std::path::PathBuf {
    let path = std::env::temp_dir()
        .join(format!("worker_e2e_{}.json", uuid::Uuid::now_v7()));
    std::fs::write(
        &path,
        b"{\"id\":1,\"name\":\"Alice\",\"age\":30}\n\
          {\"id\":2,\"name\":\"Bob\",\"age\":25}\n\
          {\"id\":3,\"name\":\"Charlie\",\"age\":35}\n",
    )
    .expect("write fixture");
    path
}

/// Connect to a Unix socket, retrying up to 20 times with 100 ms between attempts.
#[cfg(unix)]
async fn connect_socket(path: &std::path::Path) -> tokio::net::UnixStream {
    for _ in 0..20u32 {
        if let Ok(s) = tokio::net::UnixStream::connect(path).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for worker socket at {}", path.display());
}

#[cfg(unix)]
#[tokio::test]
async fn worker_e2e_small_import_streams_events_and_writes_result() {
    // ── Start PostgreSQL container ─────────────────────────────────────────
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("failed to start postgres container — is Docker running?");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get PG port");

    // ── Prepare fixture + run pass1 to get schemas ─────────────────────────
    let fixture = write_fixture();
    let root_table = "worker_e2e".to_string();

    let pass1_result = run_pass1(
        &fixture,
        &Pass1Config {
            root_table: root_table.clone(),
            registry: RegistryConfig {
                text_threshold: 256,
                array_as_pg_array: false,
                wide_column_threshold: 100,
                sibling_threshold: 3,
                sibling_jaccard: 0.5,
                stable_threshold: 0.10,
                rare_threshold: 0.001,
                disabled_strategies: HashSet::new(),
            },
            num_workers: None,
        },
        None,
    )
    .expect("pass1 must succeed");

    assert!(!pass1_result.schemas.is_empty(), "pass1 must infer at least one table");

    // ── Create the target schema in PG ────────────────────────────────────
    let schema_name = format!("e2e_{}", &uuid::Uuid::now_v7().to_string().replace('-', "")[20..]);
    {
        let pg_url = format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        );
        let (client, conn) = tokio_postgres::connect(&pg_url, tokio_postgres::NoTls)
            .await
            .expect("connect to PG for schema creation");
        tokio::spawn(async move { let _ = conn.await; });
        client
            .execute(&format!("CREATE SCHEMA \"{}\"", schema_name), &[])
            .await
            .expect("create schema");
    }

    // ── Build WorkerConfig ─────────────────────────────────────────────────
    let socket_path = new_socket_path();
    let result_file = socket_path.with_extension("json");

    let cfg = WorkerConfig {
        source_file: fixture.clone(),
        root_table: root_table.clone(),
        pg_host: "127.0.0.1".to_string(),
        pg_port: port,
        pg_database: "postgres".to_string(),
        pg_user: "postgres".to_string(),
        pg_schema: schema_name.clone(),
        schemas: pass1_result.schemas,
        drop_existing: true,
        anomaly_dir: None,
        pass2_parallel: 1,
        import_limit: None,
        verbose_logs: false,
        hint_format: pass1_result.detected_format,
        skip_constraints: false,
        socket_path: socket_path.clone(),
        result_file: result_file.clone(),
    };
    let cfg_json = serde_json::to_vec(&cfg).expect("serialize config");

    // ── Spawn the worker binary ────────────────────────────────────────────
    use tokio::io::AsyncWriteExt as _;
    use tokio::process::Command;

    let bin = worker_bin_path();
    eprintln!("Worker binary: {}", bin.display());

    let mut child = Command::new(&bin)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .env("J2S_PG_PASSWORD", "postgres")
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn {}: {e}", bin.display()));

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&cfg_json).await.expect("write config");
        // stdin drops here → worker reads EOF and starts
    }

    // ── Connect to socket and collect events ──────────────────────────────
    let stream = connect_socket(&socket_path).await;
    let mut lines = tokio::io::BufReader::new(stream).lines();

    let mut received: Vec<String> = Vec::new();
    let mut pass2_done = false;

    loop {
        let next = tokio::time::timeout(Duration::from_secs(60), lines.next_line())
            .await
            .expect("timed out waiting for next event (60s)")
            .expect("IO error reading socket");

        match next {
            None => {
                // EOF — worker closed the connection
                break;
            }
            Some(line) => {
                if let Ok(event) = serde_json::from_str::<ProgressEvent>(&line) {
                    let variant = variant_name(&event);
                    eprintln!("  event received: {variant}");
                    received.push(variant.to_string());
                    if matches!(event, ProgressEvent::Pass2Done { .. }) {
                        pass2_done = true;
                        break;
                    }
                }
            }
        }
    }

    // ── Wait for the worker to exit ────────────────────────────────────────
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("worker exit timeout")
        .expect("wait error");
    assert!(status.success(), "worker must exit 0, got: {status:?}");

    // ── Assert result file ────────────────────────────────────────────────
    let result_content =
        std::fs::read_to_string(&result_file).expect("result file must exist after worker exits");
    let result: WorkerResult =
        serde_json::from_str(&result_content).expect("result file must be valid JSON");
    eprintln!("  result: {result:?}");
    assert_eq!(result.status, "success", "import must succeed — got: {result:?}\nevents received: {received:?}");
    assert_eq!(result.total_rows, 3, "must import all 3 rows: {result:?}");

    // ── Assert event stream ────────────────────────────────────────────────
    assert!(pass2_done, "Pass2Done must be received; got: {received:?}");
    assert!(
        received.iter().any(|e| e.contains("Ddl")),
        "at least one DDL event must be received; got: {received:?}"
    );

    // ── Cleanup ────────────────────────────────────────────────────────────
    let _ = std::fs::remove_file(&fixture);
    let _ = std::fs::remove_file(&result_file);
    let _ = std::fs::remove_file(&socket_path);
}

/// Extract a short variant name from a ProgressEvent for assertion messages.
fn variant_name(event: &ProgressEvent) -> &'static str {
    match event {
        ProgressEvent::Pass1Log(_) => "Pass1Log",
        ProgressEvent::Pass1Progress { .. } => "Pass1Progress",
        ProgressEvent::Pass1Done { .. } => "Pass1Done",
        ProgressEvent::DdlStart { .. } => "DdlStart",
        ProgressEvent::DdlProgress { .. } => "DdlProgress",
        ProgressEvent::DdlDone { .. } => "DdlDone",
        ProgressEvent::Pass2Progress { .. } => "Pass2Progress",
        ProgressEvent::Pass2Flush { .. } => "Pass2Flush",
        ProgressEvent::Pass2AnomalyUpdate { .. } => "Pass2AnomalyUpdate",
        ProgressEvent::Pass2Log(_) => "Pass2Log",
        ProgressEvent::Pass2Done { .. } => "Pass2Done",
        ProgressEvent::Pass2Error { .. } => "Pass2Error",
        ProgressEvent::ConstraintsStart { .. } => "ConstraintsStart",
        ProgressEvent::ConstraintsProgress { .. } => "ConstraintsProgress",
        ProgressEvent::ConstraintsDone { .. } => "ConstraintsDone",
    }
}
