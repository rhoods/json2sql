//! #65 — PG tuning survives interruptions: an interrupted Pass 2 puts `max_wal_size` and
//! autovacuum back, and a killed one (no cleanup at all) is undone by the next import.
//! Own test binary: it points `XDG_STATE_HOME` (restore marker location) at a temp dir.
#![cfg(feature = "integration-tests")]
#![allow(clippy::disallowed_methods)]

mod integration;

use json2sql::db::pg_tuning::{raise_max_wal_size, record_autovacuum_restore, restore_pending};
use json2sql::{db, pass1, pass2};

/// `max_wal_size` once it equals `expected`, or its last value after 5 s: `pg_reload_conf()`
/// only signals the server, so a session sees the new value shortly after, not instantly.
async fn max_wal_size(client: &tokio_postgres::Client, expected: &str) -> String {
    let mut value = String::new();
    for _ in 0..50 {
        value = client.query_one("SHOW max_wal_size", &[]).await.unwrap().get(0);
        if value == expected { break; }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    value
}

#[tokio::test]
async fn tune_wal_is_undone_after_interrupt_and_after_kill() {
    let state = tempfile::TempDir::new().unwrap();
    std::env::set_var("XDG_STATE_HOME", state.path()); // single test in this binary: no race
    let marker = state.path().join("json2sql").join("pg_tuning_restore.json");

    integration::with_pg_container(|client, schema, url| async move {
        let before: String = client.query_one("SHOW max_wal_size", &[]).await.unwrap().get(0);

        // 1. Interrupted Pass 2 (worker cancel / SIGTERM / Ctrl-C): cleanup runs before Err.
        let path = integration::fixture("tc_nested_fk.jsonl");
        let p1 = pass1::runner::run(&path, &json2sql::pass1::runner::Pass1Config {
            root_table: "orders".to_string(),
            registry: json2sql::schema::registry::RegistryConfig::default(),
            num_workers: None,
        }, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None).await.unwrap();
        let cfg = pass2::Pass2Config {
            root_table: "orders".to_string(),
            pg_schema: schema.clone(),
            parallel: 2,
            anomaly_dir: None,
            limit: None,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
            hint_format: None,
            skip_constraints: false,
            tune_wal: true,
        };
        let result = pass2::runner::run_interruptible(&path, &p1.schemas, &client, &url, &cfg, None, async {}).await;
        assert!(result.is_err(), "an immediate interrupt must stop Pass 2");
        assert_eq!(max_wal_size(&client, &before).await, before, "interrupt must restore max_wal_size");
        let autovacuum_off: i64 = client.query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND 'autovacuum_enabled=false' = ANY(c.reloptions)",
            &[&schema],
        ).await.unwrap().get(0);
        assert_eq!(autovacuum_off, 0, "interrupt must re-enable autovacuum");
        assert!(!marker.exists(), "nothing left pending after a clean restore");

        // 2. Killed import: raised + autovacuum off, then no cleanup at all (kill -9).
        assert!(raise_max_wal_size(&client, None).await.is_some(), "container role is superuser");
        assert!(record_autovacuum_restore(&url, &p1.schemas, &schema, None).is_some());
        db::ddl::set_tables_autovacuum(&url, &p1.schemas, &schema, false, 2).await.unwrap();
        assert_eq!(max_wal_size(&client, "16GB").await, "16GB");
        assert!(marker.exists(), "restore recorded before raising");

        // The next import starts with restore_pending (also when 16 GB is already in place).
        restore_pending(&client, &url, None).await;
        assert_eq!(max_wal_size(&client, &before).await, before, "next import must undo the killed one");
        let autovacuum_off: i64 = client.query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND 'autovacuum_enabled=false' = ANY(c.reloptions)",
            &[&schema],
        ).await.unwrap().get(0);
        assert_eq!(autovacuum_off, 0, "next import must re-enable autovacuum left off by the killed one");
        assert!(!marker.exists(), "both restores applied and cleared");
    })
    .await;
}
