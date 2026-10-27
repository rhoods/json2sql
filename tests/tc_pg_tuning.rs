//! #65, #80 — PG tuning survives interruptions: an interrupted Pass 2 puts `max_wal_size` and
//! autovacuum back without waiting for its flusher or for a statement PostgreSQL still runs for it
//! (an interrupt during the load COMMITs lets them all finish), and a killed one (no cleanup at
//! all) is undone by the next import.
//! Own test binary: it points `XDG_STATE_HOME` (restore marker location) at a temp dir.
#![cfg(feature = "integration-tests")]
#![allow(clippy::disallowed_methods)]

mod integration;

use json2sql::db::pg_tuning::{raise_max_wal_size, record_autovacuum_restore, restore_pending};
use json2sql::io::progress_event::ProgressEvent;
use json2sql::{db, pass1, pass2};

/// `max_wal_size` once it equals `expected`, or its last value after 5 s: `pg_reload_conf()`
/// only signals the server, so a session sees the new value shortly after, not instantly.
async fn max_wal_size(client: &tokio_postgres::Client, expected: &str) -> String {
    let mut value = String::new();
    for _ in 0..50 {
        value = client
            .query_one("SHOW max_wal_size", &[])
            .await
            .unwrap()
            .get(0);
        if value == expected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    value
}

/// Resolves once a backend running a statement that matches `pattern` (`LIKE`) waits on a lock.
/// Panics after 20 s.
async fn waits_on_lock(client: &tokio_postgres::Client, pattern: &str) {
    let sql =
        "SELECT count(*) FROM pg_stat_activity WHERE query LIKE $1 AND wait_event_type = 'Lock'";
    for _ in 0..400 {
        if client
            .query_one(sql, &[&pattern])
            .await
            .unwrap()
            .get::<_, i64>(0)
            > 0
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("no statement matching {pattern} waited on a lock within 20 s");
}

/// Whether a backend still runs a statement that matches `pattern` (`LIKE`) after 10 s.
async fn still_running(client: &tokio_postgres::Client, pattern: &str) -> bool {
    let sql = "SELECT count(*) FROM pg_stat_activity WHERE query LIKE $1 AND state = 'active'";
    for _ in 0..100 {
        if client
            .query_one(sql, &[&pattern])
            .await
            .unwrap()
            .get::<_, i64>(0)
            == 0
        {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    true
}

/// Pass 2 returned within its time budget, stopped by the interrupt and not by an error.
fn assert_interrupted(
    result: Result<
        json2sql::error::Result<pass2::runner::Pass2Result>,
        tokio::time::error::Elapsed,
    >,
    step: &str,
) {
    let Ok(Err(e)) = result else {
        panic!("{step}: the interrupt must stop Pass 2 within 30 s")
    };
    assert!(
        e.to_string().contains("interrupted"),
        "{step}: stopped by an error, not by the interrupt: {e}"
    );
}

/// `max_wal_size` back to `before`, autovacuum back on every table of `schema`, nothing pending.
async fn assert_restored(
    client: &tokio_postgres::Client,
    schema: &str,
    before: &str,
    marker: &std::path::Path,
    step: &str,
) {
    assert_eq!(
        max_wal_size(client, before).await,
        before,
        "{step}: max_wal_size not restored"
    );
    let autovacuum_off: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND 'autovacuum_enabled=false' = ANY(c.reloptions)",
            &[&schema],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(autovacuum_off, 0, "{step}: autovacuum not re-enabled");
    assert!(!marker.exists(), "{step}: restore left pending");
}

#[tokio::test]
async fn tune_wal_is_undone_after_interrupt_and_after_kill() {
    let state = tempfile::TempDir::new().unwrap();
    std::env::set_var("XDG_STATE_HOME", state.path()); // single test in this binary: no race
    let marker = state.path().join("json2sql").join("pg_tuning_restore.json");

    integration::with_pg_container(|client, schema, url| async move {
        let before: String = client.query_one("SHOW max_wal_size", &[]).await.unwrap().get(0);

        // 1. Pass 2 interrupted during the load (worker cancel / SIGTERM / Ctrl-C), right after the
        // root's COPY: cleanup runs before Err. Another session reads the grandchild: a flusher left
        // running would block on it, still holding the children's FREEZE locks, and the restore
        // would wait behind them (#80).
        let path = integration::fixture("users.jsonl");
        let p1 = pass1::runner::run(&path, &json2sql::pass1::runner::Pass1Config {
            root_table: "users".to_string(),
            registry: json2sql::schema::registry::RegistryConfig::default(),
            num_workers: None,
        }, None).unwrap();
        // The drain reaches the grandchild only once its parent is written, never in the pass that
        // reports the root.
        let parent = |t: &str| p1.schemas.iter().find(|s| s.name == t).and_then(|s| s.parent_table.clone());
        assert_eq!(parent("users_orders_items").as_deref(), Some("users_orders"));
        assert_eq!(parent("users_orders").as_deref(), Some("users"));
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None).await.unwrap();
        let reader = db::connection::connect(&url).await.unwrap();
        reader.batch_execute(&format!("BEGIN; LOCK TABLE \"{schema}\".users_orders_items IN ACCESS SHARE MODE")).await.unwrap();
        let cfg = pass2::Pass2Config {
            root_table: "users".to_string(),
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
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let root_copied = async move {
            while let Some(event) = rx.recv().await {
                if matches!(event, ProgressEvent::Pass2Flush { ref table_name, .. } if table_name == "users") { break; }
            }
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            pass2::runner::run_interruptible(&path, &p1.schemas, &client, &url, &cfg, Some(tx), root_copied),
        ).await.expect("the interrupt must not wait for the flusher to drain (#80)");
        let Err(e) = result else { panic!("the interrupt must stop Pass 2") };
        assert!(e.to_string().contains("interrupted"), "stopped by the interrupt, not by an error: {e}");
        reader.batch_execute("ROLLBACK").await.unwrap();
        for s in &p1.schemas {
            assert_eq!(integration::row_count(&client, &schema, &s.name).await, 0, "interrupted load left rows in {}", s.name);
        }
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

        // 3-6. Interrupted while PostgreSQL runs a statement for Pass 2. Each one below waits on a
        // lock held by `reader`, so the interrupt always lands while it runs server side.
        let thirty_s = std::time::Duration::from_secs(30);

        // 3. During the load COMMITs, held by a deferred trigger waiting on an advisory lock: they all
        // finish, so the load is kept whole, never partly rolled back.
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, true, None).await.unwrap();
        client.batch_execute(&format!(
            "CREATE FUNCTION \"{schema}\".hold() RETURNS trigger LANGUAGE plpgsql \
             AS $$ BEGIN PERFORM pg_advisory_xact_lock_shared(80); RETURN NULL; END $$"
        )).await.unwrap();
        for s in &p1.schemas {
            client.batch_execute(&format!(
                "CREATE CONSTRAINT TRIGGER hold AFTER INSERT ON \"{schema}\".\"{}\" \
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION \"{schema}\".hold()",
                s.name
            )).await.unwrap();
        }
        reader.batch_execute("SELECT pg_advisory_lock(80)").await.unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let release = async {
            waits_on_lock(&client, "COMMIT").await;
            stop.send(()).unwrap();
            reader.batch_execute("SELECT pg_advisory_unlock(80)").await.unwrap();
        };
        let (result, ()) = tokio::join!(
            tokio::time::timeout(thirty_s, pass2::runner::run_interruptible(
                &path, &p1.schemas, &client, &url, &cfg, None, async { let _ = stopped.await; })),
            release,
        );
        assert_interrupted(result, "COMMIT");
        for s in &p1.schemas {
            assert!(integration::row_count(&client, &schema, &s.name).await > 0, "{} rolled back, other tables committed", s.name);
        }
        assert_restored(&client, &schema, &before, &marker, "COMMIT").await;

        // 4. During the VACUUM, which waits on a lock taken on the root once its rows are loaded
        // (earlier, autovacuum could not be switched off): cancelled, not left waiting server side.
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, true, None).await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let hold = async {
            while let Some(event) = rx.recv().await {
                if matches!(event, ProgressEvent::Pass2Flush { ref table_name, .. } if table_name == "users") { break; }
            }
            reader.batch_execute(&format!("BEGIN; LOCK TABLE \"{schema}\".users IN SHARE UPDATE EXCLUSIVE MODE")).await.unwrap();
            waits_on_lock(&client, "VACUUM%").await;
            stop.send(()).unwrap();
            // The restore waits on this lock too: released once the VACUUM is gone, or after 10 s.
            let left = still_running(&client, "VACUUM%").await;
            reader.batch_execute("ROLLBACK").await.unwrap();
            left
        };
        let (result, left) = tokio::join!(
            tokio::time::timeout(thirty_s, pass2::runner::run_interruptible(
                &path, &p1.schemas, &client, &url, &cfg, Some(tx), async { let _ = stopped.await; })),
            hold,
        );
        assert!(!left, "the interrupted VACUUM must be cancelled, not left waiting for the lock");
        assert_interrupted(result, "VACUUM");
        assert_restored(&client, &schema, &before, &marker, "VACUUM").await;

        // 5. During the PRIMARY KEY builds, the root's waiting on a read lock: cancelled too, else the
        // restore's ALTER TABLE queues behind it.
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, true, None).await.unwrap();
        reader.batch_execute(&format!("BEGIN; LOCK TABLE \"{schema}\".users IN ACCESS SHARE MODE")).await.unwrap();
        let result = tokio::time::timeout(thirty_s, pass2::runner::run_interruptible(
            &path, &p1.schemas, &client, &url, &cfg, None, waits_on_lock(&client, "%PRIMARY KEY%"))).await;
        reader.batch_execute("ROLLBACK").await.unwrap();
        assert_interrupted(result, "PRIMARY KEY");
        assert_restored(&client, &schema, &before, &marker, "PRIMARY KEY").await;

        // 6. During the load, the LOCK that claims the grandchild for COPY FREEZE waiting on a read
        // lock: cancelled with the flusher, else the restore's ALTER TABLE queues behind it.
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, true, None).await.unwrap();
        reader.batch_execute(&format!("BEGIN; LOCK TABLE \"{schema}\".users_orders_items IN ACCESS SHARE MODE")).await.unwrap();
        let result = tokio::time::timeout(thirty_s, pass2::runner::run_interruptible(
            &path, &p1.schemas, &client, &url, &cfg, None, waits_on_lock(&client, "LOCK TABLE%"))).await;
        reader.batch_execute("ROLLBACK").await.unwrap();
        assert_interrupted(result, "claim LOCK");
        assert_restored(&client, &schema, &before, &marker, "claim LOCK").await;
    })
    .await;
}
