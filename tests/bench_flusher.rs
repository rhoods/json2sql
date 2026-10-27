//! #59 — timed bench of the flusher drain with the disk taken out of the picture (PG data dir on tmpfs).
//!
//! Compares, for COPY buffers already in RAM:
//! - `seq-1conn` (one client, tables drained one after another, as `run_flusher` does)
//!   vs `par-Nconn` (one client per table, drained concurrently);
//! - LOGGED vs UNLOGGED tables (WAL cost, cf. `LWLock:WALWrite` observed in #58).
//!
//! Run: `cargo test --features integration-tests --test bench_flusher -- --ignored --nocapture`
//! Override the matrix with `J2S_BENCH_TABLES=4,16` / `J2S_BENCH_ROWS=200000,1000000`.
#![cfg(feature = "integration-tests")]
#![allow(clippy::disallowed_methods)]

use std::time::Instant;

use bytes::Bytes;
use testcontainers::{core::Mount, runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::Client;

use json2sql::db::connection::connect;
use json2sql::db::copy_sink::flush_mem_sink_to_pg;

const SCHEMA: &str = "j2s_bench";

fn env_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |v| {
            v.split(',')
                .map(|s| s.trim().parse().expect("bad bench env value"))
                .collect()
        },
    )
}

/// COPY-text buffer of `rows` rows `(uuid, bigint, text)`, shaped like a typical child table.
fn build_buffer(rows: usize) -> Bytes {
    let mut buf = String::with_capacity(rows * 80);
    for i in 0..rows {
        buf.push_str(&format!(
            "{}\t{i}\tvalue number {i} with some padding text\n",
            uuid::Uuid::now_v7()
        ));
    }
    Bytes::from(buf)
}

async fn bench_client(url: &str) -> Client {
    let c = connect(url).await.expect("connect");
    c.execute("SET synchronous_commit = off", &[])
        .await
        .unwrap();
    c
}

async fn reset_schema(client: &Client, tables: usize, unlogged: bool) {
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA}"
        ))
        .await
        .unwrap();
    let kind = if unlogged { "UNLOGGED " } else { "" };
    for t in 0..tables {
        client
            .batch_execute(&format!(
                "CREATE {kind}TABLE {SCHEMA}.t{t} (id uuid, n bigint, s text)"
            ))
            .await
            .unwrap();
    }
}

fn copy_sql(t: usize) -> String {
    format!("COPY {SCHEMA}.t{t} (id, n, s) FROM STDIN")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "bench: run explicitly with --ignored --nocapture"]
async fn bench_flusher_drain() {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .with_mount(Mount::tmpfs_mount("/var/lib/postgresql/data"))
        .start()
        .await
        .expect("failed to start postgres container — is Docker running?");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let admin = bench_client(&url).await;

    eprintln!("| tables | rows/table | mode | wal | MB | ms | MB/s | Mrows/s |");
    eprintln!("|---|---|---|---|---|---|---|---|");
    for &rows in &env_list("J2S_BENCH_ROWS", &[200_000, 1_000_000]) {
        let buf = build_buffer(rows);
        for &tables in &env_list("J2S_BENCH_TABLES", &[4, 16]) {
            for unlogged in [false, true] {
                for parallel in [false, true] {
                    reset_schema(&admin, tables, unlogged).await;
                    // Connections opened outside the timer: we measure the drain, not the handshake.
                    let clients: Vec<Client> = if parallel {
                        futures_util::future::join_all((0..tables).map(|_| bench_client(&url)))
                            .await
                    } else {
                        vec![bench_client(&url).await]
                    };

                    let start = Instant::now();
                    if parallel {
                        let mut set = tokio::task::JoinSet::new();
                        for (t, client) in clients.into_iter().enumerate() {
                            let buf = buf.clone();
                            set.spawn(async move {
                                flush_mem_sink_to_pg(buf, &copy_sql(t), &client).await
                            });
                        }
                        while let Some(r) = set.join_next().await {
                            r.unwrap().expect("parallel COPY failed");
                        }
                    } else {
                        for t in 0..tables {
                            flush_mem_sink_to_pg(buf.clone(), &copy_sql(t), &clients[0])
                                .await
                                .expect("COPY failed");
                        }
                    }
                    let elapsed = start.elapsed();

                    let mut total: i64 = 0;
                    for t in 0..tables {
                        total += admin
                            .query_one(&format!("SELECT count(*) FROM {SCHEMA}.t{t}"), &[])
                            .await
                            .unwrap()
                            .get::<_, i64>(0);
                    }
                    assert_eq!(total, (tables * rows) as i64, "row count mismatch");

                    let mb = (buf.len() * tables) as f64 / 1_048_576.0;
                    let secs = elapsed.as_secs_f64();
                    eprintln!(
                        "| {tables} | {rows} | {} | {} | {mb:.0} | {} | {:.1} | {:.2} |",
                        if parallel {
                            format!("par-{tables}conn")
                        } else {
                            "seq-1conn".to_string()
                        },
                        if unlogged { "UNLOGGED" } else { "LOGGED" },
                        elapsed.as_millis(),
                        mb / secs,
                        (tables * rows) as f64 / secs / 1e6,
                    );
                }
            }
        }
    }
}
