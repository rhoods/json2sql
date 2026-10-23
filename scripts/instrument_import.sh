#!/usr/bin/env bash
# Instrumented UI import run (issue #58): launches json2sql-ui with timestamped stderr
# plus system/PG monitors, all logs in one run directory.
# Usage: DATABASE_URL=postgres://user:pw@localhost/db scripts/instrument_import.sh [run_root]
# In the UI: tick "verbose logs", then run the import as usual.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
RUN="${1:-/media/dylan/data/j2s_runs}/$(date +%F_%H%M)"
INTERVAL=10
mkdir -p "$RUN"
START="$(date '+%F %T')"
echo "Logs → $RUN"

cargo build --release --manifest-path "$REPO/Cargo.toml" -p json2sql-ui -p json2sql-worker || exit 1

# Prefix every line with a wall-clock timestamp (bash builtin, no extra dependency).
stamp() { while IFS= read -r l; do printf '%(%F %T)T %s\n' -1 "$l"; done; }

PIDS=()
cleanup() {
    kill "${PIDS[@]}" 2>/dev/null
    wait "${PIDS[@]}" 2>/dev/null
    journalctl -k --since "$START" 2>&1 | grep -iE 'oom|killed process' > "$RUN/kernel.log"
    if [[ -r /var/log/postgresql/postgresql-17-main.log ]]; then
        tail -n 500 /var/log/postgresql/postgresql-17-main.log > "$RUN/postgres_server.log"
    else
        echo "(postgres log not readable — run: sudo tail -n 500 /var/log/postgresql/postgresql-17-main.log)" > "$RUN/postgres_server.log"
    fi
    df -h / /media/dylan/data >> "$RUN/kernel.log"
    echo "Done. Logs in $RUN"
}
trap cleanup EXIT

iostat -xmt "$INTERVAL" > "$RUN/iostat.log" 2>&1 & PIDS+=($!)
pidstat -urdh -C 'json2sql|postgres' "$INTERVAL" > "$RUN/pidstat.log" 2>&1 & PIDS+=($!)
vmstat -t "$INTERVAL" > "$RUN/vmstat.log" 2>&1 & PIDS+=($!)

if [[ -n "${DATABASE_URL:-}" ]]; then
    (
        while true; do
            echo "=== $(date '+%F %T')"
            psql "$DATABASE_URL" -X -q -c "
                SELECT pid, state, wait_event_type, wait_event,
                       now() - query_start AS query_age, left(query, 80) AS query
                FROM pg_stat_activity
                WHERE datname = current_database() AND pid <> pg_backend_pid();" \
                -c "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size;" \
                -c "SELECT pg_current_wal_lsn() AS wal_lsn,
                       (SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()) AS temp_bytes,
                       (SELECT sum(writes * op_bytes) FROM pg_stat_io WHERE context = 'bulkread') AS scan_rewrite_bytes,
                       (SELECT sum(writes * op_bytes) FROM pg_stat_io) AS buffer_write_bytes;" 2>&1
            df -h / | tail -1
            sleep 60
        done
    ) > "$RUN/pg.log" 2>&1 & PIDS+=($!)
else
    echo "WARNING: DATABASE_URL not set — pg.log skipped" | tee "$RUN/pg.log"
fi

# time -v writes max RSS + exit status to stderr → captured in app.log too.
/usr/bin/time -v "$REPO/target/release/json2sql-ui" 2>&1 | stamp > "$RUN/app.log"
