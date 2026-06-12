//! Channel-based progress events streamed from Pass 1/2 runners to the IHM.
//!
//! The sender ([`ProgressTx`]) is optional — runners emit nothing when `None` (CLI mode).
//! Events are consumed in a Dioxus coroutine to drive UI state updates.

/// Events streamed by Pass 1 and Pass 2 runners to the IHM via an unbounded channel.
///
/// The sender is optional — when None the runners emit nothing (CLI / batch mode).
/// The IHM creates the channel, passes the sender to the runners, and consumes events
/// in a Dioxus coroutine to update the UI.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ProgressEvent {
    // ── Pass 1 ───────────────────────────────────────────────────────────────
    /// Periodic scan progress during schema inference.
    Pass1Progress {
        rows_scanned: u64,
        bytes_read: u64,
        total_bytes: u64,
    },
    /// Pass 1 finished — schema is ready.
    Pass1Done {
        total_rows: u64,
        tables_count: usize,
        columns_count: usize,
    },

    // ── DDL (table creation before Pass 2) ───────────────────────────────────
    /// Table creation phase starting.
    DdlStart { table_count: usize },
    /// One table created — done/total counter for progress display.
    DdlProgress { done: usize, total: usize },
    /// All tables created — DDL phase complete.
    DdlDone,

    // ── Constraints (PK + FK applied after data load) ─────────────────────────
    /// Constraint-application phase starting.
    ConstraintsStart { table_count: usize },
    /// One constraint applied — done counts across both PK and FK phases.
    ConstraintsProgress { done: usize, total: usize },
    /// All constraints applied — constraint phase complete.
    ConstraintsDone,

    // ── Pass 2 ───────────────────────────────────────────────────────────────
    /// Periodic row-processing progress during import.
    Pass2Progress {
        rows_processed: u64,
        bytes_read: u64,
        total_bytes: u64,
    },
    /// A batch of rows was flushed (COPY) for a specific table.
    Pass2Flush {
        table_name: String,
        rows_flushed: u64,
    },
    /// A COPY failed for a specific table — emitted in real time before `run()` returns Err.
    Pass2Error {
        table_name: String,
        message: String,
    },
    /// Per-table anomaly count, emitted once per table after all workers complete.
    Pass2AnomalyUpdate {
        table_name: String,
        count: u64,
    },
    /// A plain-text log message (mirrors what the CLI prints to stderr).
    Pass2Log(String),
    /// Pass 2 finished — all rows imported.
    Pass2Done {
        total_rows: u64,
        anomaly_count: u64,
        /// Number of FK constraints that could not be applied after import.
        /// PK failures are fatal (surfaced as errors); only FK failures appear here.
        constraint_warning_count: u64,
    },
}

/// Convenience alias for the sender half of a progress channel.
pub type ProgressTx = tokio::sync::mpsc::UnboundedSender<ProgressEvent>;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_and_constraints_variants_construct_and_clone() {
        let events: Vec<ProgressEvent> = vec![
            ProgressEvent::DdlStart { table_count: 3 },
            ProgressEvent::DdlProgress { done: 1, total: 3 },
            ProgressEvent::DdlDone,
            ProgressEvent::ConstraintsStart { table_count: 3 },
            ProgressEvent::ConstraintsProgress { done: 1, total: 6 },
            ProgressEvent::ConstraintsDone,
        ];
        let cloned = events.clone();
        for e in &cloned {
            let _ = format!("{e:?}");
        }
    }

    #[test]
    fn pass2_error_constructs_with_table_name_and_message() {
        let e = ProgressEvent::Pass2Error {
            table_name: "orders".to_string(),
            message: "COPY failed: duplicate key".to_string(),
        };
        let cloned = e.clone();
        let debug = format!("{cloned:?}");
        assert!(debug.contains("orders"));
        assert!(debug.contains("duplicate key"));
    }
}
