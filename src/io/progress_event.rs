//! Channel-based progress events streamed from Pass 1/2 runners to the IHM.
//!
//! The sender ([`ProgressTx`]) is optional — runners emit nothing when `None` (CLI mode).
//! Events are consumed in a Dioxus coroutine to drive UI state updates.
//!
//! Fonctions :
//! - enum `ProgressEvent` — événements Pass 1/Pass 2/DDL/contraintes streamés vers l'IHM.

/// Events streamed by Pass 1 and Pass 2 runners to the IHM via an unbounded channel.
///
/// The sender is optional — when None the runners emit nothing (CLI / batch mode).
/// The IHM creates the channel, passes the sender to the runners, and consumes events
/// in a Dioxus coroutine to update the UI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[allow(dead_code)]
pub enum ProgressEvent {
    // ── Pass 1 ───────────────────────────────────────────────────────────────
    /// A plain-text informational message from Pass 1 (mirrors what the CLI prints to stderr).
    Pass1Log(String),
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
    fn pass1_log_constructs_and_clones() {
        let e = ProgressEvent::Pass1Log("Root wrapper detected — streaming keys: [Foods]".to_string());
        let cloned = e.clone();
        assert!(format!("{cloned:?}").contains("Foods"));
    }

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

    #[test]
    fn progress_event_serde_round_trip() {
        let events = vec![
            ProgressEvent::Pass2Done { total_rows: 42, anomaly_count: 1, constraint_warning_count: 0 },
            ProgressEvent::Pass2Flush { table_name: "orders".to_string(), rows_flushed: 100 },
            ProgressEvent::DdlStart { table_count: 5 },
            ProgressEvent::Pass1Log("hello".to_string()),
            ProgressEvent::ConstraintsStart { table_count: 3 },
        ];
        for e in events {
            let json = serde_json::to_string(&e).expect("serialize");
            let back: ProgressEvent = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(format!("{e:?}"), format!("{back:?}"));
        }
    }
}
