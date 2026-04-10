/// Events streamed by Pass 1 and Pass 2 runners to the IHM via an unbounded channel.
///
/// The sender is optional — when None the runners emit nothing (CLI / batch mode).
/// The IHM creates the channel, passes the sender to the runners, and consumes events
/// in a Dioxus coroutine to update the UI.
#[derive(Debug, Clone)]
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
    /// A plain-text log message (mirrors what the CLI prints to stderr).
    Pass2Log(String),
    /// Pass 2 finished — all rows imported.
    Pass2Done {
        total_rows: u64,
        anomaly_count: u64,
    },
}

/// Convenience alias for the sender half of a progress channel.
pub type ProgressTx = tokio::sync::mpsc::UnboundedSender<ProgressEvent>;
