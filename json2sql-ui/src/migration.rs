use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::watch;

/// État courant de la migration.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MigrationStatus {
    /// Aucune migration en cours
    Idle,
    /// Migration en cours
    Running {
        started_at_secs: u64,
        elapsed_secs: u64,
    },
    /// Migration terminée avec succès
    Done {
        elapsed_secs: u64,
        rows_per_table: HashMap<String, u64>,
        total_rows: u64,
    },
    /// Migration échouée
    Failed {
        error: String,
        elapsed_secs: u64,
    },
}

/// Handle partagé pour lire et mettre à jour l'état de migration.
pub struct MigrationState {
    pub sender: watch::Sender<MigrationStatus>,
    pub receiver: watch::Receiver<MigrationStatus>,
}

impl MigrationState {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(MigrationStatus::Idle);
        Self { sender, receiver }
    }

    /// Marks migration as running. Returns (Instant, started_at_secs) for the caller to use
    /// when sending periodic elapsed updates and the final Done/Failed status.
    pub fn set_running(&self) -> (Instant, u64) {
        let started = Instant::now();
        let started_at_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = self.sender.send(MigrationStatus::Running {
            started_at_secs,
            elapsed_secs: 0,
        });
        (started, started_at_secs)
    }

    pub fn update_elapsed(&self, started_at_secs: u64, elapsed: u64) {
        let _ = self.sender.send(MigrationStatus::Running {
            started_at_secs,
            elapsed_secs: elapsed,
        });
    }

    pub fn set_done(&self, elapsed: Duration, rows_per_table: HashMap<String, u64>) {
        let total_rows = rows_per_table.values().sum();
        let _ = self.sender.send(MigrationStatus::Done {
            elapsed_secs: elapsed.as_secs(),
            rows_per_table,
            total_rows,
        });
    }

    pub fn set_failed(&self, error: String, elapsed: Duration) {
        let _ = self.sender.send(MigrationStatus::Failed {
            error,
            elapsed_secs: elapsed.as_secs(),
        });
    }

    pub fn is_running(&self) -> bool {
        matches!(*self.receiver.borrow(), MigrationStatus::Running { .. })
    }
}
