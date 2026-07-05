//! État en mémoire d'un import en cours — historique d'événements + notification des connexions.
//!
//! Une nouvelle connexion socket reçoit l'historique complet (snapshot) puis attend les
//! événements incrémentaux via `Notify`, évitant les races snapshot/delta d'un broadcast channel.
//!
//! Fonctions :
//! - `ImportSummary::new` — crée un résumé vide.
//! - `ImportSummary::push` — ajoute un événement, marque `done` sur `Pass2Done`, réveille les connexions.
//! - `ImportSummary::snapshot` — tous les événements accumulés.
//! - `ImportSummary::len`, `::is_done` — nombre d'événements, statut de complétion.
//! - `ImportSummary::notifier` — clone du handle `Notify` pour qu'un connection handler attende.

use std::sync::Arc;

use json2sql::io::progress_event::ProgressEvent;

/// Shared in-memory state of an in-progress import.
///
/// New socket connections receive the full event history (snapshot), then wait
/// for incremental events via a `tokio::sync::Notify`. This avoids broadcast
/// channels and eliminates snapshot/delta race conditions.
pub struct ImportSummary {
    events: Vec<ProgressEvent>,
    done: bool,
    notify: Arc<tokio::sync::Notify>,
}

impl ImportSummary {
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
            done: false,
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Append an event and wake any waiting connection handlers.
    pub fn push(&mut self, event: ProgressEvent) {
        if matches!(event, ProgressEvent::Pass2Done { .. }) {
            self.done = true;
        }
        self.events.push(event);
        self.notify.notify_one();
    }

    /// All events accumulated since the worker started.
    pub fn snapshot(&self) -> &[ProgressEvent] {
        &self.events
    }

    /// Total events accumulated so far.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// True once `Pass2Done` has been pushed.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Returns a clone of the notify handle so connection handlers can await new events.
    pub fn notifier(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.notify)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_log(msg: &str) -> ProgressEvent {
        ProgressEvent::Pass1Log(msg.to_string())
    }

    #[test]
    fn new_summary_is_empty_and_not_done() {
        let s = ImportSummary::new();
        assert_eq!(s.len(), 0);
        assert!(s.snapshot().is_empty());
        assert!(!s.is_done());
    }

    #[test]
    fn push_appends_event_to_snapshot() {
        let mut s = ImportSummary::new();
        s.push(make_log("hello"));
        assert_eq!(s.len(), 1);
        assert!(matches!(&s.snapshot()[0], ProgressEvent::Pass1Log(m) if m == "hello"));
    }

    #[test]
    fn push_multiple_events_in_order() {
        let mut s = ImportSummary::new();
        s.push(make_log("first"));
        s.push(make_log("second"));
        s.push(ProgressEvent::DdlStart { table_count: 3 });
        assert_eq!(s.len(), 3);
        assert!(matches!(&s.snapshot()[2], ProgressEvent::DdlStart { table_count: 3 }));
    }

    #[test]
    fn pass2done_marks_done() {
        let mut s = ImportSummary::new();
        s.push(make_log("before"));
        assert!(!s.is_done());
        s.push(ProgressEvent::Pass2Done {
            total_rows: 100,
            anomaly_count: 0,
            constraint_warning_count: 0,
        });
        assert!(s.is_done());
        assert_eq!(s.len(), 2);
    }

    #[test]
    fn snapshot_after_done_contains_all_events() {
        let mut s = ImportSummary::new();
        for i in 0..5 {
            s.push(make_log(&format!("msg{i}")));
        }
        s.push(ProgressEvent::Pass2Done { total_rows: 5, anomaly_count: 0, constraint_warning_count: 0 });
        assert_eq!(s.snapshot().len(), 6);
        assert!(s.is_done());
    }

    #[tokio::test]
    async fn notifier_is_woken_after_push() {
        let mut s = ImportSummary::new();
        let notify = s.notifier();

        // Spawn a task that waits for notification
        let handle = tokio::spawn(async move {
            notify.notified().await;
        });

        s.push(make_log("trigger"));

        // The spawned task should complete quickly
        tokio::time::timeout(std::time::Duration::from_millis(100), handle)
            .await
            .expect("timeout — notifier was not woken")
            .expect("task panicked");
    }

    #[test]
    fn notifier_is_shared_clone() {
        let s = ImportSummary::new();
        let n1 = s.notifier();
        let n2 = s.notifier();
        // Both point to the same underlying Notify (same Arc target)
        assert!(Arc::ptr_eq(&n1, &n2));
    }
}
