//! Signal d'annulation partagé entre le handler de commande socket et la boucle d'import.
//!
//! Fonctions :
//! - `CancelToken::new` — crée un token non annulé.
//! - `CancelToken::cancel` — signale l'annulation (idempotent).
//! - `CancelToken::is_cancelled` — lecture immédiate de l'état.
//! - `CancelToken::cancelled` — attend l'annulation (résout immédiatement si déjà annulé).

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use tokio::sync::Notify;

/// Shared cancellation signal between the socket command handler and the import pipeline.
///
/// Clone is cheap (Arc clone). The import loop calls `is_cancelled()` periodically;
/// the socket handler calls `cancel()` when it receives `{"cmd":"cancel"}`.
#[derive(Clone)]
pub struct CancelToken(Arc<Inner>);

struct Inner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancelToken {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    /// Signal cancellation. Safe to call multiple times.
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }

    /// Returns `true` if `cancel()` has been called.
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    /// Async wait — resolves immediately if already cancelled, otherwise blocks until `cancel()`.
    pub async fn cancelled(&self) {
        // Fast path: already cancelled.
        if self.is_cancelled() {
            return;
        }
        // Slow path: wait for the notify, then re-check (spurious wake guard).
        loop {
            self.0.notify.notified().await;
            if self.is_cancelled() {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn new_token_is_not_cancelled() {
        let t = CancelToken::new();
        assert!(!t.is_cancelled());
    }

    #[test]
    fn cancel_sets_is_cancelled() {
        let t = CancelToken::new();
        t.cancel();
        assert!(t.is_cancelled());
    }

    #[test]
    fn cancel_is_idempotent() {
        let t = CancelToken::new();
        t.cancel();
        t.cancel();
        assert!(t.is_cancelled());
    }

    #[test]
    fn clone_shares_state() {
        let t1 = CancelToken::new();
        let t2 = t1.clone();
        assert!(!t2.is_cancelled());
        t1.cancel();
        assert!(t2.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_resolves_immediately_if_already_cancelled() {
        let t = CancelToken::new();
        t.cancel();
        tokio::time::timeout(Duration::from_millis(10), t.cancelled())
            .await
            .expect("must resolve immediately when already cancelled");
    }

    #[tokio::test]
    async fn cancelled_wakes_when_cancel_is_called() {
        let t = CancelToken::new();
        let t2 = t.clone();
        let handle = tokio::spawn(async move { t2.cancelled().await });
        tokio::task::yield_now().await; // let the spawned task park on notify
        t.cancel();
        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("timeout — cancelled() was not woken")
            .expect("task panicked");
    }
}
