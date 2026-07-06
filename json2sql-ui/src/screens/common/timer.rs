//! Hook Dioxus : compteur de secondes écoulées, utilisé par les écrans Analysis et Import
//! pendant une passe de traitement en cours.
//!
//! Fonctions :
//! - fn `use_elapsed_timer` — hook Dioxus : incrémente un compteur de secondes jusqu'à ce que `is_done()` retourne vrai
#![allow(clippy::disallowed_methods)]
use dioxus::prelude::*;

/// Increments a seconds counter every second until `is_done()` returns true.
/// Returns the `Signal<u32>` so the caller can display elapsed time.
pub fn use_elapsed_timer<F>(is_done: F) -> Signal<u32>
where
    F: Fn() -> bool + Clone + 'static,
{
    let mut elapsed_secs: Signal<u32> = use_signal(|| 0);
    use_coroutine(move |_: UnboundedReceiver<()>| {
        let is_done = is_done.clone();
        async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if is_done() { break; }
                let e = *elapsed_secs.read();
                if e < u32::MAX { *elapsed_secs.write() = e + 1; }
            }
        }
    });
    elapsed_secs
}
