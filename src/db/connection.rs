//! `PostgreSQL` connection helper — wraps [`tokio_postgres::connect`] and spawns the
//! background connection driver task.
//!
//! Fonctions :
//! - fn `connect` — ouvre une connexion PG (`NoTls`) et spawn la tâche pilotant le protocole en arrière-plan.
//! - fn `cancel_on_drop` — attend une requête ; abandonnée avant sa fin (Pass 2 interrompue), demande au serveur de l'annuler.
//! - fn `Guard::drop` — garde de `cancel_on_drop` : envoie la demande d'annulation (`cancel_query`) si la requête n'a pas fini.

use tokio_postgres::{CancelToken, Client, NoTls};

use crate::error::{J2sError, Result};

/// Connect to `PostgreSQL` and return a client.
/// Uses `NoTls` for simplicity; add TLS support later if needed.
pub async fn connect(db_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(db_url, NoTls)
        .await
        .map_err(J2sError::Db)?;

    // Spawn the connection task; it drives the protocol in the background.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {e}");
        }
    });

    Ok(client)
}

/// Awaits `statement`, a query running on `client`. Dropped before it ends (Pass 2 interrupted), it
/// asks the server to cancel the query: dropping the `Client` only closes the connection once the
/// query is over, so the server would run it to the end, holding its locks.
pub async fn cancel_on_drop<T>(client: &Client, statement: impl std::future::Future<Output = T>) -> T {
    struct Guard(Option<CancelToken>);
    impl Drop for Guard {
        fn drop(&mut self) {
            if let (Some(token), Ok(rt)) = (self.0.take(), tokio::runtime::Handle::try_current()) {
                rt.spawn(async move { let _ = token.cancel_query(NoTls).await; });
            }
        }
    }
    let mut guard = Guard(Some(client.cancel_token()));
    let out = statement.await;
    guard.0 = None;
    out
}
