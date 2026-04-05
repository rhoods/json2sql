use tokio_postgres::{Client, NoTls};

use crate::error::{J2sError, Result};

/// Connect to PostgreSQL and return a client.
/// Uses NoTls for simplicity; add TLS support later if needed.
pub async fn connect(db_url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(db_url, NoTls)
        .await
        .map_err(J2sError::Db)?;

    // Spawn the connection task; it drives the protocol in the background.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });

    Ok(client)
}
