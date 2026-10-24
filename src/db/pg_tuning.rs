//! `PostgreSQL` server tuning around Pass 2 (#65): optional temporary `max_wal_size` raise
//! (`--tune-wal`), autovacuum switched off during the load, and advice when the server runs
//! with bulk-load-unfriendly defaults.
//!
//! Chaque retour arrière (`max_wal_size`, autovacuum des tables) est écrit dans un fichier
//! marqueur (`$XDG_STATE_HOME/json2sql/`) **avant** de modifier le serveur, et chaque Pass 2
//! commence par appliquer ceux laissés par un import interrompu (`kill -9`, crash, coupure) :
//! le serveur ne reste jamais modifié durablement.
//!
//! Fonctions :
//! - struct `WalRestore` — commande SQL qui remet `max_wal_size` comme avant l'import, et clé du serveur.
//! - fn `log` — message vers stderr et, si fourni, `ProgressEvent::Pass2Log` (IHM).
//! - fn `wal_restore_sql` — calcule la commande de retour arrière (pure).
//! - fn `marker_path` — chemin du fichier marqueur des retours arrière en attente.
//! - fn `read_markers` — lit les retours arrière en attente (clé serveur → SQL).
//! - fn `write_markers` — écrit les retours arrière en attente (supprime le fichier si vide).
//! - fn `set_marker` — ajoute ou retire une entrée du marqueur, sous verrou de fichier exclusif.
//! - fn `server_key` — identifie le serveur PG (`data_directory:port`).
//! - fn `autovacuum_key` — clé du retour arrière autovacuum (cible de connexion + schéma, sans mot de passe).
//! - fn `record_autovacuum_restore` — enregistre la réactivation de l'autovacuum avant de le couper.
//! - fn `forget_restore` — retire un retour arrière du marqueur une fois appliqué.
//! - fn `apply_restore` — exécute un retour arrière puis recharge la configuration.
//! - fn `restore_pending_wal` — remet `max_wal_size` laissé relevé par un import interrompu.
//! - fn `restore_pending_autovacuum` — réactive l'autovacuum laissé coupé par un import interrompu.
//! - fn `restore_pending` — applique tous les retours arrière laissés par un import interrompu.
//! - fn `current_max_wal_size` — valeur actuelle (MB) et si elle vient de `postgresql.auto.conf`.
//! - fn `raise_max_wal_size` — relève `max_wal_size` (best-effort), renvoie le retour arrière.
//! - fn `record_then_raise` — écrit le marqueur puis relève `max_wal_size`.
//! - fn `restore_max_wal_size` — applique le retour arrière en fin d'import (best-effort).
//! - fn `tuning_hints` — conseils selon `shared_buffers` / `max_wal_size` (pure).
//! - fn `warn_pg_tuning` — lit les réglages du serveur et émet les conseils.
//! - fn `wal_level_hint` — conseil `wal_level = minimal` quand les tables enfants sont chargées en `COPY FREEZE` (pure).
//! - fn `warn_wal_level` — lit `wal_level`, émet ce conseil et dit s'il vaut `minimal`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tokio_postgres::Client;

use crate::io::progress_event::{ProgressEvent, ProgressTx};

/// `max_wal_size` used by `--tune-wal`: fewer checkpoints during the bulk load.
pub const TUNED_MAX_WAL_SIZE_MB: i64 = 16384;

/// Below this, `shared_buffers` is flagged as too small for a bulk import.
const MIN_SHARED_BUFFERS_MB: i64 = 1024;

/// SQL that puts `max_wal_size` back as it was before the import (followed by a config reload),
/// and the server it applies to (key in the marker file).
pub struct WalRestore {
    pub sql: String,
    key: String,
}

pub fn log(progress_tx: Option<&ProgressTx>, msg: String) {
    eprintln!("{msg}");
    if let Some(tx) = progress_tx {
        let _ = tx.send(ProgressEvent::Pass2Log(msg));
    }
}

/// A value set earlier with `ALTER SYSTEM` (in `postgresql.auto.conf`) is put back as is;
/// otherwise `RESET` falls back to `postgresql.conf` / the built-in default.
#[must_use]
pub fn wal_restore_sql(prev_mb: i64, prev_from_auto_conf: bool) -> String {
    if prev_from_auto_conf {
        format!("ALTER SYSTEM SET max_wal_size = '{prev_mb}MB'")
    } else {
        "ALTER SYSTEM RESET max_wal_size".to_string()
    }
}

/// `$XDG_STATE_HOME/json2sql/pg_tuning_restore.json` (default `~/.local/state`): survives reboots,
/// like the `ALTER SYSTEM` it undoes.
fn marker_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local").join("state")))?;
    Some(base.join("json2sql").join("pg_tuning_restore.json"))
}

/// Pending restores, server key → SQL. Missing or unreadable file → none.
fn read_markers(path: &Path) -> HashMap<String, String> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

/// Atomic write (temp file + rename); removes the file when nothing is pending.
fn write_markers(path: &Path, markers: &HashMap<String, String>) -> std::io::Result<()> {
    if markers.is_empty() {
        return match std::fs::remove_file(path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
    }
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(markers).map_err(std::io::Error::other)?)?;
    std::fs::rename(tmp, path)
}

/// Adds (`Some(sql)`) or removes (`None`) the pending restore under `key`. The read-modify-write
/// runs under an exclusive file lock: concurrent imports (or tests) never drop each other's entries.
fn set_marker(key: &str, sql: Option<&str>) -> std::io::Result<()> {
    let path = marker_path().ok_or_else(|| std::io::Error::other("no HOME / XDG_STATE_HOME"))?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let lock = std::fs::File::create(path.with_extension("lock"))?;
    lock.lock()?; // released when `lock` is dropped
    let mut markers = read_markers(&path);
    match sql {
        Some(sql) => { markers.insert(key.to_string(), sql.to_string()); }
        None => { markers.remove(key); }
    }
    write_markers(&path, &markers)
}

/// Identifies the PG cluster. Readable by the roles that may `ALTER SYSTEM` anyway.
async fn server_key(client: &Client) -> Option<String> {
    let row = client
        .query_one("SELECT current_setting('data_directory') || ':' || current_setting('port')", &[])
        .await
        .ok()?;
    Some(row.get(0))
}

/// `autovacuum:<host>:<port>/<db>/<schema>`, from the connection target: computable by any role,
/// and never contains the password.
fn autovacuum_key(pg_url: &str, pg_schema: &str) -> Option<String> {
    use tokio_postgres::config::Host;
    let cfg: tokio_postgres::Config = pg_url.parse().ok()?;
    let host = match cfg.get_hosts().first() {
        Some(Host::Tcp(h)) => h.clone(),
        Some(other) => format!("{other:?}"),
        None => "localhost".to_string(),
    };
    let port = cfg.get_ports().first().copied().unwrap_or(5432);
    let db = cfg.get_dbname().or_else(|| cfg.get_user()).unwrap_or_default();
    Some(format!("autovacuum:{host}:{port}/{db}/{pg_schema}"))
}

/// Records how to re-enable autovacuum on `schemas` before it is switched off, so a killed
/// import is undone by the next one. Returns the marker key, `None` (warned) if it cannot be written.
pub fn record_autovacuum_restore(
    pg_url: &str,
    schemas: &[crate::schema::table_schema::TableSchema],
    pg_schema: &str,
    progress_tx: Option<&ProgressTx>,
) -> Option<String> {
    use crate::db::ddl::quote_ident;
    let key = autovacuum_key(pg_url, pg_schema)?;
    let sql = schemas.iter()
        .map(|s| format!("ALTER TABLE IF EXISTS {}.{} RESET (autovacuum_enabled)", quote_ident(pg_schema), quote_ident(&s.name)))
        .collect::<Vec<_>>()
        .join(";\n");
    match set_marker(&key, Some(&sql)) {
        Ok(()) => Some(key),
        Err(e) => {
            log(progress_tx, format!("WARNING: cannot record the autovacuum restore ({e}) — a killed import would leave it off"));
            None
        }
    }
}

/// Drops `key` from the marker once its restore has been applied.
pub fn forget_restore(key: &str, progress_tx: Option<&ProgressTx>) {
    if let Err(e) = set_marker(key, None) {
        log(progress_tx, format!("WARNING: could not clear the restore marker ({e})"));
    }
}

async fn apply_restore(client: &Client, sql: &str) -> Result<(), tokio_postgres::Error> {
    client.batch_execute(sql).await?;
    client.batch_execute("SELECT pg_reload_conf()").await
}

/// Applies every restore left by an interrupted import on this server / database. Called at the
/// start of every Pass 2, with or without `--tune-wal`.
pub async fn restore_pending(client: &Client, pg_url: &str, progress_tx: Option<&ProgressTx>) {
    restore_pending_wal(client, progress_tx).await;
    restore_pending_autovacuum(client, pg_url, progress_tx).await;
}

/// Re-enables autovacuum on the tables an interrupted import left it off, in any schema of
/// the database `pg_url` points to. `IF EXISTS`: tables dropped since then are skipped.
async fn restore_pending_autovacuum(client: &Client, pg_url: &str, progress_tx: Option<&ProgressTx>) {
    let (Some(path), Some(prefix)) = (marker_path(), autovacuum_key(pg_url, "")) else { return };
    for (key, sql) in read_markers(&path).into_iter().filter(|(k, _)| k.starts_with(&prefix)) {
        match client.batch_execute(&sql).await {
            Ok(()) => {
                log(progress_tx, format!("Autovacuum re-enabled on tables left without it by an interrupted import ({key})"));
                forget_restore(&key, progress_tx);
            }
            Err(e) => log(progress_tx, format!("WARNING: could not re-enable autovacuum left off by an interrupted import ({e})")),
        }
    }
}

/// Puts back `max_wal_size` left raised by an interrupted import on this server, if any.
async fn restore_pending_wal(client: &Client, progress_tx: Option<&ProgressTx>) {
    let Some(path) = marker_path() else { return };
    let markers = read_markers(&path);
    if markers.is_empty() {
        return;
    }
    let Some(key) = server_key(client).await else { return };
    let Some(sql) = markers.get(&key) else { return };
    match apply_restore(client, sql).await {
        Ok(()) => {
            log(progress_tx, format!("--tune-wal: max_wal_size restored ({sql}) — left raised by an interrupted import"));
            if let Err(e) = set_marker(&key, None) {
                log(progress_tx, format!("WARNING: --tune-wal: could not update {} ({e})", path.display()));
            }
        }
        Err(e) => log(progress_tx, format!(
            "WARNING: --tune-wal: could not restore max_wal_size left by an interrupted import ({e}). \
             Run manually: {sql}; SELECT pg_reload_conf();"
        )),
    }
}

/// Current `max_wal_size` in MB, and whether it was set with `ALTER SYSTEM` (`postgresql.auto.conf`).
async fn current_max_wal_size(client: &Client) -> Result<(i64, bool), tokio_postgres::Error> {
    let row = client
        .query_one("SELECT setting::bigint, coalesce(sourcefile, '') FROM pg_settings WHERE name = 'max_wal_size'", &[])
        .await?;
    Ok((row.get(0), row.get::<_, String>(1).ends_with("postgresql.auto.conf")))
}

/// Raises `max_wal_size` to `TUNED_MAX_WAL_SIZE_MB` server-wide. Best-effort: returns `None`
/// (with a warning) when the role lacks the privilege, when the restore cannot be recorded first,
/// or when the value is already high enough.
pub async fn raise_max_wal_size(client: &Client, progress_tx: Option<&ProgressTx>) -> Option<WalRestore> {
    // An earlier interrupted import may have left 16 GB in place: put the real value back first.
    restore_pending_wal(client, progress_tx).await;
    let (prev_mb, from_auto_conf) = match current_max_wal_size(client).await {
        Ok(v) => v,
        Err(e) => {
            log(progress_tx, format!("WARNING: --tune-wal: cannot read max_wal_size ({e}) — continuing without it"));
            return None;
        }
    };
    if prev_mb >= TUNED_MAX_WAL_SIZE_MB {
        return None;
    }
    let Some(key) = server_key(client).await else {
        log(progress_tx, "WARNING: --tune-wal: cannot identify the server (data_directory) — continuing without it".to_string());
        return None;
    };
    let restore = WalRestore { sql: wal_restore_sql(prev_mb, from_auto_conf), key };
    if !record_then_raise(client, &restore, progress_tx).await {
        return None;
    }
    log(progress_tx, format!(
        "--tune-wal: max_wal_size {prev_mb}MB → {TUNED_MAX_WAL_SIZE_MB}MB for this import. \
         Restored at the end, or by the next import if this one is killed. Manual undo: {}; SELECT pg_reload_conf();",
        restore.sql
    ));
    Some(restore)
}

/// Records `restore` in the marker, then raises `max_wal_size`. Recording first means even a
/// kill -9 right after leaves a way back for the next run. Returns `false` (warned) on failure.
async fn record_then_raise(client: &Client, restore: &WalRestore, progress_tx: Option<&ProgressTx>) -> bool {
    if let Err(e) = set_marker(&restore.key, Some(&restore.sql)) {
        log(progress_tx, format!("WARNING: --tune-wal: cannot record the restore command ({e}) — continuing without it"));
        return false;
    }
    // Two round-trips: ALTER SYSTEM refuses to run inside a multi-statement (implicit) transaction.
    let set = format!("ALTER SYSTEM SET max_wal_size = '{TUNED_MAX_WAL_SIZE_MB}MB'");
    if let Err(e) = client.batch_execute(&set).await {
        let _ = set_marker(&restore.key, None);
        log(progress_tx, format!(
            "WARNING: --tune-wal: cannot raise max_wal_size ({e}) — needs a superuser role or \
             GRANT ALTER SYSTEM ON PARAMETER max_wal_size; continuing without it"
        ));
        return false;
    }
    if let Err(e) = client.batch_execute("SELECT pg_reload_conf()").await {
        log(progress_tx, format!("WARNING: --tune-wal: pg_reload_conf failed ({e})"));
    }
    true
}

/// Applies `restore` then reloads the config. Best-effort: on failure the marker is kept, so the
/// next import retries, and the manual command is logged.
pub async fn restore_max_wal_size(client: &Client, restore: &WalRestore, progress_tx: Option<&ProgressTx>) {
    match apply_restore(client, &restore.sql).await {
        Ok(()) => {
            log(progress_tx, "--tune-wal: max_wal_size restored".to_string());
            if let Err(e) = set_marker(&restore.key, None) {
                log(progress_tx, format!("WARNING: --tune-wal: could not clear the restore marker ({e})"));
            }
        }
        Err(e) => log(progress_tx, format!(
            "WARNING: --tune-wal: could not restore max_wal_size ({e}); the next import retries. \
             Run manually: {}; SELECT pg_reload_conf();",
            restore.sql
        )),
    }
}

/// Advice for a server running with defaults that slow down a bulk import.
#[must_use]
pub fn tuning_hints(shared_buffers_mb: i64, max_wal_size_mb: i64, tune_wal: bool) -> Vec<String> {
    let mut hints = Vec::new();
    if shared_buffers_mb < MIN_SHARED_BUFFERS_MB {
        hints.push(format!(
            "PG tuning: shared_buffers = {shared_buffers_mb}MB is small for a bulk import — consider \
             ALTER SYSTEM SET shared_buffers = '3GB' (about 1/4 of RAM), then restart PostgreSQL."
        ));
    }
    if !tune_wal && max_wal_size_mb < TUNED_MAX_WAL_SIZE_MB {
        hints.push(format!(
            "PG tuning: max_wal_size = {max_wal_size_mb}MB triggers frequent checkpoints during the load — \
             consider --tune-wal (raised for this import only)."
        ));
    }
    hints
}

/// Reads `shared_buffers` / `max_wal_size` and logs `tuning_hints`. Silent if the query fails.
pub async fn warn_pg_tuning(client: &Client, tune_wal: bool, progress_tx: Option<&ProgressTx>) {
    let sql = "SELECT pg_size_bytes(current_setting('shared_buffers')) / 1048576, \
               pg_size_bytes(current_setting('max_wal_size')) / 1048576";
    let Ok(row) = client.query_one(sql, &[]).await else { return };
    for hint in tuning_hints(row.get(0), row.get(1), tune_wal) {
        log(progress_tx, hint);
    }
}

/// Advice for a `COPY FREEZE` load (#75): under `wal_level = minimal`, a table truncated in the load
/// transaction is written without WAL; any other level writes every row twice.
#[must_use]
pub fn wal_level_hint(wal_level: &str) -> Option<String> {
    (wal_level != "minimal").then(|| format!(
        "PG tuning: wal_level = {wal_level} writes the imported tables twice (table + WAL) and loads the root \
         without COPY FREEZE — on a local \
         analysis server, consider ALTER SYSTEM SET wal_level = 'minimal' and ALTER SYSTEM SET \
         max_wal_senders = 0 (archive_mode must stay off), then restart PostgreSQL. This disables \
         replication and point-in-time recovery."
    ))
}

/// Reads `wal_level`, logs `wal_level_hint` and returns whether it is `minimal`. Silent (and
/// `false`) if the query fails.
pub async fn warn_wal_level(client: &Client, progress_tx: Option<&ProgressTx>) -> bool {
    let Ok(row) = client.query_one("SELECT current_setting('wal_level')", &[]).await else { return false };
    let level: &str = row.get(0);
    if let Some(hint) = wal_level_hint(level) {
        log(progress_tx, hint);
    }
    level == "minimal"
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    #[test]
    fn restore_puts_back_an_alter_system_value() {
        assert_eq!(wal_restore_sql(2048, true), "ALTER SYSTEM SET max_wal_size = '2048MB'");
    }

    #[test]
    fn restore_resets_when_value_came_from_postgresql_conf() {
        assert_eq!(wal_restore_sql(1024, false), "ALTER SYSTEM RESET max_wal_size");
    }

    #[test]
    fn markers_round_trip_and_file_removed_when_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("sub").join("m.json");
        assert!(read_markers(&path).is_empty(), "missing file → nothing pending");
        let mut m = HashMap::new();
        m.insert("/data:5432".to_string(), "ALTER SYSTEM RESET max_wal_size".to_string());
        write_markers(&path, &m).unwrap();
        assert_eq!(read_markers(&path), m);
        write_markers(&path, &HashMap::new()).unwrap();
        assert!(!path.exists(), "empty marker set removes the file");
        write_markers(&path, &HashMap::new()).unwrap(); // already absent: still Ok
    }

    #[test]
    fn autovacuum_key_has_target_and_schema_but_no_password() {
        let key = autovacuum_key("postgres://bob:secret@db.local:5433/lab", "public").unwrap();
        assert_eq!(key, "autovacuum:db.local:5433/lab/public");
        let sock = autovacuum_key("host=/var/run/postgresql user=dylan dbname=lab", "s").unwrap();
        assert!(sock.contains("/var/run/postgresql") && sock.ends_with(":5432/lab/s"), "{sock}");
        assert!(autovacuum_key("host=h dbname=lab", "x").unwrap().starts_with(&autovacuum_key("host=h dbname=lab", "").unwrap()));
    }

    #[test]
    fn hints_flag_small_shared_buffers_and_wal_without_tune_wal() {
        let hints = tuning_hints(128, 1024, false);
        assert_eq!(hints.len(), 2);
        assert!(hints[0].contains("shared_buffers = 128MB"));
        assert!(hints[1].contains("--tune-wal"));
    }

    #[test]
    fn hints_skip_wal_advice_with_tune_wal_and_stay_quiet_when_tuned() {
        assert_eq!(tuning_hints(128, 1024, true).len(), 1);
        assert_eq!(tuning_hints(4096, 16384, false), Vec::<String>::new());
    }

    #[test]
    fn wal_level_hint_only_when_not_minimal() {
        let hint = wal_level_hint("replica").unwrap();
        assert!(hint.contains("wal_level = replica") && hint.contains("max_wal_senders = 0"), "{hint}");
        assert_eq!(wal_level_hint("minimal"), None);
    }
}
