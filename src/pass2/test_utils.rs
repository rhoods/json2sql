//! Sink `RowSink` de test — capture les lignes écrites en mémoire pour assertions,
//! sans passer par `MemSink`/PostgreSQL.
//!
//! Fonctions :
//! - `CaptureSink::write_row` (`impl RowSink`) — pousse la ligne dans le `Vec` interne.

use crate::error::Result;
use crate::pass2::sink::RowSink;

pub(crate) struct CaptureSink(pub Vec<Vec<u8>>);

impl RowSink for CaptureSink {
    fn write_row(&mut self, row: &[u8]) -> Result<()> {
        self.0.push(row.to_vec());
        Ok(())
    }
}
