use std::sync::{Arc, Mutex};

use crate::db::copy_sink::TempFileSink;
use crate::error::Result;

/// Abstraction over row writing, decoupling insert logic from the filesystem.
/// Implement this trait with an in-memory buffer for unit tests.
pub trait RowSink {
    fn write_row(&mut self, row: Vec<u8>) -> Result<()>;
}

impl RowSink for TempFileSink {
    fn write_row(&mut self, row: Vec<u8>) -> Result<()> {
        TempFileSink::write_row(self, row)
    }
}

/// Shared sink: multiple workers write to the same TempFileSink under a Mutex.
/// Lock is held only for the duration of write_row (nanoseconds unless a spill
/// triggers disk I/O, which is bounded and infrequent at 4 MiB intervals).
impl RowSink for Arc<Mutex<TempFileSink>> {
    fn write_row(&mut self, row: Vec<u8>) -> Result<()> {
        self.lock().unwrap().write_row(row)
    }
}
