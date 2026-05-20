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
