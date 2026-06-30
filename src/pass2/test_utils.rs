use crate::error::Result;
use crate::pass2::sink::RowSink;

pub(crate) struct CaptureSink(pub Vec<Vec<u8>>);

impl RowSink for CaptureSink {
    fn write_row(&mut self, row: &[u8]) -> Result<()> {
        self.0.push(row.to_vec());
        Ok(())
    }
}
