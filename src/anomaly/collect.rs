use tokio::sync::mpsc::UnboundedSender;

use crate::error::{J2sError, Result};

/// One anomaly event sent from a worker to the central writer task.
#[derive(Debug)]
pub enum AnomalyEvent {
    Record {
        table: String,
        column: String,
        row_id: String,
        expected_type: String,
        actual_value: String,
        actual_type: String,
    },
    IncTotal {
        table: String,
    },
}

/// Abstraction over anomaly collection — either in-process (`AnomalyCollector`)
/// or cross-task via channel (`AnomalyProxy`).
pub trait AnomalyCollect {
    fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()>;

    fn inc_total(&mut self, table: &str);
}

impl AnomalyCollect for super::collector::AnomalyCollector {
    fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()> {
        self.record(table, column, row_id, expected_type, actual_value, actual_type)
    }

    fn inc_total(&mut self, table: &str) {
        self.inc_total(table);
    }
}

/// Sends anomaly events to a writer task via an unbounded channel.
/// Used by parallel workers — no blocking, no file I/O in worker threads.
pub struct AnomalyProxy {
    tx: UnboundedSender<AnomalyEvent>,
}

impl AnomalyProxy {
    #[must_use]
    pub fn new(tx: UnboundedSender<AnomalyEvent>) -> Self {
        Self { tx }
    }
}

impl AnomalyCollect for AnomalyProxy {
    fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()> {
        self.tx
            .send(AnomalyEvent::Record {
                table: table.to_string(),
                column: column.to_string(),
                row_id: row_id.to_string(),
                expected_type: expected_type.to_string(),
                actual_value: actual_value.to_string(),
                actual_type: actual_type.to_string(),
            })
            .map_err(|e| J2sError::AnomalyReport(e.to_string()))
    }

    fn inc_total(&mut self, table: &str) {
        let _ = self.tx.send(AnomalyEvent::IncTotal {
            table: table.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anomaly::collector::AnomalyCollector;

    #[test]
    fn collector_implements_anomaly_collect_record() {
        let mut c = AnomalyCollector::new(None);
        let result = AnomalyCollect::record(
            &mut c, "products", "price", "row1", "double precision", "gratuit", "string",
        );
        assert!(result.is_ok());
        assert_eq!(c.total_anomalies(), 1);
    }

    #[test]
    fn collector_implements_anomaly_collect_inc_total() {
        let mut c = AnomalyCollector::new(None);
        AnomalyCollect::inc_total(&mut c, "products");
        AnomalyCollect::inc_total(&mut c, "products");
        // Rate denominator: 2 rows, 0 anomalies → rate = 0
        assert!((c.overall_anomaly_rate() - 0.0).abs() < 1e-9);
    }

    #[test]
    fn proxy_sends_record_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut proxy = AnomalyProxy::new(tx);
        proxy
            .record("orders", "qty", "r1", "integer", "bad", "string")
            .unwrap();
        let event = rx.try_recv().expect("event must be in channel");
        match event {
            AnomalyEvent::Record { table, column, row_id, expected_type, actual_value, actual_type } => {
                assert_eq!(table, "orders");
                assert_eq!(column, "qty");
                assert_eq!(row_id, "r1");
                assert_eq!(expected_type, "integer");
                assert_eq!(actual_value, "bad");
                assert_eq!(actual_type, "string");
            }
            _ => panic!("expected AnomalyEvent::Record"),
        }
    }

    #[test]
    fn proxy_sends_inc_total_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut proxy = AnomalyProxy::new(tx);
        proxy.inc_total("users");
        let event = rx.try_recv().expect("event must be in channel");
        match event {
            AnomalyEvent::IncTotal { table } => assert_eq!(table, "users"),
            _ => panic!("expected AnomalyEvent::IncTotal"),
        }
    }

    #[test]
    fn proxy_record_errors_when_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let mut proxy = AnomalyProxy::new(tx);
        let result = proxy.record("t", "c", "r", "int4", "bad", "string");
        assert!(result.is_err(), "send on closed channel must return Err");
    }
}
