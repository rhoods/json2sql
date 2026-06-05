//! Anomaly detection and reporting: type coercion failures and dropped keys from Pass 2.
//!
//! [`collector`] gathers events from workers; [`reporter`] serializes the summary.
pub mod collector;
pub mod reporter;
