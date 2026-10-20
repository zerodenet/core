//! DNS uses the neutral native Service dispatcher with its actual DnsQuery
//! purpose. Unsupported plans are identified before opening any carrier.
pub(super) use crate::runtime::tcp_dispatch::service::{connect, prepare};
