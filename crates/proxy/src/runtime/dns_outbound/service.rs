//! DNS uses the neutral native Service dispatcher with its actual DnsQuery
//! purpose. Unsupported plans are identified before opening any carrier.
pub(super) use crate::runtime::tcp_dispatch::service::{connect, prepare};
mod binding;
pub(super) use binding::NativeDnsBinding;

mod error;
pub(super) use error::DnsDetourFailure;
