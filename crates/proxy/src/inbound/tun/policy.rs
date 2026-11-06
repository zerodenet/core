//! Concrete projection of the existing System TUN ingress behavior.
use std::{
    net::IpAddr,
    sync::{atomic::AtomicU64, Arc},
};
#[derive(Clone)]
pub(crate) struct TunIngressOptions {
    pub(crate) addresses: Vec<(IpAddr, IpAddr)>,
    pub(crate) mtu: u16,
    pub(crate) dns_hijack: bool,
    pub(crate) dns_hijacked_queries: Arc<AtomicU64>,
}
