//! The same catalog-installed Node service, bound with its actual private fence.
use crate::contract::{dns::DnsService, CancellationToken};
use std::sync::Arc;
/// Read-only Node source for one actual HostResources. Protocol BuildContext
/// has no way to install its own source or obtain the private fence Arc.
#[derive(Clone)]
pub struct HostNodeDnsSource {
    pub(in crate::controlled_carrier) source: Arc<dyn DnsService>,
    pub(in crate::controlled_carrier) host_fence: Arc<CancellationToken>,
}
impl HostNodeDnsSource {
    /// Actual Host construction only. The original private fence is checked
    /// against the Host-issued target issuer before any resolver can be called.
    /// A new Arc around a cloned cancellation token is a foreign identity.
    pub fn for_host(source: Arc<dyn DnsService>, host_fence: &Arc<CancellationToken>) -> Self {
        Self {
            source,
            host_fence: host_fence.clone(),
        }
    }
}
