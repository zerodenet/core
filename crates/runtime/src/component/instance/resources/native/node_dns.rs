//! This exact actual HostResources binds the catalog source with its private fence.
use super::*;
use zero_platform_tokio::{contract::dns::DnsService, controlled_carrier::loan::HostNodeDnsSource};
impl HostResources {
    pub(in crate::component) fn bind_node_dns_source(
        &self,
        source: Arc<std::sync::OnceLock<Arc<dyn DnsService>>>,
    ) {
        assert!(
            self.node_dns_source.set(source).is_ok(),
            "one actual Node source cell"
        );
    }
    pub(in crate::component) fn host_node_dns_source(
        &self,
    ) -> Result<HostNodeDnsSource, ChannelError> {
        let source = self
            .node_dns_source
            .get()
            .and_then(|cell| cell.get())
            .ok_or_else(|| self.denied())?;
        Ok(HostNodeDnsSource::for_host(
            source.clone(),
            self.carriers.host_fence()?,
        ))
    }
}
