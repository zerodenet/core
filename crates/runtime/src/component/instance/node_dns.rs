//! Same catalog cell across cold assembly, actual build and source observation.
use super::*;
use zero_platform_tokio::{contract::dns::DnsService, controlled_carrier::loan::HostNodeDnsSource};
impl ComponentSlot {
    pub(in crate::component) fn bind_node_dns_source(
        &self,
        source: Arc<OnceLock<Arc<dyn DnsService>>>,
    ) {
        if let Err(source) = self.node_dns_source.set(source) {
            assert!(
                Arc::ptr_eq(
                    self.node_dns_source.get().expect("actual Node source cell"),
                    &source
                ),
                "actual reused Slot cannot change its Host Node source"
            );
        }
    }
    pub(in crate::component) fn host_node_dns_source(
        &self,
    ) -> Result<HostNodeDnsSource, ChannelError> {
        self.check_dependency_loan_start()?;
        self.live
            .get()
            .and_then(|live| live.as_ref().ok())
            .ok_or_else(|| self.failure(Reason::StalePlan, "actual Node consumer is not built"))?
            .resources
            .host_node_dns_source()
    }
}
