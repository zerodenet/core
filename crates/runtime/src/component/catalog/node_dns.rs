//! Install once in the actual Host; ordinary protocols never set this source.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{contract::dns::DnsService, controlled_carrier::loan::HostNodeDnsSource};
impl ComponentCatalog {
    pub fn install_node_dns_source(&self, source: Arc<dyn DnsService>) -> Result<(), ChannelError> {
        if let Err(source) = self.node_dns_source.set(source) {
            if !Arc::ptr_eq(
                self.node_dns_source.get().expect("installed Node source"),
                &source,
            ) {
                return Err(invalid(
                    Reason::StalePlan,
                    "catalog already owns another Node source",
                ));
            }
        }
        Ok(())
    }
}
impl ComponentPlan {
    /// Actual started Slot only. This observation grants no native/cached right.
    pub fn host_node_dns_source(
        &self,
        actual: ComponentInstanceId,
    ) -> Result<HostNodeDnsSource, ChannelError> {
        let slot = self
            .order
            .iter()
            .find(|slot| slot.instance_id() == actual)
            .ok_or_else(|| invalid(Reason::StalePlan, "actual Node consumer is absent"))?;
        slot.host_node_dns_source()
    }
}
