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
        let source = slot.host_node_dns_source()?;
        match self.dns_wire.get() {
            Some(issuer) => source.with_wire_issuer(issuer.clone()),
            None => Ok(source),
        }
    }
}

impl ComponentPlan {
    /// Read only the issuer already bound by the unique accepted target
    /// snapshot. No issuer is installed in reused Slots or old caller contexts.
    pub fn dns_wire_candidate(
        &self,
    ) -> Option<zero_traits::packet_metadata::PacketMetadataCandidateIssuer> {
        self.dns_wire.get().cloned()
    }
    /// The Proxy supplies only its exact accepted snapshot's metadata issuer,
    /// before starting this already selected target graph. No Slot/old plan is
    /// changed and this grants no resource, execution or close authority.
    pub fn bind_dns_wire_candidate(
        &self,
        issuer: zero_traits::packet_metadata::PacketMetadataCandidateIssuer,
    ) -> Result<(), ChannelError> {
        if let Err(issuer) = self.dns_wire.set(issuer) {
            if !self
                .dns_wire
                .get()
                .is_some_and(|same| same.same_candidate(&issuer))
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "component plan already names another DNS wire attempt",
                ));
            }
        }
        Ok(())
    }
}
