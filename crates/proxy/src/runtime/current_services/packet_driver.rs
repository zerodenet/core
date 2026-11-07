//! Exact pending/current snapshot selection for an actual registered Packet actor.
use super::*;
use zero_platform_tokio::contract::{
    component::GrantedPacketServiceCaller, ChannelError, Impact, Reason, Stage,
};
impl CurrentServiceSource {
    pub(crate) fn packet_driver_services(
        self: &Arc<Self>,
        grant: &GrantedPacketServiceCaller,
    ) -> Result<TcpRuntimeServices, ChannelError> {
        let denied = || ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
        if grant.caller().is_joined()
            || !grant.caller().scope_identity().resource_open()
            || grant.original().is_retired()
        {
            return Err(denied());
        }
        let protocols = self
            .protocols()
            .map_err(|source| denied().with_source(source))?;
        // A real external Source is not in plan.order. Logical packet lineage
        // does not turn it into an ordinary Slot: first match the actual owner
        // and its exact native/controller record, input and dependency tuple.
        #[cfg(feature = "datagram-route-runtime")]
        let source = self
            .inbounds
            .upgrade()
            .ok_or_else(denied)?
            .packet_driver_candidate(&protocols, grant)?;
        #[cfg(not(feature = "datagram-route-runtime"))]
        let source = None;
        let actual = match source {
            Some(actual) => actual,
            None => protocols
                .runtime_host()
                .components()
                .packet_driver_candidate(grant)?,
        };
        let (config, plan) = actual;
        let host = protocols.runtime_host();
        let pending = host
            .reload_ack()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|pending| Arc::ptr_eq(pending.expected.config(), &config))
            .map(|pending| pending.expected.clone());
        let snapshot = match pending {
            Some(pending) => pending,
            None => {
                let current = self.engine.runtime_snapshot();
                if !Arc::ptr_eq(current.config(), &config) {
                    return Err(denied());
                }
                current
            }
        };
        if grant.binding().network_generation.0 != self.egress_interface.generation() {
            return Err(denied());
        }
        let resolver = self.resolver.upgrade().ok_or_else(denied)?;
        let retired = self.retired_datagrams.upgrade().ok_or_else(denied)?;
        Ok(TcpRuntimeServices::from_prepared(
            self.engine.clone(),
            (snapshot, Ok(plan)),
            resolver,
            protocols,
            self.egress_interface.clone(),
            self.principal_rate_limits.clone(),
            retired,
        )
        .with_current_service_source(Arc::downgrade(self)))
    }
}

#[cfg(feature = "datagram-route-runtime")]
impl CurrentServiceSource {
    pub(in crate::runtime) fn packet_carrier_source(
        &self,
        input: &zero_config::InboundConfig,
        plan: &crate::component::catalog::ComponentPlan,
        native: &zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        memory: &zero_platform_tokio::contract::packet::PacketSourceMemory,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, zero_engine::EngineError>
    {
        self.inbounds
            .upgrade()
            .ok_or_else(|| std::io::Error::other("actual Source registry expired"))?
            .packet_carrier_source(input, plan, native, memory)
    }
}
