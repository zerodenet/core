//! Host-lifetime authority for short service calls. Retained connectors only
//! receive a Weak handle; this owner never points back to Proxy or its pools.
use super::principal_rate_limit::PrincipalRateLimitRegistry;
use crate::{
    component::datagram_retirement::RetiredDatagrams,
    inventory::{ProtocolInventory, WeakProtocolInventory},
    protocol_registry::TcpRuntimeServices,
};
use std::{
    io,
    sync::{Arc, Weak},
};
use zero_dns::DnsSystem;
use zero_engine::Engine;
use zero_platform_tokio::EgressInterfaceControl;

pub(crate) struct CurrentServiceSource {
    #[cfg(feature = "tls-ech-runtime")]
    pub(crate) ech_owner:
        Arc<std::sync::OnceLock<Arc<dyn zero_transport::tls::ech::EchConfigResolver>>>,
    engine: Engine,
    resolver: Weak<DnsSystem>,
    protocols: WeakProtocolInventory,
    egress_interface: EgressInterfaceControl,
    principal_rate_limits: PrincipalRateLimitRegistry,
    retired_datagrams: Weak<RetiredDatagrams>,
}
impl std::fmt::Debug for CurrentServiceSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurrentServiceSource")
            .finish_non_exhaustive()
    }
}
impl CurrentServiceSource {
    pub(super) fn new(
        engine: Engine,
        resolver: &Arc<DnsSystem>,
        protocols: &ProtocolInventory,
        egress_interface: EgressInterfaceControl,
        principal_rate_limits: PrincipalRateLimitRegistry,
        retired_datagrams: &Arc<RetiredDatagrams>,
    ) -> Self {
        Self {
            #[cfg(feature = "tls-ech-runtime")]
            ech_owner: Arc::new(std::sync::OnceLock::new()),
            engine,
            resolver: Arc::downgrade(resolver),
            protocols: protocols.downgrade(),
            egress_interface,
            principal_rate_limits,
            retired_datagrams: Arc::downgrade(retired_datagrams),
        }
    }
    /// Only borrow after upgrading the host's Weak source for this operation.
    /// Retained providers must not copy Engine or its snapshot into their state.
    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    pub(super) fn protocols(&self) -> io::Result<ProtocolInventory> {
        self.protocols
            .upgrade()
            .ok_or_else(|| unavailable("proxy runtime is shutting down"))
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(super) fn egress_generation(&self) -> u64 {
        self.egress_interface.generation()
    }

    /// Observe retained cleanup custody without preparing the Engine's pending
    /// configuration or admitting a caller against a historical role.
    pub(crate) fn validate_owned_carrier_adoption(
        &self,
        role: zero_core::component::RoleInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> io::Result<()> {
        self.protocols()?
            .runtime_host()
            .components()
            .validate_owned_carrier_adoption(role, adoption)
            .map_err(io::Error::other)
    }
    pub(crate) fn validate_owned_carrier_instance(
        &self,
        consumer: zero_core::component::ComponentInstanceId,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> io::Result<()> {
        self.protocols()?
            .runtime_host()
            .components()
            .validate_owned_carrier_instance(consumer, adoption)
            .map_err(io::Error::other)
    }
    pub(crate) fn runtime_services(self: &Arc<Self>) -> io::Result<TcpRuntimeServices> {
        let resolver = self
            .resolver
            .upgrade()
            .ok_or_else(|| unavailable("DNS runtime is shutting down"))?;
        let protocols = self.protocols()?;
        let retired_datagrams = self
            .retired_datagrams
            .upgrade()
            .ok_or_else(|| unavailable("datagram owner is shutting down"))?;
        Ok(TcpRuntimeServices::new(
            self.engine().clone(),
            self.engine().runtime_snapshot(),
            resolver,
            protocols,
            self.egress_interface.clone(),
            self.principal_rate_limits.clone(),
            retired_datagrams,
        )
        .with_current_service_source(Arc::downgrade(self)))
    }
}
fn unavailable(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, message)
}

#[cfg(test)]
mod tests;
