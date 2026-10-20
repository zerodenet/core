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

    pub(super) fn runtime_services(&self) -> io::Result<TcpRuntimeServices> {
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
        ))
    }
}
fn unavailable(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, message)
}

#[cfg(test)]
mod tests;
