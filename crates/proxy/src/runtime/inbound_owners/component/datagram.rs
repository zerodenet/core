//! The original UDP owner is installed before start; it binds the actual TCP endpoint.
use super::*;
use std::sync::OnceLock;
use zero_platform_tokio::{
    contract::component::DatagramPeerLimits,
    controlled_datagram::listener::DatagramListenerIdentity,
};
pub(in crate::runtime) struct DatagramResource {
    address: std::net::SocketAddr,
    declared: Destination,
    native: Mutex<crate::component::instance::HostDatagramListenerOwner>,
    pub identity: Arc<OnceLock<DatagramListenerIdentity>>,
    pub actual: OnceLock<std::net::SocketAddr>,
}
impl DatagramResource {
    pub(super) fn new(
        component: zero_core::component::ComponentId,
        declaration: &ResourceDeclaration,
        actual: std::net::SocketAddr,
    ) -> Result<Self, EngineError> {
        let ResourceDeclaration::DatagramListener {
            destination,
            max_bind_attempts,
            ..
        } = declaration
        else {
            return Err(std::io::Error::other(denied()).into());
        };
        let mut concrete = declaration.clone();
        if let ResourceDeclaration::DatagramListener { destination, .. } = &mut concrete {
            *destination = Destination::new(
                match actual.ip() {
                    std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                    std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                },
                actual.port(),
            );
        }
        Ok(Self {
            address: actual,
            declared: destination.clone(),
            native: Mutex::new(
                crate::component::instance::HostDatagramListenerOwner::new(
                    component,
                    &concrete,
                    *max_bind_attempts,
                )
                .map_err(std::io::Error::other)?,
            ),
            identity: Arc::default(),
            actual: OnceLock::new(),
        })
    }
    pub(super) fn bind_original_budgets(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        roots: Arc<zero_runtime::HostOriginalBudgets>,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
        stop: CancellationToken,
    ) -> Result<(), EngineError> {
        self.native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bind_original_budgets_for_instance(instance, roots, tasks.downgrade(), stop)
            .map_err(|source| std::io::Error::other(source).into())
    }
    pub(super) fn open(
        &self,
        destination: Destination,
    ) -> Result<GrantedDatagramListener, ChannelError> {
        if destination != self.declared {
            return Err(denied());
        }
        let io = self
            .native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bind_listener(self.address)?;
        self.actual
            .set(
                io.rx
                    .local_addr()
                    .map_err(|source| denied().with_source(source))?,
            )
            .map_err(|_| denied())?;
        self.identity.set(io.tx.identity()).map_err(|_| denied())?;
        Ok(io)
    }
    pub(super) fn revoke(&self) {
        self.native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .revoke();
    }
    pub(super) fn close(&self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.native
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .shutdown(deadline)
    }
}
#[derive(Clone)]
pub(in crate::runtime) struct PeerSourceContext {
    pub carrier: Option<Arc<OnceLock<std::net::SocketAddr>>>,
    pub identity: Arc<OnceLock<DatagramListenerIdentity>>,
    pub limits: DatagramPeerLimits,
    pub peers: Arc<super::super::InboundPeerTasks>,
}
#[cfg(test)]
mod tests;
