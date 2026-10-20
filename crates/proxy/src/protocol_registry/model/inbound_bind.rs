//! Temporary projection of canonical declarations onto the legacy operation.
//! Remove this bridge when its caller uses the typed inbound factory directly.
use std::{net::SocketAddr, sync::Arc};
use zero_engine::EngineError;
use zero_platform_tokio::contract::component::ResourceDeclaration;

use crate::runtime::inbound_operation::PreparedInboundListenerOperation;

pub(crate) struct PreparedInboundBind {
    operation: Box<dyn PreparedInboundListenerOperation>,
    resources: Arc<[ResourceDeclaration]>,
    address: SocketAddr,
}
impl PreparedInboundBind {
    /// Cold and literal-only. Selection errors must not retry the old binder.
    pub(crate) fn new(
        operation: Box<dyn PreparedInboundListenerOperation>,
        resources: Arc<[ResourceDeclaration]>,
    ) -> Result<Self, EngineError> {
        let [ResourceDeclaration::TcpListener { destination, .. }] = resources.as_ref() else {
            return Err(unsupported());
        };
        let ip = match destination.address {
            zero_core::Address::Ipv4(ip) => std::net::IpAddr::V4(ip.into()),
            zero_core::Address::Ipv6(ip) => std::net::IpAddr::V6(ip.into()),
            zero_core::Address::Domain(_) => return Err(unsupported()),
        };
        Ok(Self {
            address: SocketAddr::new(ip, destination.port),
            operation,
            resources,
        })
    }
    pub(crate) fn resources(&self) -> &[ResourceDeclaration] {
        &self.resources
    }
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
    pub(crate) fn internal_tasks(&self) -> usize {
        self.operation.internal_tasks()
    }
    pub(crate) fn into_operation(self) -> Box<dyn PreparedInboundListenerOperation> {
        self.operation
    }
}
fn unsupported() -> EngineError {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "host listener lease currently requires one declared literal TCP listener",
    )
    .into()
}
