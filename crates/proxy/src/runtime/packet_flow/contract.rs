use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use zero_engine::EngineError;
use zero_stack::{ClientTcpStackError, ClientUdpSocket, ClientUdpStackError, UserTcpStream};
use zero_traits::IoObserver;

/// Prepared shared IP stack with validated local addresses and MTU. The owner
/// feeds correlated replies and retains bounded queues, fragmentation and ICMP
/// handling. Closing one Flow never closes this endpoint. Resource revocation
/// closes its I/O owner; existing flows may drain on a retired generation.
#[async_trait::async_trait]
pub(crate) trait PacketFlowEndpoint: Send + Sync {
    /// Admission fence, including resource identity and egress generation.
    fn is_current(&self) -> bool;
    /// Terminal I/O state, distinct from retirement of new-flow admission.
    fn is_closed(&self) -> bool;
    /// Only installed strict endpoint owners expose a real transferred-user ledger.
    fn flow_users(&self) -> Option<Arc<super::FlowUsers>> {
        None
    }
    async fn open_tcp_tracked(
        &self,
        local_ip: IpAddr,
        target: SocketAddr,
        observer: Option<Arc<dyn IoObserver>>,
        user: Option<&super::FlowUser>,
    ) -> Result<UserTcpStream, ClientTcpStackError> {
        if user.is_some() {
            return Err(ClientTcpStackError::OutboundClosed);
        }
        self.open_tcp(local_ip, target, observer).await
    }
    /// The original socket worker belongs to the same persistent endpoint
    /// task owner. Implementations may not detach it or borrow another Scope.
    fn spawn_packet_worker(
        &self,
        worker: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), EngineError>> + Send + 'static>,
        >,
    ) -> Result<(), EngineError>;
    fn bind_udp(
        &self,
        local_ip: IpAddr,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<ClientUdpSocket, ClientUdpStackError>;
    async fn open_tcp(
        &self,
        local_ip: IpAddr,
        target: SocketAddr,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<UserTcpStream, ClientTcpStackError>;
}

pub(crate) struct PacketFlowBinding {
    pub(crate) local_ip: IpAddr,
    pub(crate) peer_id: Option<Arc<str>>,
    pub(crate) endpoint: Arc<dyn PacketFlowEndpoint>,
}

/// Protocol/adapter-owned destination selection. The runtime does not parse
/// keys, allowed ranges or opaque resource identities. Implementations may
/// reject unsupported address families or destinations without opening I/O.
pub(crate) enum PacketFlowBindError {
    /// Try another resolved address within this already selected outbound.
    UnsupportedTarget(EngineError),
    /// Admission/runtime failure is terminal; never switch resource implicitly.
    Failed(EngineError),
}

pub(crate) trait PacketFlowSource: Send + Sync {
    fn bind(
        &self,
        target: IpAddr,
        egress_generation: u64,
    ) -> Result<PacketFlowBinding, PacketFlowBindError>;
}

#[derive(Clone)]
pub(crate) struct PacketFlowAdapter {
    pub(crate) tag: Arc<str>,
    source: Arc<dyn PacketFlowSource>,
}

impl PacketFlowAdapter {
    pub(crate) fn new(tag: Arc<str>, source: Arc<dyn PacketFlowSource>) -> Self {
        Self { tag, source }
    }

    pub(crate) fn select(
        &self,
        targets: &[SocketAddr],
        generation: u64,
    ) -> Result<(SocketAddr, PacketFlowBinding), EngineError> {
        let mut last_error = None;
        for target in targets {
            match self.source.bind(target.ip(), generation) {
                Ok(binding) => {
                    if binding.local_ip.is_ipv4() != target.is_ipv4() {
                        last_error = Some(invalid("Packet Flow address family mismatch"));
                        continue;
                    }
                    if !binding.endpoint.is_current() {
                        return Err(invalid("Packet Flow endpoint changed during admission"));
                    }
                    return Ok((*target, binding));
                }
                Err(PacketFlowBindError::UnsupportedTarget(error)) => last_error = Some(error),
                Err(PacketFlowBindError::Failed(error)) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| invalid("no target IP")))
    }

    pub(crate) fn prepare_tcp(
        &self,
    ) -> Box<dyn crate::runtime::tcp_dispatch::operation::PreparedTcpConnectOperation> {
        Box::new(super::tcp::PacketTcpOperation(self.clone()))
    }

    pub(crate) fn prepare_udp(
        &self,
    ) -> Box<dyn crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation> {
        Box::new(super::udp::PacketUdpOperation(self.clone()))
    }
}

pub(super) fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.to_string(),
    ))
}
