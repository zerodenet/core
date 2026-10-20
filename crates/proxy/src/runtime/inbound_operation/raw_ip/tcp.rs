use std::{io, sync::Arc};

use crate::runtime::{
    inbound_owners::{InboundPeerTasks, InboundTaskAccess},
    route_runtime::InboundRouteRuntimeFactory,
    tcp_ingress::NoClientResponseStreamProtocol,
};
use crate::transport::ReplayStream;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;
use zero_stack::UserTcpStack;
use zero_traits::{IpAddress, SocketAddress, TcpStack};

pub(super) const MAX_CONNECTIONS: usize = 4096;

pub(super) async fn accept(
    tcp: Arc<UserTcpStack>,
    runtime_factory: InboundRouteRuntimeFactory,
    group: Arc<InboundPeerTasks>,
    host: InboundTaskAccess,
    native: super::native_obligations::NativeObligations,
) -> Result<(), EngineError> {
    loop {
        let Some((stream, source, destination)) = tcp.accept().await else {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "raw-IP TCP stack closed",
            )));
        };
        if group.occupied() >= MAX_CONNECTIONS {
            continue;
        }
        let mut native = native
            .reserve(super::native_obligations::ChildKind::Tcp)
            .map_err(std::io::Error::other)?;
        let source_addr = zero_platform_tokio::socket_address_to_socket_addr(source);
        // The existing Engine.prepare_session in serve mints the real identity
        // before dispatch; this preserves its full original admission/auth chain.
        let mut session = Session::new(
            0,
            socket_address_to_address(destination),
            destination.port,
            Network::Tcp,
            ProtocolType::UNKNOWN,
        );
        session.inbound_peer_identity = stream.peer_identity();
        session.transparent_target = true;
        let runtime = runtime_factory.for_connection(Some(source_addr));
        group.spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            native.bind_caller(binding).map_err(std::io::Error::other)?;
            let runtime = runtime.with_caller_native(native.registration());
            let stream = ReplayStream::new(stream, Vec::new());
            native.enter().map_err(std::io::Error::other)?;
            let result = runtime
                .serve(session, stream, &NoClientResponseStreamProtocol::new())
                .await;
            native.consume_business_result(result)
        })?;
    }
}

fn socket_address_to_address(address: SocketAddress) -> Address {
    match address.ip {
        IpAddress::V4(ip) => Address::Ipv4(ip),
        IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}
