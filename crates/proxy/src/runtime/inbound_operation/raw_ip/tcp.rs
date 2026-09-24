use std::{io, sync::Arc};

use tokio::task::JoinSet;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;
use zero_stack::UserTcpStack;
use zero_traits::{IpAddress, SocketAddress, TcpStack};

use crate::runtime::{
    route_runtime::InboundRouteRuntimeFactory, tcp_ingress::NoClientResponseStreamProtocol,
};
use crate::transport::ReplayStream;

pub(super) async fn accept(
    tcp: Arc<UserTcpStack>,
    runtime_factory: InboundRouteRuntimeFactory,
) -> Result<(), EngineError> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = tcp.accept() => {
                let Some((stream, source, destination)) = accepted else {
                    return Err(EngineError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "raw-IP TCP stack closed")));
                };
                if connections.len() >= 4096 { continue; }
                let source_addr = zero_platform_tokio::socket_address_to_socket_addr(source);
                let mut session = Session::new(0, socket_address_to_address(destination), destination.port, Network::Tcp, ProtocolType::UNKNOWN);
                session.transparent_target = true;
                let runtime = runtime_factory.for_connection(Some(source_addr));
                connections.spawn(async move {
                    let stream = ReplayStream::new(stream, Vec::new());
                    runtime.serve(session, stream, &NoClientResponseStreamProtocol::new()).await
                });
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                match result {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => tracing::debug!(%error, "raw-IP TCP connection failed"),
                    Err(error) => tracing::warn!(%error, "raw-IP TCP connection task failed"),
                }
            }
        }
    }
}

fn socket_address_to_address(address: SocketAddress) -> Address {
    match address.ip {
        IpAddress::V4(ip) => Address::Ipv4(ip),
        IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}
