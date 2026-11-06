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

/// The existing DNS consumer plus this actual Source's input admission. The
/// standalone stack fixture deliberately has no Source/native-wire identity.
pub(super) struct TcpSourceTasks {
    dns: Option<crate::runtime::raw_ip::tasks::scoped::ScopedSourceTasks<Result<(), io::Error>>>,
    input: SourceInput,
}
enum SourceInput {
    Hosted(Arc<zero_platform_tokio::contract::packet::PacketSourcePause>),
    #[cfg(test)]
    ExplicitStackFixture,
}
impl TcpSourceTasks {
    pub(super) fn hosted(
        dns: Option<
            crate::runtime::raw_ip::tasks::scoped::ScopedSourceTasks<Result<(), io::Error>>,
        >,
        admission: Arc<zero_platform_tokio::contract::packet::PacketSourcePause>,
    ) -> Self {
        Self {
            dns,
            input: SourceInput::Hosted(admission),
        }
    }
    #[cfg(test)]
    pub(super) fn fixture() -> Self {
        Self {
            dns: None,
            input: SourceInput::ExplicitStackFixture,
        }
    }
}

pub(super) async fn accept(
    tcp: Arc<UserTcpStack>,
    runtime_factory: InboundRouteRuntimeFactory,
    group: Arc<InboundPeerTasks>,
    host: InboundTaskAccess,
    native: super::native_obligations::NativeObligations,
    tun: Option<crate::inbound::tun::TunIngressOptions>,
    source_tasks: TcpSourceTasks,
) -> Result<(), EngineError> {
    let dns = &source_tasks.dns;
    let mut dns_tasks = match dns {
        Some(dns) => Some(dns.lock().await.map_err(std::io::Error::other)?),
        None => None,
    };
    loop {
        let accepted = tokio::select! {
            accepted=async {
                match &source_tasks.input {
                    SourceInput::Hosted(admission) => {
                        let (accepted, acquisition) = crate::runtime::raw_ip::tasks::input::admitted(
                            tcp.accept(), admission,
                        ).await.map_err(io::Error::other)?;
                        Ok::<_, EngineError>((accepted, Some(acquisition)))
                    }
                    #[cfg(test)]
                    SourceInput::ExplicitStackFixture => Ok((tcp.accept().await, None)),
                }
            }=>accepted?,
            completed=async {dns_tasks.as_mut().expect("declared device DNS receiver").join_next().await},if dns_tasks.as_ref().is_some_and(|tasks|tasks.len()!=0)=>{
                match completed {
                    Some(Ok(Ok(())))=>{},
                    Some(Ok(Err(source)))=>tracing::warn!(error=%source,"TUN DNS TCP connection failed"),
                    Some(Err(source))=>return Err(std::io::Error::other(dns.as_ref().expect("same DNS receiver").remember(source)).into()),
                    None=>{},
                }
                continue;
            }
        };
        // Keep the same admission across reserve and actual spawn_bound. A
        // pause cannot ACK between accepted socket dequeue and its real owner.
        let (accepted, _acquisition) = accepted;
        let Some((stream, source, destination)) = accepted else {
            return Err(EngineError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "raw-IP TCP stack closed",
            )));
        };
        if let Some(policy) = tun
            .as_ref()
            .filter(|policy| policy.dns_hijack && destination.port == 53)
        {
            policy
                .dns_hijacked_queries
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let tasks = dns_tasks.as_mut().ok_or_else(|| {
                std::io::Error::other("actual device DNS task set was not declared")
            })?;
            if tasks.len() >= crate::inbound::tun::MAX_TCP_DNS_CONNECTIONS {
                tracing::warn!(
                    active_dns_connections = tasks.len(),
                    "rejecting TUN DNS TCP connection at the concurrency limit"
                );
                continue;
            }
            let resolver = runtime_factory.tun_dns_resolver();
            let native = native.clone();
            tasks
                .spawn_with_witness(move |caller| {
                    crate::inbound::tun::serve_dns_tcp_owned(resolver, stream, native, caller)
                })
                .map_err(std::io::Error::other)?;
            continue;
        }
        if group.occupied() >= MAX_CONNECTIONS {
            continue;
        }
        let mut native = native
            .reserve_reclaimed(
                super::native_obligations::ChildKind::Tcp,
                tokio::time::Instant::now() + std::time::Duration::from_secs(5),
            )
            .await
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
        let sniff = tun.is_some();
        group.spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            let stop = binding.cancellation();
            native.bind_caller(binding).map_err(std::io::Error::other)?;
            let runtime = runtime.with_caller_native(native.registration());
            let (session, stream) = if sniff {
                crate::inbound::tun::sniff_tcp_target(session, stream).await
            } else {
                (session, ReplayStream::new(stream, Vec::new()))
            };
            native.enter().map_err(std::io::Error::other)?;
            let protocol = NoClientResponseStreamProtocol::new();
            let result = tokio::select! { biased;
                result = runtime.serve(session, stream, &protocol) => result,
                _ = stop.cancelled() => Ok(()),
            };
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
