//! Production dispatch uses the original host-issued multi-target native provider.
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod bound;
use crate::runtime::{udp_ingress::UdpIngressRuntime, udp_socket::*};
use std::net::SocketAddr;
use zero_core::Address;
use zero_engine::EngineError;

pub(crate) enum DispatchDirectSockets {
    #[cfg(test)]
    Legacy(Box<DirectUdpSockets>),
    #[cfg(feature = "managed-datagram-runtime")]
    Bound(Box<bound::BoundDirectSockets>),
}
impl DispatchDirectSockets {
    pub(crate) async fn close_originals_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<
        zero_platform_tokio::contract::lifecycle::CloseReport,
        zero_platform_tokio::contract::ChannelError,
    > {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => {
                let _ = deadline;
                socket.close_all_native();
                Ok(socket.take_native_close_report())
            }
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => socket.close_originals(deadline).await,
        }
    }

    #[cfg(test)]
    pub(crate) fn close_originals(
        &mut self,
    ) -> zero_platform_tokio::contract::lifecycle::CloseReport {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => {
                socket.close_all_native();
                socket.take_native_close_report()
            }
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(_) => zero_platform_tokio::contract::lifecycle::CloseReport {
                unfinished: vec![
                    zero_platform_tokio::contract::lifecycle::Unfinished::Resource(
                        "bound UDP originals require their real peer owner".into(),
                    ),
                ],
                ..Default::default()
            },
        }
    }
    pub(crate) async fn new(
        runtime: &UdpIngressRuntime,
        preferred_port: Option<u16>,
    ) -> Result<Self, EngineError> {
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(budget) = runtime.bound_direct_budget() {
            return bound::BoundDirectSockets::new(runtime, budget)
                .await
                .map(|bound| Self::Bound(Box::new(bound)));
        }
        #[cfg(test)]
        if runtime.accepted_caller().is_none() && runtime.peer_resources().is_none() {
            let sockets = DirectUdpSockets::new(runtime.services().network(), preferred_port);
            let sockets = match runtime.accepted_native_socket_limit() {
                Some(limit) => sockets.with_native_originals(limit),
                None => sockets,
            };
            return Ok(Self::Legacy(Box::new(sockets)));
        }
        let _ = preferred_port;
        Err(std::io::Error::other("UDP dispatch has no actual installed Direct provider").into())
    }
    pub(crate) fn generation(&self) -> u64 {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => socket.generation(),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => socket.generation,
        }
    }
    pub(crate) fn refresh_if_stale(&mut self) {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => socket.refresh_if_stale(),
            // The same original bound driver checks actual epoch fences before
            // every native send/receive and retires stale source guards.
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(_) => {}
        }
    }
    pub(crate) fn isolate_association(&mut self, session: u64, association: u64) {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => socket.isolate_association(session, association),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => {
                socket.isolation.insert(session, association);
            }
        }
    }
    pub(crate) fn retire_session(&mut self, session: u64) {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => socket.retire_session(session),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => {
                socket.isolation.remove(&session);
                socket
                    .bridge
                    .retire_session(zero_core::component::SessionId(session));
            }
        }
    }
    pub(crate) async fn send_to_addr(
        &mut self,
        payload: &[u8],
        target: SocketAddr,
        session: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<usize, EngineError> {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => socket.send_to_addr(payload, target, session, policy).await,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => socket
                .bridge
                .send_new_packet(
                    zero_core::component::SessionId(session),
                    &match target.ip() {
                        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
                    },
                    &[target],
                    payload,
                )
                .await
                .map(|sent| sent.sent),
        }
    }
    pub(crate) async fn send_new_packet(
        &mut self,
        target: &Address,
        candidates: &[SocketAddr],
        payload: &[u8],
        session: u64,
        policy: &DirectUdpPolicy,
    ) -> Result<DirectUdpSentPacket, EngineError> {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => {
                socket
                    .send_new_packet(target, candidates, payload, session, policy)
                    .await
            }
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => {
                socket
                    .bridge
                    .send_new_packet(
                        zero_core::component::SessionId(session),
                        target,
                        candidates,
                        payload,
                    )
                    .await
            }
        }
    }
    pub(crate) fn receiver(&self) -> DispatchDirectReceive<'_> {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => DispatchDirectReceive::Legacy(socket),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => DispatchDirectReceive::Bound(socket.bridge.receiver()),
        }
    }
}
/// Borrow only the real receive side, never the unique Send-only controllers.
pub(crate) enum DispatchDirectReceive<'a> {
    #[cfg(test)]
    Legacy(&'a DirectUdpSockets),
    #[cfg(feature = "managed-datagram-runtime")]
    Bound(crate::runtime::udp_socket::endpoint::dispatch::DirectDispatchReceive<'a>),
}
impl DispatchDirectReceive<'_> {
    pub(crate) async fn recv_from_addr(
        self,
        output: &mut [u8],
    ) -> std::io::Result<(usize, DirectUdpResponseSource)> {
        match self {
            #[cfg(test)]
            Self::Legacy(socket) => socket.recv_from_addr(output).await,
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(receiver) => {
                receiver
                    .recv_from_addr(output)
                    .await
                    .map_err(|error| match error {
                        EngineError::Io(error) => error,
                        other => std::io::Error::other(other),
                    })
            }
        }
    }
}
