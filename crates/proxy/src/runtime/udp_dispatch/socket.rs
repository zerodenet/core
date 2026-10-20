//! Same dispatch polling API over either the legacy or explicitly granted owner.
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) mod bound;
use crate::runtime::{udp_ingress::UdpIngressRuntime, udp_socket::*};
use std::net::SocketAddr;
use zero_core::Address;
use zero_engine::EngineError;

pub(crate) enum DispatchDirectSockets {
    Legacy(Box<DirectUdpSockets>),
    #[cfg(feature = "managed-datagram-runtime")]
    Bound(Box<bound::BoundDirectSockets>),
}
impl DispatchDirectSockets {
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
        Ok(Self::Legacy(Box::new(DirectUdpSockets::new(
            runtime.services().network(),
            preferred_port,
        ))))
    }
    pub(crate) fn generation(&self) -> u64 {
        match self {
            Self::Legacy(socket) => socket.generation(),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => socket.generation,
        }
    }
    pub(crate) fn refresh_if_stale(&mut self) {
        match self {
            Self::Legacy(socket) => socket.refresh_if_stale(),
            // The same original bound driver checks actual epoch fences before
            // every native send/receive and retires stale source guards.
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(_) => {}
        }
    }
    pub(crate) fn isolate_association(&mut self, session: u64, association: u64) {
        match self {
            Self::Legacy(socket) => socket.isolate_association(session, association),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => {
                socket.isolation.insert(session, association);
            }
        }
    }
    pub(crate) fn retire_session(&mut self, session: u64) {
        match self {
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
            Self::Legacy(socket) => DispatchDirectReceive::Legacy(socket),
            #[cfg(feature = "managed-datagram-runtime")]
            Self::Bound(socket) => DispatchDirectReceive::Bound(socket.bridge.receiver()),
        }
    }
}
/// Borrow only the real receive side, never the unique Send-only controllers.
pub(crate) enum DispatchDirectReceive<'a> {
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
