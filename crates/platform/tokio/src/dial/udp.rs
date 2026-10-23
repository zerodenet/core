use super::prepare;
use crate::{
    egress::{bind_udp_to_interface, datagram_bind_address},
    EgressSelection, TokioDatagramSocket,
};
use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;
use zero_traits::{AddressFamily, DialPolicy};
impl TokioDatagramSocket {
    pub async fn bind_for_peer_with_policy(
        peer: SocketAddr,
        policy: &DialPolicy,
        selection: &EgressSelection,
    ) -> io::Result<Self> {
        Self::bind_for_peer_with_policy_preserving_port(peer, None, policy, selection).await
    }
    /// Only a source-port collision permits retrying an ephemeral port. The
    /// source address, family, interface and strict-route mark are retained.
    pub async fn bind_for_peer_with_policy_preserving_port(
        peer: SocketAddr,
        preferred_port: Option<u16>,
        policy: &DialPolicy,
        selection: &EgressSelection,
    ) -> io::Result<Self> {
        Self::bind_for_peer_with_policy_preserving_port_sync(
            peer,
            preferred_port,
            policy,
            selection,
        )
    }
    /// Synchronous native acquisition with no observer callbacks or hidden job.
    /// A grant owner must reserve its attempt before calling this and install
    /// success before its next await. An error can consume a native handle (for
    /// example Tokio registration failure); it is not a native close receipt.
    pub fn bind_for_peer_with_policy_preserving_port_sync(
        peer: SocketAddr,
        preferred_port: Option<u16>,
        policy: &DialPolicy,
        selection: &EgressSelection,
    ) -> io::Result<Self> {
        let dial = prepare(peer, policy, selection)
            .map_err(|error| phase("validate_dial_policy", error))?;
        let mut source = match dial.source {
            Some(source) => source,
            None => datagram_bind_address(dial.peer, dial.interface.as_ref())
                .map_err(|error| phase("select_source", error))?,
        };
        source.set_port(preferred_port.unwrap_or(0));
        let v6_only = policy
            .effective_family()
            .is_ok_and(|family| family == AddressFamily::OnlyIpv6);
        let socket = match bind_native(source, v6_only) {
            Ok(socket) => socket,
            Err(error) if preferred_port.is_some() && error.kind() == io::ErrorKind::AddrInUse => {
                source.set_port(0);
                bind_native(source, v6_only).map_err(|error| phase("bind_source", error))?
            }
            Err(error) => return Err(phase("bind_source", error)),
        };
        if let Some(interface) = &dial.interface {
            bind_udp_to_interface(&socket, source, interface)
                .map_err(|error| phase("bind_interface", error))?;
        }
        socket
            .set_nonblocking(true)
            .map_err(|error| phase("configure_socket", error))?;
        UdpSocket::from_std(socket)
            .map(|inner| Self {
                inner,
                egress_interface: dial.interface,
                observer: None,
            })
            .map_err(|error| phase("configure_socket", error))
    }
}
fn phase(stage: &'static str, error: io::Error) -> io::Error {
    // The outer error adds context, so its raw_os_error() may be None. Keep the
    // original error (including its native code or typed payload) in source().
    io::Error::new(
        error.kind(),
        BindPhaseError {
            stage,
            source: error,
        },
    )
}
#[derive(Debug)]
struct BindPhaseError {
    stage: &'static str,
    source: io::Error,
}
impl std::fmt::Display for BindPhaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.stage, self.source)
    }
}
impl std::error::Error for BindPhaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
fn bind_native(source: SocketAddr, v6_only: bool) -> io::Result<std::net::UdpSocket> {
    let domain = if source.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    if source.is_ipv6() && v6_only {
        // A v6 socket must not provide a mapped-v4 escape from only_ipv6.
        socket.set_only_v6(true)?;
    }
    socket.bind(&source.into())?;
    Ok(socket.into())
}

#[cfg(test)]
#[path = "../../tests/dial_policy/udp_error.rs"]
mod tests;
