//! Neutral UDP endpoint resolution, socket binding, and packet sending.

#[cfg(feature = "udp-runtime")]
use std::net::SocketAddr;

#[cfg(feature = "udp-runtime")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "udp-runtime")]
use zero_core::Address;
#[cfg(feature = "udp-runtime")]
use zero_engine::EngineError;
#[cfg(feature = "udp-runtime")]
use zero_platform_tokio::TokioDatagramSocket;

/// Immutable route-time policy identity. A tag can return to the same policy
/// after a reload, so its monotonic revision is part of the identity as well.
#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DirectUdpPolicy {
    pub(crate) tag: Option<String>,
    pub(crate) dial_policy: zero_traits::DialPolicy,
    pub(crate) generation: u64,
}

#[cfg(feature = "udp-runtime")]
pub(crate) struct DirectUdpSentPacket {
    pub(crate) sent: usize,
    pub(crate) target: SocketAddr,
    pub(crate) local: Option<SocketAddr>,
    pub(crate) selection: zero_platform_tokio::EgressSelection,
}

#[cfg(feature = "udp-runtime")]
pub(crate) struct DirectUdpSockets {
    sockets: Vec<DirectUdpSocket>,
    services: crate::protocol_registry::UdpNetworkServices,
    preferred_port: Option<u16>,
    generation: u64,
    next_socket_id: u64,
    isolated_sessions: HashMap<u64, u64>,
    // Socket identity includes the policy and association, unlike a remote
    // endpoint alone. Even unscoped sockets require an exact registered peer.
    response_flows: HashMap<(u64, SocketAddr), DirectUdpResponseFlow>,
}

#[cfg(feature = "udp-runtime")]
struct DirectUdpSocket {
    id: u64,
    socket: TokioDatagramSocket,
    binding: DirectUdpSocketBinding,
    selection: zero_platform_tokio::EgressSelection,
    receive_buffer: tokio::sync::Mutex<Vec<u8>>,
    association_id: Option<u64>,
    // A peer's old packets must never acquire a new owner on this socket.
    retired_peers: HashSet<SocketAddr>,
}

// Do not expire tombstones on a live socket: arbitrary delayed packets could
// then be misattributed. Bound lifetime peer admission instead, and close each
// socket when its last active mapping is retired.
#[cfg(feature = "udp-runtime")]
const MAX_DIRECT_UDP_PEERS_PER_SOCKET: usize = 256;

#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectUdpSocketBinding {
    policy: DirectUdpPolicy,
    ipv6: bool,
    egress_generation: u64,
    source: Option<SocketAddr>,
    egress: Option<zero_platform_tokio::EgressInterface>,
}

#[cfg(feature = "udp-runtime")]
impl DirectUdpSockets {
    /// A dispatcher need not use Direct or either native address family.
    /// Defer every socket bind until a policy-checked destination is known.
    pub(crate) fn new(
        services: crate::protocol_registry::UdpNetworkServices,
        preferred_port: Option<u16>,
    ) -> Self {
        Self {
            generation: services.egress_generation(),
            services,
            sockets: Vec::new(),
            preferred_port,
            next_socket_id: 0,
            isolated_sessions: HashMap::new(),
            response_flows: HashMap::new(),
        }
    }

    pub(crate) fn refresh_if_stale(&mut self) {
        let generation = self.services.egress_generation();
        if self.generation != generation {
            self.sockets.clear();
            self.response_flows.clear();
            self.generation = generation;
        } else {
            self.sockets.retain(|socket| {
                self.services
                    .direct_policy_is_current(&socket.binding.policy)
            });
            self.response_flows.retain(|(socket_id, _), _| {
                self.sockets.iter().any(|socket| socket.id == *socket_id)
            });
        }
    }

    #[cfg(test)]
    pub(crate) fn select_target(
        logical_target: &Address,
        candidates: &[SocketAddr],
        policy: &zero_traits::DialPolicy,
    ) -> Result<SocketAddr, EngineError> {
        let candidates = policy
            .filter_candidates(candidates.iter().copied())
            .map_err(invalid_policy)?;
        select_stable_udp_target(logical_target, &candidates, true, true).ok_or_else(|| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no direct UDP target satisfies the dial policy",
            ))
        })
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

#[cfg(feature = "udp-runtime")]
fn invalid_policy(error: zero_traits::DialPolicyError) -> EngineError {
    EngineError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

/// Select one candidate without pinning every logical target to the first DNS
/// answer. The resolver's first usable address family remains preferred, while
/// rendezvous hashing makes selection within that family stable across answer
/// reordering and minimally disruptive when the answer set changes.
#[cfg(feature = "udp-runtime")]
fn select_stable_udp_target(
    logical_target: &Address,
    candidates: &[SocketAddr],
    ipv4_available: bool,
    ipv6_available: bool,
) -> Option<SocketAddr> {
    let preferred_ipv6 = candidates
        .iter()
        .find(|candidate| {
            (candidate.is_ipv4() && ipv4_available) || (candidate.is_ipv6() && ipv6_available)
        })
        .map(SocketAddr::is_ipv6)?;

    candidates
        .iter()
        .copied()
        .filter(|candidate| {
            (candidate.is_ipv4() && ipv4_available) || (candidate.is_ipv6() && ipv6_available)
        })
        .filter(|candidate| candidate.is_ipv6() == preferred_ipv6)
        .max_by_key(|candidate| udp_candidate_score(logical_target, *candidate))
}

#[cfg(feature = "udp-runtime")]
fn family_name(ipv6: bool) -> &'static str {
    if ipv6 {
        "IPv6"
    } else {
        "IPv4"
    }
}

#[cfg(feature = "udp-runtime")]
fn udp_candidate_score(logical_target: &Address, candidate: SocketAddr) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    fn extend(mut hash: u64, bytes: &[u8]) -> u64 {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash
    }

    let mut hash = match logical_target {
        Address::Domain(domain) => extend(OFFSET, domain.to_ascii_lowercase().as_bytes()),
        Address::Ipv4(address) => extend(OFFSET, address),
        Address::Ipv6(address) => extend(OFFSET, address),
    };
    hash = match candidate.ip() {
        std::net::IpAddr::V4(address) => extend(hash, &address.octets()),
        std::net::IpAddr::V6(address) => extend(hash, &address.octets()),
    };
    extend(hash, &candidate.port().to_be_bytes())
}

#[cfg(feature = "udp-runtime")]
fn log_direct_socket(family: &str, socket: &TokioDatagramSocket) {
    let local = socket.local_addr().ok();
    let egress = socket.egress_interface();
    tracing::debug!(
        family,
        ?local,
        egress_name = egress.map(zero_platform_tokio::EgressInterface::name),
        egress_index = egress.map(zero_platform_tokio::EgressInterface::index),
        "direct UDP socket bound"
    );
}

/// Send UDP packet directly to target.
#[cfg(feature = "udp-runtime")]
pub(crate) async fn send_direct_udp_packet(
    socket: &TokioDatagramSocket,
    target_addr: SocketAddr,
    payload: &[u8],
) -> Result<usize, EngineError> {
    let egress = socket.egress_interface();
    tracing::trace!(
        local = ?socket.local_addr().ok(),
        target = %target_addr,
        egress_name = egress.map(zero_platform_tokio::EgressInterface::name),
        egress_index = egress.map(zero_platform_tokio::EgressInterface::index),
        payload_len = payload.len(),
        "direct UDP packet send"
    );
    socket
        .send_to_addr(payload, target_addr)
        .await
        .map_err(Into::into)
}

#[cfg(all(test, feature = "udp-runtime"))]
mod tests;

#[cfg(feature = "udp-runtime")]
mod io;
#[cfg(feature = "udp-runtime")]
use response::DirectUdpResponseFlow;
#[cfg(feature = "udp-runtime")]
pub(crate) use response::{DirectUdpResponseGuard, DirectUdpResponseSource};
#[cfg(feature = "udp-runtime")]
mod response;

#[cfg(feature = "udp-runtime")]
mod select;
