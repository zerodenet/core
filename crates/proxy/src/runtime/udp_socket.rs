//! Neutral UDP endpoint resolution, socket binding, and packet sending.

#[cfg(feature = "udp-runtime")]
use std::net::SocketAddr;

#[cfg(feature = "udp-runtime")]
use std::collections::HashSet;
#[cfg(feature = "udp-runtime")]
use zero_core::Address;
#[cfg(feature = "udp-runtime")]
use zero_engine::EngineError;
#[cfg(feature = "udp-runtime")]
use zero_platform_tokio::TokioDatagramSocket;

#[cfg(feature = "udp-runtime")]
pub(crate) struct DirectUdpSockets {
    sockets: Vec<DirectUdpSocket>,
    preferred_port: Option<u16>,
    generation: u64,
    isolated_sessions: HashSet<u64>,
}

#[cfg(feature = "udp-runtime")]
struct DirectUdpSocket {
    socket: TokioDatagramSocket,
    binding: DirectUdpSocketBinding,
    receive_buffer: tokio::sync::Mutex<Vec<u8>>,
    session_id: Option<u64>,
}

#[cfg(feature = "udp-runtime")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct DirectUdpSocketBinding {
    ipv6: bool,
    egress: Option<zero_platform_tokio::EgressInterface>,
}

#[cfg(feature = "udp-runtime")]
impl DirectUdpSocket {
    fn new(socket: TokioDatagramSocket, ipv6: bool) -> Self {
        let binding = DirectUdpSocketBinding {
            ipv6,
            egress: socket.egress_interface().cloned(),
        };
        Self {
            socket,
            binding,
            receive_buffer: tokio::sync::Mutex::new(vec![0_u8; 65_535]),
            session_id: None,
        }
    }
}
#[cfg(feature = "udp-runtime")]
impl DirectUdpSockets {
    pub(crate) async fn bind(
        services: &crate::protocol_registry::UdpNetworkServices,
        preferred_port: Option<u16>,
    ) -> Result<Self, EngineError> {
        let generation = services.egress_generation();
        Self::bind_with(generation, preferred_port, |peer, preferred_port| {
            services.bind_direct_datagram_socket(peer, preferred_port)
        })
        .await
    }

    async fn bind_with<F, Fut>(
        generation: u64,
        preferred_port: Option<u16>,
        mut bind: F,
    ) -> Result<Self, EngineError>
    where
        F: FnMut(SocketAddr, Option<u16>) -> Fut,
        Fut: std::future::Future<Output = Result<TokioDatagramSocket, EngineError>>,
    {
        let ipv4 = bind(
            "0.0.0.0:0".parse().expect("valid IPv4 wildcard"),
            preferred_port,
        )
        .await;
        let ipv6 = bind(
            "[::]:0".parse().expect("valid IPv6 wildcard"),
            preferred_port,
        )
        .await;
        let outcomes = collect_family_bind_outcomes(ipv4, ipv6);
        if outcomes.available.is_empty() {
            let failures = outcomes
                .failures
                .iter()
                .map(|(ipv6, error)| format!("{}: {error}", family_name(*ipv6)))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                format!("no direct UDP address family is available ({failures})"),
            )));
        }
        for (ipv6, error) in &outcomes.failures {
            tracing::debug!(
                address_family = family_name(*ipv6),
                error = %error,
                "direct UDP socket family is unavailable"
            );
        }
        let sockets = outcomes
            .available
            .into_iter()
            .map(|(ipv6, socket)| {
                log_direct_socket(family_name(ipv6), &socket);
                DirectUdpSocket::new(socket, ipv6)
            })
            .collect();
        Ok(Self {
            sockets,
            preferred_port,
            generation,
            isolated_sessions: HashSet::new(),
        })
    }

    pub(crate) async fn refresh_if_stale(
        &mut self,
        services: &crate::protocol_registry::UdpNetworkServices,
    ) -> Result<(), EngineError> {
        let current_generation = services.egress_generation();
        if self.generation == current_generation {
            return Ok(());
        }

        let previous_generation = self.generation;
        let mut replacement = Self::bind(services, self.preferred_port).await?;
        for _ in 0..2 {
            if replacement.generation == services.egress_generation() {
                break;
            }
            replacement = Self::bind(services, self.preferred_port).await?;
        }
        if replacement.generation != services.egress_generation() {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "egress topology changed repeatedly while rebuilding direct UDP sockets",
            )));
        }
        let replacement_generation = replacement.generation;
        replacement.isolated_sessions = std::mem::take(&mut self.isolated_sessions);
        *self = replacement;
        tracing::info!(
            previous_generation,
            generation = replacement_generation,
            "rebuilt direct UDP sockets after egress topology change"
        );
        Ok(())
    }

    pub(crate) fn select_target(
        &self,
        logical_target: &Address,
        candidates: &[SocketAddr],
    ) -> Result<SocketAddr, EngineError> {
        let ipv4_available = self.sockets.iter().any(|socket| !socket.binding.ipv6);
        let ipv6_available = self.sockets.iter().any(|socket| socket.binding.ipv6);
        select_stable_udp_target(logical_target, candidates, ipv4_available, ipv6_available)
            .ok_or_else(|| {
                EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "no usable direct UDP target address",
                ))
            })
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
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
struct FamilyBindOutcomes<T, E> {
    available: Vec<(bool, T)>,
    failures: Vec<(bool, E)>,
}

#[cfg(feature = "udp-runtime")]
fn collect_family_bind_outcomes<T, E>(
    ipv4: Result<T, E>,
    ipv6: Result<T, E>,
) -> FamilyBindOutcomes<T, E> {
    let mut outcomes = FamilyBindOutcomes {
        available: Vec::with_capacity(2),
        failures: Vec::with_capacity(2),
    };
    for (ipv6, result) in [(false, ipv4), (true, ipv6)] {
        match result {
            Ok(value) => outcomes.available.push((ipv6, value)),
            Err(error) => outcomes.failures.push((ipv6, error)),
        }
    }
    outcomes
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

#[cfg(test)]
mod tests;

#[cfg(feature = "udp-runtime")]
mod io;
#[cfg(feature = "udp-runtime")]
pub(crate) use io::DirectUdpResponseSource;
