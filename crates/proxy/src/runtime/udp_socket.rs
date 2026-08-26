//! Neutral UDP endpoint resolution, socket binding, and packet sending.

#[cfg(feature = "udp-runtime")]
use std::net::SocketAddr;

#[cfg(feature = "udp-runtime")]
use futures_util::{stream::FuturesUnordered, StreamExt};
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
}

#[cfg(feature = "udp-runtime")]
struct DirectUdpSocket {
    socket: TokioDatagramSocket,
    binding: DirectUdpSocketBinding,
    receive_buffer: tokio::sync::Mutex<Vec<u8>>,
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
        let ipv4 = services
            .bind_direct_datagram_socket(
                "0.0.0.0:0".parse().expect("valid IPv4 wildcard"),
                preferred_port,
            )
            .await?;
        let ipv6 = services
            .bind_direct_datagram_socket(
                "[::]:0".parse().expect("valid IPv6 wildcard"),
                preferred_port,
            )
            .await
            .map_err(|error| {
                tracing::debug!(error = %error, "IPv6 direct UDP socket is unavailable");
                error
            })
            .ok();
        log_direct_socket("IPv4", &ipv4);
        if let Some(ipv6) = ipv6.as_ref() {
            log_direct_socket("IPv6", ipv6);
        }
        let mut sockets = vec![DirectUdpSocket::new(ipv4, false)];
        if let Some(ipv6) = ipv6 {
            sockets.push(DirectUdpSocket::new(ipv6, true));
        }
        Ok(Self {
            sockets,
            preferred_port,
            generation,
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
        let ipv6_available = self.sockets.iter().any(|socket| socket.binding.ipv6);
        select_stable_udp_target(logical_target, candidates, ipv6_available).ok_or_else(|| {
            EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "no usable direct UDP target address",
            ))
        })
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) async fn send_to_addr(
        &mut self,
        services: &crate::protocol_registry::UdpNetworkServices,
        payload: &[u8],
        target: SocketAddr,
    ) -> Result<usize, EngineError> {
        let binding = DirectUdpSocketBinding {
            ipv6: target.is_ipv6(),
            egress: services.direct_datagram_egress(target),
        };
        let socket_index = match self
            .sockets
            .iter()
            .position(|socket| socket.binding == binding)
        {
            Some(index) => index,
            None => {
                let socket = services
                    .bind_direct_datagram_socket(target, self.preferred_port)
                    .await?;
                log_direct_socket(if target.is_ipv6() { "IPv6" } else { "IPv4" }, &socket);
                self.sockets
                    .push(DirectUdpSocket::new(socket, target.is_ipv6()));
                self.sockets.len() - 1
            }
        };
        send_direct_udp_packet(&self.sockets[socket_index].socket, target, payload).await
    }

    pub(crate) async fn recv_from_addr(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, SocketAddr), std::io::Error> {
        let mut receives = FuturesUnordered::new();
        for entry in &self.sockets {
            receives.push(async move {
                let mut buffer = entry.receive_buffer.lock().await;
                let result = entry.socket.recv_from_addr(&mut buffer).await;
                (result, buffer)
            });
        }
        let (result, buffer) = receives
            .next()
            .await
            .expect("direct UDP socket set is never empty");
        let (size, sender) = result?;
        let size = size.min(output.len());
        output[..size].copy_from_slice(&buffer[..size]);
        Ok((size, sender))
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
    ipv6_available: bool,
) -> Option<SocketAddr> {
    let preferred_ipv6 = candidates
        .iter()
        .find(|candidate| candidate.is_ipv4() || ipv6_available)
        .map(SocketAddr::is_ipv6)?;

    candidates
        .iter()
        .copied()
        .filter(|candidate| candidate.is_ipv4() || ipv6_available)
        .filter(|candidate| candidate.is_ipv6() == preferred_ipv6)
        .max_by_key(|candidate| udp_candidate_score(logical_target, *candidate))
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
