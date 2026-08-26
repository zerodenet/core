//! Neutral UDP endpoint resolution, socket binding, and packet sending.

#[cfg(feature = "udp-runtime")]
use std::net::SocketAddr;

#[cfg(feature = "udp-runtime")]
use zero_core::Address;
#[cfg(feature = "udp-runtime")]
use zero_engine::EngineError;
#[cfg(feature = "udp-runtime")]
use zero_platform_tokio::TokioDatagramSocket;

#[cfg(feature = "udp-runtime")]
pub(crate) struct DirectUdpSockets {
    ipv4: TokioDatagramSocket,
    ipv6: Option<TokioDatagramSocket>,
    ipv6_buffer: tokio::sync::Mutex<Vec<u8>>,
    generation: u64,
}
#[cfg(feature = "udp-runtime")]
impl DirectUdpSockets {
    pub(crate) async fn bind(
        services: &crate::protocol_registry::UdpNetworkServices,
    ) -> Result<Self, EngineError> {
        let generation = services.egress_generation();
        let ipv4 = services
            .bind_datagram_socket("0.0.0.0:0".parse().expect("valid IPv4 wildcard"))
            .await?;
        let ipv6 = services
            .bind_datagram_socket("[::]:0".parse().expect("valid IPv6 wildcard"))
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
        Ok(Self {
            ipv4,
            ipv6,
            ipv6_buffer: tokio::sync::Mutex::new(vec![0_u8; 65_535]),
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
        let mut replacement = Self::bind(services).await?;
        for _ in 0..2 {
            if replacement.generation == services.egress_generation() {
                break;
            }
            replacement = Self::bind(services).await?;
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
        select_stable_udp_target(logical_target, candidates, self.ipv6.is_some()).ok_or_else(|| {
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
        &self,
        payload: &[u8],
        target: SocketAddr,
    ) -> Result<usize, EngineError> {
        let socket = if target.is_ipv4() {
            &self.ipv4
        } else {
            self.ipv6.as_ref().ok_or_else(|| {
                EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "IPv6 direct UDP socket is unavailable",
                ))
            })?
        };
        send_direct_udp_packet(socket, target, payload).await
    }

    pub(crate) async fn recv_from_addr(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, SocketAddr), std::io::Error> {
        match &self.ipv6 {
            Some(ipv6) => {
                let mut ipv6_buffer = self.ipv6_buffer.lock().await;
                tokio::select! {
                    result = self.ipv4.recv_from_addr(output) => result,
                    result = ipv6.recv_from_addr(&mut ipv6_buffer) => {
                        let (size, sender) = result?;
                        let size = size.min(output.len());
                        output[..size].copy_from_slice(&ipv6_buffer[..size]);
                        Ok((size, sender))
                    }
                }
            }
            None => self.ipv4.recv_from_addr(output).await,
        }
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
