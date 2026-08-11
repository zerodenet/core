//! Neutral UDP endpoint resolution, socket binding, and packet sending.

#[cfg(feature = "udp-runtime")]
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[cfg(feature = "udp-runtime")]
use zero_engine::EngineError;
#[cfg(feature = "udp-runtime")]
use zero_platform_tokio::TokioDatagramSocket;

#[cfg(feature = "udp-runtime")]
pub(crate) struct DirectUdpSockets {
    ipv4: TokioDatagramSocket,
    ipv6: Option<TokioDatagramSocket>,
    ipv6_buffer: tokio::sync::Mutex<Vec<u8>>,
}

#[cfg(feature = "udp-runtime")]
impl DirectUdpSockets {
    pub(crate) async fn bind(
        services: &crate::protocol_registry::UdpNetworkServices,
    ) -> Result<Self, EngineError> {
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
        Ok(Self {
            ipv4,
            ipv6,
            ipv6_buffer: tokio::sync::Mutex::new(vec![0_u8; 65_535]),
        })
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

/// Send UDP packet directly to target.
#[cfg(feature = "udp-runtime")]
pub(crate) async fn send_direct_udp_packet(
    socket: &TokioDatagramSocket,
    target_addr: SocketAddr,
    payload: &[u8],
) -> Result<usize, EngineError> {
    socket
        .send_to_addr(payload, target_addr)
        .await
        .map_err(Into::into)
}

pub(crate) fn datagram_bind_addr_for_peer(peer: SocketAddr) -> SocketAddr {
    match peer {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
}

pub(crate) async fn bind_datagram_socket_for_peer(
    peer: SocketAddr,
    interface: Option<&zero_platform_tokio::EgressInterface>,
) -> Result<TokioDatagramSocket, EngineError> {
    TokioDatagramSocket::bind_addr_on(datagram_bind_addr_for_peer(peer), interface)
        .await
        .map_err(Into::into)
}
