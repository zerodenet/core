//! DNS-over-UDP transport and stable wire helper facade.

use std::io;

use zero_traits::IpAddress;

pub use crate::message::DnsQuestion;
use crate::message::{
    build_address_response, parse_question, parse_response, DEFAULT_SYNTHETIC_TTL_SECONDS,
};

#[cfg(feature = "udp")]
use std::net::SocketAddr;
#[cfg(feature = "udp")]
use std::time::Duration;
#[cfg(feature = "udp")]
use zero_platform_tokio::{EgressInterfaceControl, TokioDatagramSocket};

#[cfg(feature = "udp")]
const DNS_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(feature = "udp")]
const MAX_ATTEMPTS: usize = 2;

#[cfg(feature = "udp")]
pub(crate) struct UdpDnsResolver {
    addrs: Vec<SocketAddr>,
    egress_interface: EgressInterfaceControl,
}

#[cfg(feature = "udp")]
impl UdpDnsResolver {
    pub(crate) fn new(addrs: Vec<SocketAddr>, egress_interface: EgressInterfaceControl) -> Self {
        Self {
            addrs,
            egress_interface,
        }
    }

    pub(crate) async fn exchange(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        parse_question(query)?;
        let mut last_error = None;
        for addr in &self.addrs {
            match self.exchange_with(*addr, query).await {
                Ok(response) => return Ok(response),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS UDP backend has no endpoint",
            )
        }))
    }

    async fn exchange_with(&self, addr: SocketAddr, query: &[u8]) -> io::Result<Vec<u8>> {
        let interface = self.egress_interface.current_for_peer(addr);
        let socket = TokioDatagramSocket::bind_for_peer_on(addr, interface.as_ref()).await?;
        let selected = socket.egress_interface();
        tracing::debug!(
            server = %addr,
            local = ?socket.local_addr().ok(),
            egress_name = selected.map(zero_platform_tokio::EgressInterface::name),
            egress_index = selected.map(zero_platform_tokio::EgressInterface::index),
            "DNS UDP socket bound"
        );

        tokio::time::timeout(DNS_TIMEOUT, async {
            for attempt in 0..MAX_ATTEMPTS {
                socket.send_to_addr(query, addr).await?;
                let wait = if attempt == 0 {
                    Duration::from_secs(2)
                } else {
                    Duration::from_secs(5)
                };
                let received =
                    tokio::time::timeout(wait, receive_matching(&socket, addr, query)).await;
                match received {
                    Ok(result) => return result,
                    Err(_) => continue,
                }
            }
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "DNS UDP timeout after retry",
            ))
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "DNS UDP timeout"))?
    }
}

#[cfg(feature = "udp")]
async fn receive_matching(
    socket: &TokioDatagramSocket,
    server: SocketAddr,
    query: &[u8],
) -> io::Result<Vec<u8>> {
    let mut buffer = vec![0_u8; crate::message::MAX_DNS_MESSAGE_SIZE];
    loop {
        let (size, source) = socket.recv_from_addr(&mut buffer).await?;
        if source != server {
            tracing::warn!(%source, expected = %server, "ignored DNS UDP response from unexpected source");
            continue;
        }
        let response = &buffer[..size];
        match parse_response(query, response) {
            Ok(_) => return Ok(response.to_vec()),
            Err(error) => {
                tracing::warn!(%error, %source, "ignored mismatched DNS UDP response");
            }
        }
    }
}

/// Build an Internet-class DNS query with EDNS support.
#[cfg(test)]
pub(crate) fn build_query(domain: &str, query_type: u16) -> Vec<u8> {
    crate::message::build_query(domain, query_type).unwrap_or_default()
}

/// Build a synthetic address response using the default compatibility TTL.
pub fn build_dns_response(query: &[u8], ips: &[IpAddress]) -> Vec<u8> {
    build_address_response(query, ips, DEFAULT_SYNTHETIC_TTL_SECONDS)
}

/// Parse the single question shape accepted by the TUN DNS interceptor.
pub fn parse_dns_question(query: &[u8]) -> io::Result<DnsQuestion> {
    parse_question(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_includes_edns_and_normalizes_name() {
        let query = build_query("Example.COM.", 1);
        let question = parse_dns_question(&query).expect("parse generated query");
        assert_eq!(question.domain, "example.com");
        assert_eq!(question.udp_payload_size, 4096);
    }

    #[test]
    fn synthetic_response_uses_matching_address_family() {
        let query = build_query("example.com", 1);
        let response = build_dns_response(
            &query,
            &[IpAddress::V4([192, 0, 2, 1]), IpAddress::V6([0; 16])],
        );
        let parsed = parse_response(&query, &response).expect("parse response");
        assert_eq!(parsed.addresses, vec![IpAddress::V4([192, 0, 2, 1])]);
    }
}
