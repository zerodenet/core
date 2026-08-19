//! UDP DNS wire format — query building, response parsing, response building.

use std::io;
use zero_traits::IpAddress;

// ── UDP DNS resolver (feature-gated) ────────────────────────────────────

#[cfg(feature = "udp")]
use std::net::SocketAddr;
#[cfg(feature = "udp")]
use std::time::Duration;
#[cfg(feature = "udp")]
use zero_platform_tokio::{EgressInterfaceControl, TokioDatagramSocket};

/// Minimal UDP DNS resolver.
#[cfg(feature = "udp")]
pub(crate) struct UdpDnsResolver {
    addr: SocketAddr,
    egress_interface: EgressInterfaceControl,
}

#[cfg(feature = "udp")]
impl UdpDnsResolver {
    pub(crate) fn new(addr: SocketAddr, egress_interface: EgressInterfaceControl) -> Self {
        Self {
            addr,
            egress_interface,
        }
    }

    pub(crate) async fn resolve(&self, domain: &str) -> io::Result<Vec<IpAddress>> {
        let interface = self.egress_interface.current_for_peer(self.addr);
        let socket = TokioDatagramSocket::bind_for_peer_on(self.addr, interface.as_ref()).await?;
        let selected = socket.egress_interface();
        tracing::debug!(
            server = %self.addr,
            local = ?socket.local_addr().ok(),
            egress_name = selected.map(zero_platform_tokio::EgressInterface::name),
            egress_index = selected.map(zero_platform_tokio::EgressInterface::index),
            "DNS UDP socket bound"
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut ips = self.query(&socket, domain, 0x0001).await?;
            if ips.is_empty() {
                ips = self.query(&socket, domain, 0x001c).await?;
            }
            Ok(ips)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "dns udp timeout"))?
    }

    async fn query(
        &self,
        socket: &TokioDatagramSocket,
        domain: &str,
        qtype: u16,
    ) -> io::Result<Vec<IpAddress>> {
        let msg = build_query(domain, qtype);

        // Try up to 2 attempts: first attempt, then one retransmission after 2s.
        for attempt in 0..2 {
            socket.send_to_addr(&msg, self.addr).await?;

            let recv = tokio::time::timeout(
                if attempt == 0 {
                    Duration::from_secs(2)
                } else {
                    Duration::from_secs(5)
                },
                async {
                    let mut buf = [0u8; 512];
                    let (n, _) = socket.recv_from_addr(&mut buf).await?;
                    Ok::<_, io::Error>((n, buf))
                },
            )
            .await;

            match recv {
                Ok(Ok((n, buf))) => return parse_response(&buf[..n], qtype),
                Ok(Err(e)) => return Err(e),
                Err(_) => continue, // timeout, retry
            }
        }

        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "dns udp timeout after retry",
        ))
    }
}

/// Build a minimal DNS query message.
#[allow(dead_code)]
pub(crate) fn build_query(domain: &str, qtype: u16) -> Vec<u8> {
    use std::sync::atomic::{AtomicU16, Ordering};
    static DNS_ID: AtomicU16 = AtomicU16::new(1);
    let mut buf = Vec::with_capacity(64);

    // Header (12 bytes)
    let id = DNS_ID.fetch_add(1, Ordering::Relaxed);
    buf.extend_from_slice(&id.to_be_bytes()); // ID
    buf.extend_from_slice(&[0x01, 0x00]); // Flags: standard query, recursion desired
    buf.extend_from_slice(&[0x00, 0x01]); // QDCOUNT = 1
    buf.extend_from_slice(&[0x00, 0x00]); // ANCOUNT = 0
    buf.extend_from_slice(&[0x00, 0x00]); // NSCOUNT = 0
    buf.extend_from_slice(&[0x00, 0x00]); // ARCOUNT = 0

    // Question section
    for label in domain.split('.') {
        buf.push(label.len() as u8);
        buf.extend_from_slice(label.as_bytes());
    }
    buf.push(0x00); // zero-length label = end
    buf.extend_from_slice(&qtype.to_be_bytes());
    buf.extend_from_slice(&[0x00, 0x01]); // CLASS IN

    buf
}

/// Parse IP addresses from a DNS response's answer section.
#[allow(dead_code)]
pub(crate) fn parse_response(data: &[u8], qtype: u16) -> io::Result<Vec<IpAddress>> {
    if data.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dns response too short",
        ));
    }

    let qdcount = u16::from_be_bytes([data[4], data[5]]);
    let ancount = u16::from_be_bytes([data[6], data[7]]);

    // Skip header (12 bytes) + question section.
    let mut offset = 12;
    for _ in 0..qdcount {
        offset = skip_name(data, offset)?;
        offset += 4; // QTYPE + QCLASS
    }

    let mut ips = Vec::new();
    for _ in 0..ancount {
        offset = skip_name(data, offset)?;
        if offset + 10 > data.len() {
            break;
        }

        let atype = u16::from_be_bytes([data[offset], data[offset + 1]]);
        // skip CLASS (2 bytes) + TTL (4 bytes)
        let rdlength = u16::from_be_bytes([data[offset + 8], data[offset + 9]]) as usize;
        offset += 10;

        if offset + rdlength > data.len() {
            break;
        }

        match (atype, qtype, rdlength) {
            (1, 1, 4) => {
                // A record
                ips.push(IpAddress::V4([
                    data[offset],
                    data[offset + 1],
                    data[offset + 2],
                    data[offset + 3],
                ]));
            }
            (28, 28, 16) => {
                // AAAA record
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&data[offset..offset + 16]);
                ips.push(IpAddress::V6(octets));
            }
            _ => {}
        }

        offset += rdlength;
    }

    Ok(ips)
}

/// Build a DNS response from a raw query and resolved IP addresses.
///
/// Copies the transaction ID and question section from the query,
/// sets the QR (response) flag, and appends answer RRs for each IP.
pub fn build_dns_response(query: &[u8], ips: &[IpAddress]) -> Vec<u8> {
    if query.len() < 12 {
        return Vec::new();
    }

    let qdcount = u16::from_be_bytes([query[4], query[5]]);
    let mut response = Vec::with_capacity(query.len() + ips.len() * 16 + 16);

    // Copy header (12 bytes), then patch flags + counts.
    response.extend_from_slice(&query[..12]);
    // Set QR bit (response) and clear RA/RCODE
    response[2] = 0x81; // QR=1, OPCODE=0, AA=0, TC=0, RD=1
    response[3] = 0x80; // RA=1, Z=0, RCODE=0
                        // ANCOUNT = number of IPs
    let ancount = ips.len() as u16;
    response[6] = (ancount >> 8) as u8;
    response[7] = ancount as u8;
    response[8..12].fill(0); // no authority or additional records are copied

    // Copy question section.
    let mut offset = 12;
    for _ in 0..qdcount {
        if let Ok(end) = skip_name(query, offset) {
            // Copy the name + QTYPE + QCLASS (4 bytes after name)
            let q_end = end + 4;
            if q_end <= query.len() {
                response.extend_from_slice(&query[offset..q_end]);
            }
            offset = q_end;
        } else {
            break;
        }
    }

    // Append answer RRs.
    for ip in ips {
        match ip {
            IpAddress::V4(octets) => {
                // Name pointer (0xc00c → points to offset 12 in DNS message)
                response.extend_from_slice(&[0xc0, 0x0c]);
                response.extend_from_slice(&[0x00, 0x01]); // TYPE A
                response.extend_from_slice(&[0x00, 0x01]); // CLASS IN
                response.extend_from_slice(&[0x00, 0x00, 0x00, 0x3c]); // TTL 60
                response.extend_from_slice(&[0x00, 0x04]); // RDLENGTH 4
                response.extend_from_slice(octets);
            }
            IpAddress::V6(octets) => {
                response.extend_from_slice(&[0xc0, 0x0c]);
                response.extend_from_slice(&[0x00, 0x1c]); // TYPE AAAA
                response.extend_from_slice(&[0x00, 0x01]); // CLASS IN
                response.extend_from_slice(&[0x00, 0x00, 0x00, 0x3c]); // TTL 60
                response.extend_from_slice(&[0x00, 0x10]); // RDLENGTH 16
                response.extend_from_slice(octets);
            }
        }
    }

    response
}

/// The first Internet-class question from a UDP DNS request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsQuestion {
    pub domain: String,
    pub query_type: u16,
}

/// Parse the single question shape accepted by the TUN DNS interceptor.
pub fn parse_dns_question(query: &[u8]) -> io::Result<DnsQuestion> {
    if query.len() < 12 || u16::from_be_bytes([query[4], query[5]]) != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "DNS request must contain exactly one question",
        ));
    }
    let mut offset = 12;
    let mut labels = Vec::new();
    loop {
        let length = *query.get(offset).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated DNS question name")
        })? as usize;
        offset += 1;
        if length == 0 {
            break;
        }
        if length > 63 || offset + length > query.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid DNS question label",
            ));
        }
        let label = std::str::from_utf8(&query[offset..offset + length]).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS question label is not UTF-8",
            )
        })?;
        labels.push(label);
        offset += length;
    }
    if labels.is_empty() || offset + 4 > query.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "incomplete DNS question",
        ));
    }
    let query_type = u16::from_be_bytes([query[offset], query[offset + 1]]);
    let query_class = u16::from_be_bytes([query[offset + 2], query[offset + 3]]);
    if query_class != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "only Internet-class DNS questions are supported",
        ));
    }
    Ok(DnsQuestion {
        domain: labels.join("."),
        query_type,
    })
}

/// Skip a DNS name (possibly compressed) at `offset`.
fn skip_name(data: &[u8], mut offset: usize) -> io::Result<usize> {
    loop {
        if offset >= data.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated name"));
        }
        let len = data[offset];
        if len == 0 {
            return Ok(offset + 1);
        }
        if len & 0xc0 == 0xc0 {
            // Compressed name pointer — skip 2 bytes.
            return Ok(offset + 2);
        }
        offset += 1 + len as usize;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_a_query() {
        let msg = build_query("example.com", 0x0001);
        assert!(msg.len() > 17);
        // Check QTYPE at end: bytes at len-4 and len-3
        let n = msg.len();
        assert_eq!(&msg[n - 4..n - 2], &[0x00, 0x01]); // TYPE A
        assert_eq!(&msg[n - 2..], &[0x00, 0x01]); // CLASS IN
    }

    #[test]
    fn parse_a_response() {
        // Pre-built DNS response for example.com -> 93.184.216.34
        let response = [
            0x00, 0x01, 0x81, 0x80, // ID + flags
            0x00, 0x01, 0x00, 0x01, // QD=1, AN=1
            0x00, 0x00, 0x00, 0x00, // NS=0, AR=0
            // Question: example.com A IN
            0x07, b'e', b'x', b'a', b'm', b'p', b'l', b'e', 0x03, b'c', b'o', b'm', 0x00, 0x00,
            0x01, // TYPE A
            0x00, 0x01, // CLASS IN
            // Answer
            0xc0, 0x0c, // name pointer
            0x00, 0x01, // TYPE A
            0x00, 0x01, // CLASS IN
            0x00, 0x00, 0x0e, 0x10, // TTL 3600
            0x00, 0x04, // RDLENGTH 4
            0x5d, 0xb8, 0xd8, 0x22, // 93.184.216.34
        ];
        let ips = parse_response(&response, 0x0001).unwrap();
        assert_eq!(ips.len(), 1);
        assert_eq!(ips[0], IpAddress::V4([0x5d, 0xb8, 0xd8, 0x22]));
    }

    #[test]
    fn parse_empty_response() {
        let response = [
            0x00, 0x01, 0x81, 0x80, 0x00, 0x01, 0x00, 0x00, // ANCOUNT=0
            0x00, 0x00, 0x00, 0x00, 0x07, b'e', b'x', b'a', b'm', b'p', b'l', b'e', 0x03, b'c',
            b'o', b'm', 0x00, 0x00, 0x01, 0x00, 0x01,
        ];
        let ips = parse_response(&response, 0x0001).unwrap();
        assert!(ips.is_empty());
    }

    #[test]
    fn build_response_for_ipv4() {
        // Query for example.com A record
        let query = build_query("example.com", 0x0001);
        let ips = vec![IpAddress::V4([93, 184, 216, 34])];
        let resp = build_dns_response(&query, &ips);
        assert!(resp.len() > 12);
        // QR bit should be set
        assert_eq!(resp[2] & 0x80, 0x80);
        // ANCOUNT should be 1
        let ancount = u16::from_be_bytes([resp[6], resp[7]]);
        assert_eq!(ancount, 1);
    }

    #[test]
    fn build_response_empty_query() {
        let resp = build_dns_response(&[], &[]);
        assert!(resp.is_empty());
    }
}
