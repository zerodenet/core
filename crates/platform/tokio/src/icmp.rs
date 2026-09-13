//! OS raw ICMP I/O. Echo framing, identifiers and polling belong to the carrier.
use crate::EgressInterface;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    net::{IpAddr, SocketAddr},
};
pub struct IcmpSocket {
    socket: tokio::net::UdpSocket,
    ipv6: bool,
}
impl IcmpSocket {
    pub fn bind(address: IpAddr, egress: Option<&EgressInterface>) -> io::Result<Self> {
        let ipv6 = address.is_ipv6();
        let socket = Socket::new(
            if ipv6 { Domain::IPV6 } else { Domain::IPV4 },
            Type::RAW,
            Some(if ipv6 {
                Protocol::ICMPV6
            } else {
                Protocol::ICMPV4
            }),
        )?;
        socket.bind(&SocketAddr::new(address, 0).into())?;
        socket.set_nonblocking(true)?;
        let socket: std::net::UdpSocket = socket.into();
        if let Some(interface) = egress {
            crate::egress::bind_udp_to_interface(&socket, SocketAddr::new(address, 0), interface)?;
        }
        Ok(Self {
            socket: tokio::net::UdpSocket::from_std(socket)?,
            ipv6,
        })
    }
    pub fn local_addr(&self) -> io::Result<IpAddr> {
        self.socket.local_addr().map(|addr| addr.ip())
    }
    pub async fn send_to(&self, bytes: &[u8], peer: IpAddr) -> io::Result<usize> {
        if peer.is_ipv6() != self.ipv6 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ICMP address family mismatch",
            ));
        }
        self.socket.send_to(bytes, SocketAddr::new(peer, 0)).await
    }
    pub async fn recv_from(&self, bytes: &mut [u8]) -> io::Result<(usize, IpAddr)> {
        loop {
            let (n, peer) = self.socket.recv_from(bytes).await?;
            // Raw IPv4 sockets include an IP header on supported OSes. IPv6
            // raw ICMP sockets expose the ICMP message directly.
            if !self.ipv6 && n > 0 && bytes[0] >> 4 == 4 {
                let header = usize::from(bytes[0] & 15) * 4;
                if header < 20 || header > n || bytes[9] != 1 {
                    continue;
                }
                bytes.copy_within(header..n, 0);
                return Ok((n - header, peer.ip()));
            }
            return Ok((n, peer.ip()));
        }
    }
}
