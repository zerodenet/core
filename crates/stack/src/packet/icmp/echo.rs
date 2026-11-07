use super::{checksum, icmpv6_checksum, IcmpEchoRequest, IPPROTO_ICMP, IPPROTO_ICMPV6};
use std::net::IpAddr;
use zero_traits::PacketDeriveError;

/// The exact immutable echo image is laid out before any payload allocation.
pub struct IcmpEchoPacketSpec<'a> {
    source: IpAddr,
    destination: IpAddr,
    message: &'a [u8],
    identifier: u16,
    reply: bool,
    length: usize,
}
impl<'a> IcmpEchoPacketSpec<'a> {
    pub fn local(packet: &'a [u8], mtu: usize) -> Option<Self> {
        let request = super::parse_icmp_echo_request(packet)?;
        let unicast = |ip: IpAddr| match ip {
            IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast(),
            IpAddr::V6(ip) => !ip.is_unspecified() && !ip.is_multicast(),
        };
        if !unicast(request.source) || !unicast(request.destination) {
            return None;
        }
        Self::new(
            request.destination,
            request.source,
            request.message,
            u16::from_be_bytes(request.message[4..6].try_into().ok()?),
            true,
            mtu,
        )
    }
    pub fn tunnel_probe(
        request: &IcmpEchoRequest<'a>,
        id: u16,
        local: IpAddr,
        mtu: usize,
    ) -> Option<Self> {
        Self::new(local, request.destination, request.message, id, false, mtu)
    }
    pub fn probe_reply(
        request: &IcmpEchoRequest<'_>,
        id: u16,
        reply: &'a [u8],
        local: IpAddr,
        mtu: usize,
    ) -> Option<Self> {
        if reply.len() < 8
            || reply[1] != 0
            || reply[4..6] != id.to_be_bytes()
            || reply[6..8] != request.message[6..8]
            || reply[8..] != request.message[8..]
        {
            return None;
        }
        match (request.source, request.destination, local, reply[0]) {
            (IpAddr::V4(_), IpAddr::V4(_), IpAddr::V4(_), 0) if checksum(reply) == 0 => {}
            (IpAddr::V6(_), IpAddr::V6(destination), IpAddr::V6(local), 129)
                if icmpv6_checksum(destination, local, reply) == 0 => {}
            _ => return None,
        }
        Self::new(
            request.destination,
            request.source,
            reply,
            u16::from_be_bytes(request.message[4..6].try_into().ok()?),
            true,
            mtu,
        )
    }
    fn new(
        source: IpAddr,
        destination: IpAddr,
        message: &'a [u8],
        identifier: u16,
        reply: bool,
        mtu: usize,
    ) -> Option<Self> {
        let header: usize = match (source, destination) {
            (IpAddr::V4(_), IpAddr::V4(_)) => 20,
            (IpAddr::V6(_), IpAddr::V6(_)) => 40,
            _ => return None,
        };
        let length = header.checked_add(message.len())?;
        if message.len() < 8 || length > mtu || length > usize::from(u16::MAX) {
            return None;
        }
        Some(Self {
            source,
            destination,
            message,
            identifier,
            reply,
            length,
        })
    }
    pub fn length(&self) -> usize {
        self.length
    }
    pub fn write_into(&self, output: &mut [u8]) -> Result<(), PacketDeriveError> {
        if output.len() != self.length {
            return Err(PacketDeriveError::LayoutMismatch);
        }
        output.fill(0);
        let header = match (self.source, self.destination) {
            (IpAddr::V4(source), IpAddr::V4(destination)) => {
                output[0] = 0x45;
                output[2..4].copy_from_slice(&(self.length as u16).to_be_bytes());
                output[8] = 64;
                output[9] = IPPROTO_ICMP;
                output[12..16].copy_from_slice(&source.octets());
                output[16..20].copy_from_slice(&destination.octets());
                let sum = checksum(&output[..20]);
                output[10..12].copy_from_slice(&sum.to_be_bytes());
                20
            }
            (IpAddr::V6(source), IpAddr::V6(destination)) => {
                output[0] = 0x60;
                output[4..6].copy_from_slice(&(self.message.len() as u16).to_be_bytes());
                output[6] = IPPROTO_ICMPV6;
                output[7] = 64;
                output[8..24].copy_from_slice(&source.octets());
                output[24..40].copy_from_slice(&destination.octets());
                40
            }
            _ => unreachable!("validated echo family"),
        };
        let message = &mut output[header..];
        message.copy_from_slice(self.message);
        message[0] = match (self.source.is_ipv4(), self.reply) {
            (true, true) => 0,
            (true, false) => 8,
            (false, true) => 129,
            (false, false) => 128,
        };
        message[4..6].copy_from_slice(&self.identifier.to_be_bytes());
        message[2..4].fill(0);
        let sum = match (self.source, self.destination) {
            (IpAddr::V4(_), IpAddr::V4(_)) => checksum(message),
            (IpAddr::V6(source), IpAddr::V6(destination)) => {
                icmpv6_checksum(source, destination, message)
            }
            _ => unreachable!("same echo family"),
        };
        message[2..4].copy_from_slice(&sum.to_be_bytes());
        Ok(())
    }
    fn allocate(&self) -> Vec<u8> {
        let mut output = vec![0; self.length];
        self.write_into(&mut output).expect("same echo length");
        output
    }
}
/// Answer a validated Echo request at an explicitly assigned local IP.
pub fn build_local_icmp_echo_reply(packet: &[u8], mtu: usize) -> Option<Vec<u8>> {
    Some(IcmpEchoPacketSpec::local(packet, mtu)?.allocate())
}
pub fn write_icmp_echo_probe(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    local: IpAddr,
    output: &mut [u8],
) -> Option<()> {
    if output.len() != request.message.len() {
        return None;
    }
    output.copy_from_slice(request.message);
    output[4..6].copy_from_slice(&probe_id.to_be_bytes());
    output[2..4].fill(0);
    let sum = match (local, request.destination) {
        (IpAddr::V4(_), IpAddr::V4(_)) => checksum(output),
        (IpAddr::V6(local), IpAddr::V6(destination)) => icmpv6_checksum(local, destination, output),
        _ => return None,
    };
    output[2..4].copy_from_slice(&sum.to_be_bytes());
    Some(())
}
pub fn build_icmp_echo_probe(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    local: IpAddr,
) -> Option<Vec<u8>> {
    let mut output = vec![0; request.message.len()];
    write_icmp_echo_probe(request, probe_id, local, &mut output)?;
    Some(output)
}
pub fn build_icmp_echo_tunnel_probe(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    local: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    Some(IcmpEchoPacketSpec::tunnel_probe(request, probe_id, local, mtu)?.allocate())
}
pub fn build_icmp_echo_reply(
    request: &IcmpEchoRequest<'_>,
    probe_id: u16,
    reply: &[u8],
    local: IpAddr,
    mtu: usize,
) -> Option<Vec<u8>> {
    Some(IcmpEchoPacketSpec::probe_reply(request, probe_id, reply, local, mtu)?.allocate())
}
