//! Pure ICMP error layout and filling, before any payload allocation/admission.
use super::*;
use zero_traits::PacketDeriveError;

pub struct IcmpErrorSpec<'a> {
    quote: &'a [u8],
    source: IpAddr,
    destination: IpAddr,
    prefix: [u8; 8],
    length: usize,
}
impl<'a> IcmpErrorSpec<'a> {
    pub fn for_packet(packet: &'a [u8], mtu: usize, reject_udp: bool) -> Option<Self> {
        let transport = transport_header(packet)?;
        let oversized = packet.len() > mtu;
        if !oversized && matches!(transport.protocol, IPPROTO_TCP | IPPROTO_UDP) && !reject_udp {
            return None;
        }
        let mut prefix = [0u8; 8];
        let quote = match (transport.dst, transport.src) {
            (IpAddr::V4(_), IpAddr::V4(_)) => {
                if transport.protocol == IPPROTO_ICMP
                    && packet.get(transport.offset).is_none_or(|kind| *kind != 8)
                {
                    return None;
                }
                prefix[0] = 3;
                prefix[1] = if oversized { 4 } else { 13 };
                if oversized {
                    prefix[6..8]
                        .copy_from_slice(&(mtu.min(u16::MAX as usize) as u16).to_be_bytes());
                }
                packet
                    .len()
                    .min(mtu.saturating_sub(28))
                    .max(transport.offset.min(packet.len()))
            }
            (IpAddr::V6(_), IpAddr::V6(_)) => {
                if transport.protocol == IPPROTO_ICMPV6
                    && packet.get(transport.offset).is_none_or(|kind| *kind != 128)
                {
                    return None;
                }
                prefix[0] = if oversized { 2 } else { 1 };
                prefix[1] = if oversized { 0 } else { 1 };
                if oversized {
                    prefix[4..8]
                        .copy_from_slice(&(mtu.min(u32::MAX as usize) as u32).to_be_bytes());
                }
                packet.len().min(mtu.saturating_sub(48))
            }
            _ => return None,
        };
        Self::new(&packet[..quote], transport.dst, transport.src, prefix)
    }
    pub fn for_mtu_error(packet: &'a [u8], router: IpAddr, mtu: usize) -> Option<Self> {
        if packet.len() <= mtu || crate::packet::ipv4_fragmentation_allowed(packet) {
            return None;
        }
        let mut spec = Self::for_packet(packet, mtu, false)?;
        if spec.source.is_ipv4() != router.is_ipv4() {
            return None;
        }
        spec.source = router;
        Some(spec)
    }
    pub fn for_time_exceeded(packet: &'a [u8], router: IpAddr, mtu: usize) -> Option<Self> {
        let destination = crate::packet::ip_source(packet)?;
        let mut prefix = [0u8; 8];
        let quote = match (router, destination, packet.first()? >> 4) {
            (IpAddr::V4(_), IpAddr::V4(_), 4) => {
                let header = usize::from(packet[0] & 15) * 4;
                if packet.len() < 20
                    || header < 20
                    || header > packet.len()
                    || checksum(&packet[..header]) != 0
                    || packet[9] == IPPROTO_ICMP
                        && packet
                            .get(header)
                            .is_some_and(|kind| matches!(kind, 3 | 4 | 5 | 11 | 12))
                {
                    return None;
                }
                let quote = packet.len().min(mtu.saturating_sub(28));
                if quote < 20 {
                    return None;
                }
                prefix[0] = 11;
                quote
            }
            (IpAddr::V6(_), IpAddr::V6(_), 6) => {
                if packet.len() < 40
                    || packet[6] == IPPROTO_ICMPV6 && packet.get(40).is_some_and(|kind| *kind < 128)
                {
                    return None;
                }
                let quote = packet.len().min(mtu.saturating_sub(48));
                if quote < 40 {
                    return None;
                }
                prefix[0] = 3;
                quote
            }
            _ => return None,
        };
        Self::new(&packet[..quote], router, destination, prefix)
    }
    fn new(quote: &'a [u8], source: IpAddr, destination: IpAddr, prefix: [u8; 8]) -> Option<Self> {
        let length = quote
            .len()
            .checked_add(if source.is_ipv4() { 28 } else { 48 })?;
        u16::try_from(if source.is_ipv4() {
            length
        } else {
            length - 40
        })
        .ok()?;
        Some(Self {
            quote,
            source,
            destination,
            prefix,
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
        let offset = match (self.source, self.destination) {
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
                output[4..6].copy_from_slice(&((self.length - 40) as u16).to_be_bytes());
                output[6] = IPPROTO_ICMPV6;
                output[7] = 64;
                output[8..24].copy_from_slice(&source.octets());
                output[24..40].copy_from_slice(&destination.octets());
                40
            }
            _ => unreachable!("same validated IP family"),
        };
        output[offset..offset + 8].copy_from_slice(&self.prefix);
        output[offset + 8..].copy_from_slice(self.quote);
        let sum = match (self.source, self.destination) {
            (IpAddr::V4(_), IpAddr::V4(_)) => checksum(&output[offset..]),
            (IpAddr::V6(source), IpAddr::V6(destination)) => {
                icmpv6_checksum(source, destination, &output[offset..])
            }
            _ => unreachable!("same validated IP family"),
        };
        output[offset + 2..offset + 4].copy_from_slice(&sum.to_be_bytes());
        Ok(())
    }
    pub(super) fn allocate(&self) -> Vec<u8> {
        let mut bytes = vec![0; self.length];
        self.write_into(&mut bytes)
            .expect("same validated ICMP error length");
        bytes
    }
}
