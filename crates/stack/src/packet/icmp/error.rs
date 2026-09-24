//! Parse ICMP errors that quote a client-originated TCP/UDP packet.
//! Quoted IP headers are intentionally parsed separately: an ICMP quote may
//! contain only the first transport bytes, not the original full IP length.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::icmpv6_checksum;
use crate::packet::{
    checksum, transport_header, Endpoint, IPPROTO_ICMP, IPPROTO_ICMPV6, IPPROTO_TCP, IPPROTO_UDP,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IcmpErrorKind {
    DestinationUnreachable { code: u8 },
    PacketTooBig { mtu: Option<u32> },
    TimeExceeded { code: u8 },
    ParameterProblem { code: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedIcmpError {
    pub kind: IcmpErrorKind,
    pub quoted_source: Endpoint,
    pub quoted_destination: Endpoint,
    pub quoted_protocol: u8,
}

/// Only accept a complete, checksum-valid ICMP error that quotes one of this
/// stack's TCP/UDP packets and is addressed back to its local source IP.
pub fn parse_icmp_error(packet: &[u8]) -> Option<ParsedIcmpError> {
    let outer = transport_header(packet)?;
    let icmp = packet.get(outer.offset..outer.end)?;
    if icmp.len() < 8 {
        return None;
    }
    let kind = match (outer.src, outer.dst, outer.protocol) {
        (IpAddr::V4(_), IpAddr::V4(_), IPPROTO_ICMP) => {
            if checksum(icmp) != 0 {
                return None;
            }
            match icmp[0] {
                3 if icmp[1] == 4 => IcmpErrorKind::PacketTooBig {
                    mtu: nonzero(u16::from_be_bytes([icmp[6], icmp[7]]) as u32),
                },
                3 => IcmpErrorKind::DestinationUnreachable { code: icmp[1] },
                11 => IcmpErrorKind::TimeExceeded { code: icmp[1] },
                12 => IcmpErrorKind::ParameterProblem { code: icmp[1] },
                _ => return None,
            }
        }
        (IpAddr::V6(source), IpAddr::V6(destination), IPPROTO_ICMPV6) => {
            if icmpv6_checksum(source, destination, icmp) != 0 {
                return None;
            }
            match icmp[0] {
                1 => IcmpErrorKind::DestinationUnreachable { code: icmp[1] },
                2 => IcmpErrorKind::PacketTooBig {
                    mtu: nonzero(u32::from_be_bytes([icmp[4], icmp[5], icmp[6], icmp[7]])),
                },
                3 => IcmpErrorKind::TimeExceeded { code: icmp[1] },
                4 => IcmpErrorKind::ParameterProblem { code: icmp[1] },
                _ => return None,
            }
        }
        _ => return None,
    };
    let (quoted_source, quoted_destination, quoted_protocol) = quoted_transport(&icmp[8..])?;
    if quoted_source.ip != outer.dst {
        return None;
    }
    Some(ParsedIcmpError {
        kind,
        quoted_source,
        quoted_destination,
        quoted_protocol,
    })
}

fn nonzero(value: u32) -> Option<u32> {
    (value != 0).then_some(value)
}

fn quoted_transport(quote: &[u8]) -> Option<(Endpoint, Endpoint, u8)> {
    let (source, destination, protocol, offset) = match quote.first().map(|byte| byte >> 4) {
        Some(4) => {
            if quote.len() < 24 {
                return None;
            }
            let header_len = usize::from(quote[0] & 0x0f) * 4;
            if header_len < 20 || quote.len() < header_len + 4 {
                return None;
            }
            let fragment = u16::from_be_bytes([quote[6], quote[7]]);
            if fragment & 0x1fff != 0 {
                return None;
            }
            (
                IpAddr::V4(Ipv4Addr::new(quote[12], quote[13], quote[14], quote[15])),
                IpAddr::V4(Ipv4Addr::new(quote[16], quote[17], quote[18], quote[19])),
                quote[9],
                header_len,
            )
        }
        Some(6) => {
            if quote.len() < 44 {
                return None;
            }
            let source = Ipv6Addr::from(<[u8; 16]>::try_from(&quote[8..24]).ok()?);
            let destination = Ipv6Addr::from(<[u8; 16]>::try_from(&quote[24..40]).ok()?);
            (IpAddr::V6(source), IpAddr::V6(destination), quote[6], 40)
        }
        _ => return None,
    };
    if !matches!(protocol, IPPROTO_TCP | IPPROTO_UDP) {
        return None;
    }
    let ports = quote.get(offset..offset + 4)?;
    Some((
        Endpoint {
            ip: source,
            port: u16::from_be_bytes([ports[0], ports[1]]),
        },
        Endpoint {
            ip: destination,
            port: u16::from_be_bytes([ports[2], ports[3]]),
        },
        protocol,
    ))
}
