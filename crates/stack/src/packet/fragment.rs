use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use super::checksum;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FragmentKey {
    pub src: IpAddr,
    pub dst: IpAddr,
    pub identification: u32,
    pub next_header: u8,
}

pub struct ParsedIpFragment<'a> {
    pub key: FragmentKey,
    pub offset: usize,
    pub more_fragments: bool,
    pub payload: &'a [u8],
    kind: FragmentKind,
}

#[derive(Clone, Copy)]
enum FragmentKind {
    Ipv4 {
        header_length: usize,
    },
    Ipv6 {
        fragment_header_offset: usize,
        previous_next_header_offset: usize,
    },
}

pub fn parse_ip_fragment(packet: &[u8]) -> Option<ParsedIpFragment<'_>> {
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => parse_ipv4_fragment(packet),
        Some(6) => parse_ipv6_fragment(packet),
        _ => None,
    }
}

fn parse_ipv4_fragment(packet: &[u8]) -> Option<ParsedIpFragment<'_>> {
    if packet.len() < 20 {
        return None;
    }
    let header_length = usize::from(packet[0] & 0x0f) * 4;
    let total_length = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    if header_length < 20 || total_length < header_length || total_length > packet.len() {
        return None;
    }
    let fragment = u16::from_be_bytes([packet[6], packet[7]]);
    if fragment & 0x3fff == 0 {
        return None;
    }
    let src = IpAddr::V4(Ipv4Addr::new(
        packet[12], packet[13], packet[14], packet[15],
    ));
    let dst = IpAddr::V4(Ipv4Addr::new(
        packet[16], packet[17], packet[18], packet[19],
    ));
    Some(ParsedIpFragment {
        key: FragmentKey {
            src,
            dst,
            identification: u32::from(u16::from_be_bytes([packet[4], packet[5]])),
            next_header: packet[9],
        },
        offset: usize::from(fragment & 0x1fff) * 8,
        more_fragments: fragment & 0x2000 != 0,
        payload: &packet[header_length..total_length],
        kind: FragmentKind::Ipv4 { header_length },
    })
}

fn parse_ipv6_fragment(packet: &[u8]) -> Option<ParsedIpFragment<'_>> {
    if packet.len() < 40 {
        return None;
    }
    let payload_length = usize::from(u16::from_be_bytes([packet[4], packet[5]]));
    let end = 40usize.checked_add(payload_length)?;
    if end > packet.len() {
        return None;
    }
    let mut source = [0_u8; 16];
    source.copy_from_slice(&packet[8..24]);
    let mut destination = [0_u8; 16];
    destination.copy_from_slice(&packet[24..40]);

    let mut next_header = packet[6];
    let mut previous_next_header_offset = 6;
    let mut offset = 40;
    for _ in 0..8 {
        match next_header {
            44 => {
                if offset + 8 > end {
                    return None;
                }
                let fragment = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]);
                if fragment & 0xfff9 == 0 {
                    return None;
                }
                let upper_protocol = packet[offset];
                return Some(ParsedIpFragment {
                    key: FragmentKey {
                        src: IpAddr::V6(Ipv6Addr::from(source)),
                        dst: IpAddr::V6(Ipv6Addr::from(destination)),
                        identification: u32::from_be_bytes([
                            packet[offset + 4],
                            packet[offset + 5],
                            packet[offset + 6],
                            packet[offset + 7],
                        ]),
                        next_header: upper_protocol,
                    },
                    offset: usize::from(fragment & 0xfff8),
                    more_fragments: fragment & 1 != 0,
                    payload: &packet[offset + 8..end],
                    kind: FragmentKind::Ipv6 {
                        fragment_header_offset: offset,
                        previous_next_header_offset,
                    },
                });
            }
            0 | 43 | 60 => {
                if offset + 2 > end {
                    return None;
                }
                let length = (usize::from(packet[offset + 1]) + 1) * 8;
                if offset + length > end {
                    return None;
                }
                previous_next_header_offset = offset;
                next_header = packet[offset];
                offset += length;
            }
            51 => {
                if offset + 2 > end {
                    return None;
                }
                let length = (usize::from(packet[offset + 1]) + 2) * 4;
                if offset + length > end {
                    return None;
                }
                previous_next_header_offset = offset;
                next_header = packet[offset];
                offset += length;
            }
            _ => return None,
        }
    }
    None
}

pub fn rebuild_fragmented_packet(first_fragment: &[u8], payload: &[u8]) -> Option<Vec<u8>> {
    let parsed = parse_ip_fragment(first_fragment)?;
    if parsed.offset != 0 {
        return None;
    }
    match parsed.kind {
        FragmentKind::Ipv4 { header_length } => {
            let total_length = header_length.checked_add(payload.len())?;
            let total_length = u16::try_from(total_length).ok()?;
            let mut packet = Vec::with_capacity(usize::from(total_length));
            packet.extend_from_slice(&first_fragment[..header_length]);
            packet.extend_from_slice(payload);
            packet[2..4].copy_from_slice(&total_length.to_be_bytes());
            let flags = u16::from_be_bytes([packet[6], packet[7]]) & 0x4000;
            packet[6..8].copy_from_slice(&flags.to_be_bytes());
            packet[10..12].fill(0);
            let header_checksum = checksum(&packet[..header_length]);
            packet[10..12].copy_from_slice(&header_checksum.to_be_bytes());
            Some(packet)
        }
        FragmentKind::Ipv6 {
            fragment_header_offset,
            previous_next_header_offset,
        } => {
            let total_length = fragment_header_offset.checked_add(payload.len())?;
            let ipv6_payload_length = u16::try_from(total_length.checked_sub(40)?).ok()?;
            let mut packet = Vec::with_capacity(total_length);
            packet.extend_from_slice(&first_fragment[..fragment_header_offset]);
            packet[previous_next_header_offset] = parsed.key.next_header;
            packet.extend_from_slice(payload);
            packet[4..6].copy_from_slice(&ipv6_payload_length.to_be_bytes());
            Some(packet)
        }
    }
}

/// Fragment a complete IP packet at the source. Empty output means the packet
/// is malformed or the requested MTU cannot hold a legal fragment.
pub fn fragment_ip_packet(packet: &[u8], mtu: usize, identification: u32) -> Vec<Vec<u8>> {
    if packet.len() <= mtu {
        return vec![packet.to_vec()];
    }
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => fragment_ipv4(packet, mtu, identification as u16),
        Some(6) => fragment_ipv6(packet, mtu, identification),
        _ => Vec::new(),
    }
}

fn fragment_ipv4(packet: &[u8], mtu: usize, identification: u16) -> Vec<Vec<u8>> {
    if packet.len() < 20 {
        return Vec::new();
    }
    let header_length = usize::from(packet[0] & 0x0f) * 4;
    let total_length = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    if header_length < 20 || total_length > packet.len() || mtu <= header_length {
        return Vec::new();
    }
    let chunk_size = ((mtu - header_length) / 8) * 8;
    if chunk_size == 0 {
        return Vec::new();
    }
    let payload = &packet[header_length..total_length];
    payload
        .chunks(chunk_size)
        .enumerate()
        .map(|(index, chunk)| {
            let offset = index * chunk_size;
            let more = offset + chunk.len() < payload.len();
            let mut fragment = Vec::with_capacity(header_length + chunk.len());
            fragment.extend_from_slice(&packet[..header_length]);
            fragment.extend_from_slice(chunk);
            let length = (header_length + chunk.len()) as u16;
            fragment[2..4].copy_from_slice(&length.to_be_bytes());
            fragment[4..6].copy_from_slice(&identification.to_be_bytes());
            let field = ((offset / 8) as u16) | if more { 0x2000 } else { 0 };
            fragment[6..8].copy_from_slice(&field.to_be_bytes());
            fragment[10..12].fill(0);
            let header_checksum = checksum(&fragment[..header_length]);
            fragment[10..12].copy_from_slice(&header_checksum.to_be_bytes());
            fragment
        })
        .collect()
}

fn fragment_ipv6(packet: &[u8], mtu: usize, identification: u32) -> Vec<Vec<u8>> {
    if packet.len() < 40 || mtu <= 48 {
        return Vec::new();
    }
    let payload_length = usize::from(u16::from_be_bytes([packet[4], packet[5]]));
    let end = 40usize.saturating_add(payload_length);
    if end > packet.len() {
        return Vec::new();
    }
    let chunk_size = ((mtu - 48) / 8) * 8;
    if chunk_size == 0 {
        return Vec::new();
    }
    let upper_protocol = packet[6];
    let payload = &packet[40..end];
    payload
        .chunks(chunk_size)
        .enumerate()
        .map(|(index, chunk)| {
            let offset = index * chunk_size;
            let more = offset + chunk.len() < payload.len();
            let mut fragment = Vec::with_capacity(48 + chunk.len());
            fragment.extend_from_slice(&packet[..40]);
            fragment[6] = 44;
            fragment.extend_from_slice(&[0_u8; 8]);
            fragment[40] = upper_protocol;
            let field = (offset as u16) | u16::from(more);
            fragment[42..44].copy_from_slice(&field.to_be_bytes());
            fragment[44..48].copy_from_slice(&identification.to_be_bytes());
            fragment.extend_from_slice(chunk);
            fragment[4..6].copy_from_slice(&((8 + chunk.len()) as u16).to_be_bytes());
            fragment
        })
        .collect()
}
