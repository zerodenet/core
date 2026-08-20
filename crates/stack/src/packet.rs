//! Raw IP/TCP/UDP packet parsing, building, and checksums.
//!
//! All functions operate on byte slices and are pure (no I/O).
//! Used by both the TCP and UDP stack implementations.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

// ── Protocol numbers ──────────────────────────────────────────────────

pub const IPPROTO_TCP: u8 = 6;
pub const IPPROTO_UDP: u8 = 17;

// ── Endpoint types ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Endpoint {
    pub ip: IpAddr,
    pub port: u16,
}

// ── Parsed packet types ───────────────────────────────────────────────

pub struct ParsedTcp<'a> {
    pub src: Endpoint,
    pub dst: Endpoint,
    pub seq: u32,
    pub ack: u32,
    pub syn: bool,
    pub ack_flag: bool,
    pub fin: bool,
    pub rst: bool,
    pub psh: bool,
    pub data_off: u16,
    pub payload: &'a [u8],
}

pub struct ParsedUdp<'a> {
    pub src: Endpoint,
    pub dst: Endpoint,
    pub payload: &'a [u8],
}

// ── Parsing ───────────────────────────────────────────────────────────

/// Determine the transport protocol of a raw IP packet.
pub fn ip_protocol(packet: &[u8]) -> Option<u8> {
    transport_header(packet).map(|header| header.protocol)
}

/// Return the advertised TCP receive window from a complete IP packet.
pub fn tcp_window(packet: &[u8]) -> Option<u16> {
    let transport = transport_header(packet)?;
    if transport.protocol != IPPROTO_TCP || transport.end < transport.offset + 20 {
        return None;
    }
    Some(u16::from_be_bytes([
        packet[transport.offset + 14],
        packet[transport.offset + 15],
    ]))
}

/// Return the MSS advertised in a TCP packet, when present.
pub fn tcp_mss(packet: &[u8]) -> Option<u16> {
    let transport = transport_header(packet)?;
    if transport.protocol != IPPROTO_TCP || transport.end < transport.offset + 20 {
        return None;
    }
    let data_offset = usize::from((packet[transport.offset + 12] >> 4) & 0x0f) * 4;
    if data_offset < 20 || transport.offset + data_offset > transport.end {
        return None;
    }
    parse_tcp_mss(&packet[transport.offset + 20..transport.offset + data_offset])
}

/// Parse an IPv4/IPv6 + TCP packet.
pub fn parse_tcp(packet: &[u8]) -> Option<ParsedTcp<'_>> {
    let transport = transport_header(packet)?;
    if transport.protocol != IPPROTO_TCP || transport.end < transport.offset + 20 {
        return None;
    }
    let h = &packet[transport.offset..];
    let src_port = u16::from_be_bytes([h[0], h[1]]);
    let dst_port = u16::from_be_bytes([h[2], h[3]]);
    let seq = u16::from_be_bytes([h[4], h[5]]);
    let seq = (seq as u32) << 16 | u16::from_be_bytes([h[6], h[7]]) as u32;
    let ack = u16::from_be_bytes([h[8], h[9]]);
    let ack = (ack as u32) << 16 | u16::from_be_bytes([h[10], h[11]]) as u32;
    let data_off = ((h[12] >> 4) & 0x0f) as u16 * 4;
    if data_off < 20 || transport.offset + data_off as usize > transport.end {
        return None;
    }
    let flags = h[13];
    let payload_start = transport.offset + data_off as usize;
    let payload = &packet[payload_start..transport.end];

    Some(ParsedTcp {
        src: Endpoint {
            ip: transport.src,
            port: src_port,
        },
        dst: Endpoint {
            ip: transport.dst,
            port: dst_port,
        },
        seq,
        ack,
        syn: (flags & 0x02) != 0,
        ack_flag: (flags & 0x10) != 0,
        fin: (flags & 0x01) != 0,
        rst: (flags & 0x04) != 0,
        psh: (flags & 0x08) != 0,
        data_off,
        payload,
    })
}

fn parse_tcp_mss(options: &[u8]) -> Option<u16> {
    let mut offset = 0;
    while offset < options.len() {
        match options[offset] {
            0 => break,
            1 => offset += 1,
            2 if offset + 4 <= options.len() && options[offset + 1] == 4 => {
                return Some(u16::from_be_bytes([
                    options[offset + 2],
                    options[offset + 3],
                ]));
            }
            _ if offset + 2 <= options.len() => {
                let length = options[offset + 1] as usize;
                if length < 2 || offset + length > options.len() {
                    break;
                }
                offset += length;
            }
            _ => break,
        }
    }
    None
}

/// Parse an IPv4/IPv6 + UDP packet.
pub fn parse_udp(packet: &[u8]) -> Option<ParsedUdp<'_>> {
    let transport = transport_header(packet)?;
    if transport.protocol != IPPROTO_UDP {
        return None;
    }
    if transport.end < transport.offset + 8 {
        return None;
    }
    let h = &packet[transport.offset..];
    let src_port = u16::from_be_bytes([h[0], h[1]]);
    let dst_port = u16::from_be_bytes([h[2], h[3]]);
    let udp_len = u16::from_be_bytes([h[4], h[5]]) as usize;
    if udp_len < 8 || transport.offset + udp_len > transport.end {
        return None;
    }
    let payload_start = transport.offset + 8;
    let payload_end = (transport.offset + udp_len).min(transport.end);
    let payload = &packet[payload_start..payload_end];

    Some(ParsedUdp {
        src: Endpoint {
            ip: transport.src,
            port: src_port,
        },
        dst: Endpoint {
            ip: transport.dst,
            port: dst_port,
        },
        payload,
    })
}

/// Parse IP header, returning (src, dst, header_len, total_len).
fn parse_ip(packet: &[u8]) -> Option<(IpAddr, IpAddr, usize, Option<usize>)> {
    if packet.is_empty() {
        return None;
    }
    match packet[0] >> 4 {
        4 => {
            if packet.len() < 20 {
                return None;
            }
            let ihl = (packet[0] & 0x0f) as usize * 4;
            let total = u16::from_be_bytes([packet[2], packet[3]]) as usize;
            if ihl < 20 || ihl > packet.len() || total < ihl || total > packet.len() {
                return None;
            }
            let src = IpAddr::V4(Ipv4Addr::new(
                packet[12], packet[13], packet[14], packet[15],
            ));
            let dst = IpAddr::V4(Ipv4Addr::new(
                packet[16], packet[17], packet[18], packet[19],
            ));
            Some((src, dst, ihl, Some(total)))
        }
        6 => {
            if packet.len() < 40 {
                return None;
            }
            let mut s = [0u8; 16];
            s.copy_from_slice(&packet[8..24]);
            let mut d = [0u8; 16];
            d.copy_from_slice(&packet[24..40]);
            let src = IpAddr::V6(Ipv6Addr::from(s));
            let dst = IpAddr::V6(Ipv6Addr::from(d));
            let payload_len = u16::from_be_bytes([packet[4], packet[5]]) as usize;
            if 40 + payload_len > packet.len() {
                return None;
            }
            Some((src, dst, 40, Some(40 + payload_len)))
        }
        _ => None,
    }
}

struct TransportHeader {
    src: IpAddr,
    dst: IpAddr,
    offset: usize,
    end: usize,
    protocol: u8,
}

fn transport_header(packet: &[u8]) -> Option<TransportHeader> {
    let (src, dst, ip_header_length, total_length) = parse_ip(packet)?;
    let end = total_length.unwrap_or(packet.len()).min(packet.len());
    if src.is_ipv4() {
        let fragment = u16::from_be_bytes([packet[6], packet[7]]);
        if fragment & 0x3fff != 0 {
            return None;
        }
        return Some(TransportHeader {
            src,
            dst,
            offset: ip_header_length,
            end,
            protocol: packet[9],
        });
    }

    let mut protocol = packet[6];
    let mut offset = ip_header_length;
    for _ in 0..8 {
        let extension_length = match protocol {
            // Hop-by-hop, routing, and destination options.
            0 | 43 | 60 => {
                if offset + 2 > end {
                    return None;
                }
                (usize::from(packet[offset + 1]) + 1) * 8
            }
            // Fragment header. Atomic fragments can be inspected without reassembly.
            44 => {
                if offset + 8 > end {
                    return None;
                }
                let fragment = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]);
                if fragment & 0xfff9 != 0 {
                    return None;
                }
                8
            }
            // Authentication header.
            51 => {
                if offset + 2 > end {
                    return None;
                }
                (usize::from(packet[offset + 1]) + 2) * 4
            }
            // ESP payload cannot be inspected by this stack.
            50 => return None,
            _ => {
                return Some(TransportHeader {
                    src,
                    dst,
                    offset,
                    end,
                    protocol,
                });
            }
        };
        if extension_length == 0 || offset + extension_length > end {
            return None;
        }
        protocol = packet[offset];
        offset += extension_length;
    }
    None
}

// ── Building ──────────────────────────────────────────────────────────

/// TCP flags.
pub mod tcp_flags {
    pub const FIN: u8 = 0x01;
    pub const SYN: u8 = 0x02;
    pub const RST: u8 = 0x04;
    pub const PSH: u8 = 0x08;
    pub const ACK: u8 = 0x10;
}

/// Build an IPv4/IPv6 + TCP packet with correct checksums.
#[allow(clippy::too_many_arguments)]
pub fn build_tcp(
    src: IpAddr,
    dst: IpAddr,
    sport: u16,
    dport: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    match (src, dst) {
        (IpAddr::V4(s), IpAddr::V4(d)) => {
            build_tcp_v4(s, d, sport, dport, seq, ack, flags, payload)
        }
        (IpAddr::V6(s), IpAddr::V6(d)) => {
            build_tcp_v6(s, d, sport, dport, seq, ack, flags, payload)
        }
        _ => Vec::new(),
    }
}

/// Build an IPv4/IPv6 + TCP packet with MSS option (for SYN-ACK).
///
/// Adds a single TCP option: MSS (kind=2, length=4, value=mss).
/// The TCP data offset is adjusted to 24 bytes (20 base + 4 option).
#[allow(clippy::too_many_arguments)]
pub fn build_tcp_with_mss(
    src: IpAddr,
    dst: IpAddr,
    sport: u16,
    dport: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    mss: u16,
) -> Vec<u8> {
    match (src, dst) {
        (IpAddr::V4(s), IpAddr::V4(d)) => {
            build_tcp_v4_with_mss(s, d, sport, dport, seq, ack, flags, mss)
        }
        (IpAddr::V6(s), IpAddr::V6(d)) => {
            build_tcp_v6_with_mss(s, d, sport, dport, seq, ack, flags, mss)
        }
        _ => Vec::new(),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_tcp_v4_with_mss(
    src: Ipv4Addr,
    dst: Ipv4Addr,
    sport: u16,
    dport: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    mss: u16,
) -> Vec<u8> {
    // TCP header = 20 base + 4 MSS option = 24 bytes. No payload in SYN-ACK.
    let tcp_hdr_len: usize = 24;
    let total = 20 + tcp_hdr_len;
    let mut p = vec![0u8; total];

    // IP header.
    p[0] = 0x45;
    p[2] = (total >> 8) as u8;
    p[3] = total as u8;
    p[8] = 64;
    p[9] = IPPROTO_TCP;
    p[12..16].copy_from_slice(&src.octets());
    p[16..20].copy_from_slice(&dst.octets());
    let ip_cksum = checksum(&p[0..20]);
    p[10] = (ip_cksum >> 8) as u8;
    p[11] = ip_cksum as u8;

    // TCP header.
    let o = 20;
    p[o..o + 2].copy_from_slice(&sport.to_be_bytes());
    p[o + 2..o + 4].copy_from_slice(&dport.to_be_bytes());
    p[o + 4..o + 8].copy_from_slice(&seq.to_be_bytes());
    p[o + 8..o + 12].copy_from_slice(&ack.to_be_bytes());
    p[o + 12] = ((tcp_hdr_len as u8) / 4) << 4; // data offset = 6 (24/4)
    p[o + 13] = flags;
    p[o + 14..o + 16].copy_from_slice(&65535u16.to_be_bytes()); // window

    // MSS option: kind=2, len=4, value=mss (big-endian).
    p[o + 20] = 2; // kind
    p[o + 21] = 4; // length
    p[o + 22..o + 24].copy_from_slice(&mss.to_be_bytes());

    let tcp_cksum = tcp_checksum_v4(&src, &dst, &p[o..]);
    p[o + 16] = (tcp_cksum >> 8) as u8;
    p[o + 17] = tcp_cksum as u8;

    p
}

#[allow(clippy::too_many_arguments)]
fn build_tcp_v6_with_mss(
    src: Ipv6Addr,
    dst: Ipv6Addr,
    sport: u16,
    dport: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    mss: u16,
) -> Vec<u8> {
    let tcp_hdr_len: usize = 24;
    let total = 40 + tcp_hdr_len;
    let mut p = vec![0u8; total];

    // IPv6 header.
    p[0] = 0x60;
    p[4..6].copy_from_slice(&(tcp_hdr_len as u16).to_be_bytes());
    p[6] = IPPROTO_TCP;
    p[7] = 64;
    p[8..24].copy_from_slice(&src.octets());
    p[24..40].copy_from_slice(&dst.octets());

    // TCP header.
    let o = 40;
    p[o..o + 2].copy_from_slice(&sport.to_be_bytes());
    p[o + 2..o + 4].copy_from_slice(&dport.to_be_bytes());
    p[o + 4..o + 8].copy_from_slice(&seq.to_be_bytes());
    p[o + 8..o + 12].copy_from_slice(&ack.to_be_bytes());
    p[o + 12] = ((tcp_hdr_len as u8) / 4) << 4;
    p[o + 13] = flags;
    p[o + 14..o + 16].copy_from_slice(&65535u16.to_be_bytes());

    // MSS option.
    p[o + 20] = 2;
    p[o + 21] = 4;
    p[o + 22..o + 24].copy_from_slice(&mss.to_be_bytes());

    let tcp_cksum = tcp_checksum_v6(&src, &dst, &p[o..]);
    p[o + 16] = (tcp_cksum >> 8) as u8;
    p[o + 17] = tcp_cksum as u8;

    p
}

#[allow(clippy::too_many_arguments)]
fn build_tcp_v4(
    src: Ipv4Addr,
    dst: Ipv4Addr,
    sport: u16,
    dport: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let tcp_len = 20 + payload.len();
    let total = 20 + tcp_len;
    let mut p = vec![0u8; total];

    // IP header
    p[0] = 0x45;
    p[2] = (total >> 8) as u8;
    p[3] = total as u8;
    p[8] = 64; // TTL
    p[9] = IPPROTO_TCP;
    p[12..16].copy_from_slice(&src.octets());
    p[16..20].copy_from_slice(&dst.octets());
    let ip_cksum = checksum(&p[0..20]);
    p[10] = (ip_cksum >> 8) as u8;
    p[11] = ip_cksum as u8;

    // TCP header
    p[20..22].copy_from_slice(&sport.to_be_bytes());
    p[22..24].copy_from_slice(&dport.to_be_bytes());
    p[24..28].copy_from_slice(&seq.to_be_bytes());
    p[28..32].copy_from_slice(&ack.to_be_bytes());
    p[32] = 0x50; // data offset = 5 (20 bytes)
    p[33] = flags;
    p[34..36].copy_from_slice(&65535u16.to_be_bytes()); // window
                                                        // checksum at [36..38] — filled below

    if !payload.is_empty() {
        p[40..].copy_from_slice(payload);
    }

    let tcp_cksum = tcp_checksum_v4(&src, &dst, &p[20..]);
    p[36] = (tcp_cksum >> 8) as u8;
    p[37] = tcp_cksum as u8;

    p
}

#[allow(clippy::too_many_arguments)]
fn build_tcp_v6(
    src: Ipv6Addr,
    dst: Ipv6Addr,
    sport: u16,
    dport: u16,
    seq: u32,
    ack: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let tcp_len = 20 + payload.len();
    let total = 40 + tcp_len;
    let mut p = vec![0u8; total];

    // IPv6 header
    p[0] = 0x60;
    p[4..6].copy_from_slice(&(tcp_len as u16).to_be_bytes());
    p[6] = IPPROTO_TCP;
    p[7] = 64; // hop limit
    p[8..24].copy_from_slice(&src.octets());
    p[24..40].copy_from_slice(&dst.octets());

    // TCP header
    let o = 40;
    p[o..o + 2].copy_from_slice(&sport.to_be_bytes());
    p[o + 2..o + 4].copy_from_slice(&dport.to_be_bytes());
    p[o + 4..o + 8].copy_from_slice(&seq.to_be_bytes());
    p[o + 8..o + 12].copy_from_slice(&ack.to_be_bytes());
    p[o + 12] = 0x50; // data offset = 5
    p[o + 13] = flags;
    p[o + 14..o + 16].copy_from_slice(&65535u16.to_be_bytes()); // window
                                                                // checksum at [o+16..o+18] — filled below

    if !payload.is_empty() {
        p[o + 20..].copy_from_slice(payload);
    }

    let tcp_cksum = tcp_checksum_v6(&src, &dst, &p[o..]);
    p[o + 16] = (tcp_cksum >> 8) as u8;
    p[o + 17] = tcp_cksum as u8;

    p
}

/// Build an IPv4/IPv6 + UDP packet with correct checksums.
pub fn build_udp(src: IpAddr, dst: IpAddr, sport: u16, dport: u16, payload: &[u8]) -> Vec<u8> {
    match (src, dst) {
        (IpAddr::V4(s), IpAddr::V4(d)) => build_udp_v4(s, d, sport, dport, payload),
        (IpAddr::V6(s), IpAddr::V6(d)) => build_udp_v6(s, d, sport, dport, payload),
        _ => Vec::new(),
    }
}

fn build_udp_v4(src: Ipv4Addr, dst: Ipv4Addr, sport: u16, dport: u16, payload: &[u8]) -> Vec<u8> {
    let udp_total = 8 + payload.len();
    let total = 20 + udp_total;
    let mut p = vec![0u8; total];

    // IP header
    p[0] = 0x45;
    p[2] = (total >> 8) as u8;
    p[3] = total as u8;
    p[8] = 64;
    p[9] = IPPROTO_UDP;
    p[12..16].copy_from_slice(&src.octets());
    p[16..20].copy_from_slice(&dst.octets());
    let ip_cksum = checksum(&p[0..20]);
    p[10] = (ip_cksum >> 8) as u8;
    p[11] = ip_cksum as u8;

    // UDP header
    p[20..22].copy_from_slice(&sport.to_be_bytes());
    p[22..24].copy_from_slice(&dport.to_be_bytes());
    p[24..26].copy_from_slice(&(udp_total as u16).to_be_bytes());
    // checksum at [26..28] — filled below

    if !payload.is_empty() {
        p[28..].copy_from_slice(payload);
    }

    let udp_cksum = udp_checksum_v4(&src, &dst, &p[20..]);
    p[26] = (udp_cksum >> 8) as u8;
    p[27] = udp_cksum as u8;

    p
}

fn build_udp_v6(src: Ipv6Addr, dst: Ipv6Addr, sport: u16, dport: u16, payload: &[u8]) -> Vec<u8> {
    let udp_total = 8 + payload.len();
    let total = 40 + udp_total;
    let mut p = vec![0u8; total];

    // IPv6 header
    p[0] = 0x60;
    p[4..6].copy_from_slice(&(udp_total as u16).to_be_bytes());
    p[6] = IPPROTO_UDP;
    p[7] = 64;
    p[8..24].copy_from_slice(&src.octets());
    p[24..40].copy_from_slice(&dst.octets());

    // UDP header
    let o = 40;
    p[o..o + 2].copy_from_slice(&sport.to_be_bytes());
    p[o + 2..o + 4].copy_from_slice(&dport.to_be_bytes());
    p[o + 4..o + 6].copy_from_slice(&(udp_total as u16).to_be_bytes());
    // checksum at [o+6..o+8] — filled below

    if !payload.is_empty() {
        p[o + 8..].copy_from_slice(payload);
    }

    let udp_cksum = udp_checksum_v6(&src, &dst, &p[o..]);
    p[o + 6] = (udp_cksum >> 8) as u8;
    p[o + 7] = udp_cksum as u8;

    p
}

// ── Checksums ─────────────────────────────────────────────────────────

/// RFC 1071 ones' complement checksum over a byte slice.
/// Returns the checksum as a host `u16`; callers serialize it with
/// `u16::to_be_bytes` (or equivalent high/low-byte writes).
pub fn checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let len = data.len();
    let words = len / 2;
    for i in 0..words {
        sum += u16::from_be_bytes([data[i * 2], data[i * 2 + 1]]) as u32;
    }
    if !len.is_multiple_of(2) {
        sum += (data[len - 1] as u32) << 8;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// TCP checksum with IPv4 pseudo-header.
fn tcp_checksum_v4(src: &Ipv4Addr, dst: &Ipv4Addr, tcp: &[u8]) -> u16 {
    let tcp_len = tcp.len() as u32;
    let mut pseudo = Vec::with_capacity(12 + tcp.len());
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.push(0); // reserved
    pseudo.push(IPPROTO_TCP);
    pseudo.extend_from_slice(&(tcp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(tcp);
    checksum(&pseudo)
}

/// TCP checksum with IPv6 pseudo-header.
fn tcp_checksum_v6(src: &Ipv6Addr, dst: &Ipv6Addr, tcp: &[u8]) -> u16 {
    let tcp_len = tcp.len() as u32;
    let mut pseudo = Vec::with_capacity(40 + tcp.len());
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.extend_from_slice(&tcp_len.to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0]);
    pseudo.push(IPPROTO_TCP);
    pseudo.extend_from_slice(tcp);
    checksum(&pseudo)
}

/// UDP checksum with IPv4 pseudo-header.
/// Returns 0xFFFF when the computed checksum is zero (RFC 768).
fn udp_checksum_v4(src: &Ipv4Addr, dst: &Ipv4Addr, udp: &[u8]) -> u16 {
    let udp_len = udp.len() as u32;
    let mut pseudo = Vec::with_capacity(12 + udp.len());
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.push(0);
    pseudo.push(IPPROTO_UDP);
    pseudo.extend_from_slice(&(udp_len as u16).to_be_bytes());
    pseudo.extend_from_slice(udp);
    let c = checksum(&pseudo);
    if c == 0 {
        0xFFFF
    } else {
        c
    }
}

/// UDP checksum with IPv6 pseudo-header.
fn udp_checksum_v6(src: &Ipv6Addr, dst: &Ipv6Addr, udp: &[u8]) -> u16 {
    let udp_len = udp.len() as u32;
    let mut pseudo = Vec::with_capacity(40 + udp.len());
    pseudo.extend_from_slice(&src.octets());
    pseudo.extend_from_slice(&dst.octets());
    pseudo.extend_from_slice(&udp_len.to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0]);
    pseudo.push(IPPROTO_UDP);
    pseudo.extend_from_slice(udp);
    let c = checksum(&pseudo);
    if c == 0 {
        0xFFFF
    } else {
        c
    }
}

// ── Tests ─────────────────────────────────────────────────────────────
