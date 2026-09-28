//! Pure ICMP address/identifier rewriting for an explicitly selected adapter.

use std::net::IpAddr;

use super::{icmpv6_checksum, parse_icmp_echo_reply, parse_icmp_echo_request};
use crate::packet::{checksum, transport_header, IPPROTO_ICMP, IPPROTO_ICMPV6};

/// Preserve the IP header, options, hop limit and payload of an Echo request.
pub fn translate_echo_request(packet: &[u8], local: IpAddr, id: u16) -> Option<Vec<u8>> {
    parse_icmp_echo_request(packet)?;
    let header = transport_header(packet)?;
    if packet[0] >> 4 == 4 && checksum(&packet[..header.offset]) != 0 {
        return None;
    }
    let mut translated = packet[..header.end].to_vec();
    set_address(&mut translated, local, true)?;
    translated[header.offset + 4..header.offset + 6].copy_from_slice(&id.to_be_bytes());
    repair_checksums(&mut translated)?;
    Some(translated)
}

/// Identify a reply or error quoting a translated Echo request.
pub fn echo_response_key(packet: &[u8]) -> Option<(IpAddr, IpAddr, u16)> {
    if let Some(reply) = parse_icmp_echo_reply(packet) {
        return Some((reply.destination, reply.source, reply.identifier));
    }
    let (local, remote, id, _) = quoted_echo(packet)?;
    Some((local, remote, id))
}

/// Restore only a checksum-valid response matching this exact request.
/// ICMP errors retain their router source, type/code, MTU and quoted length.
pub fn restore_echo_response(packet: &[u8], original: &[u8], translated: &[u8]) -> Option<Vec<u8>> {
    let request = parse_icmp_echo_request(original)?;
    let probe = parse_icmp_echo_request(translated)?;
    let header = transport_header(packet)?;
    let mut restored = packet[..header.end].to_vec();
    if packet[0] >> 4 == 4 && checksum(&packet[..header.offset]) != 0 {
        return None;
    }
    if let Some(reply) = parse_icmp_echo_reply(packet) {
        if reply.destination != probe.source
            || reply.source != probe.destination
            || reply.message[4..] != probe.message[4..]
        {
            return None;
        }
        restored[header.offset + 4..header.offset + 6].copy_from_slice(&request.message[4..6]);
    } else {
        let (local, remote, id, sequence) = quoted_echo(packet)?;
        if local != probe.source
            || remote != probe.destination
            || id.to_be_bytes() != probe.message[4..6]
            || sequence != probe.message[6..8]
        {
            return None;
        }
        let quote = &packet[header.offset + 8..header.end];
        if quote.len() > original.len() {
            return None;
        }
        // Restore the original header and transport checksum together, even
        // when the error quotes only the mandated first eight ICMP bytes.
        restored[header.offset + 8..].copy_from_slice(&original[..quote.len()]);
    }
    set_address(&mut restored, request.source, false)?;
    repair_checksums(&mut restored)?;
    Some(restored)
}

fn quoted_echo(packet: &[u8]) -> Option<(IpAddr, IpAddr, u16, [u8; 2])> {
    let outer = transport_header(packet)?;
    let message = packet.get(outer.offset..outer.end)?;
    if message.len() < 8 {
        return None;
    }
    let valid = match (outer.src, outer.dst, outer.protocol) {
        (IpAddr::V4(_), IpAddr::V4(_), IPPROTO_ICMP) => {
            matches!(message[0], 3 | 11 | 12) && checksum(message) == 0
        }
        (IpAddr::V6(source), IpAddr::V6(destination), IPPROTO_ICMPV6) => {
            matches!(message[0], 1..=4) && icmpv6_checksum(source, destination, message) == 0
        }
        _ => false,
    };
    if !valid {
        return None;
    }
    // Quotes may be truncated. Normalize the advertised length for the
    // existing bounded IP/extension-header parser; do not recreate it here.
    let mut quote = message[8..].to_vec();
    let length = u16::try_from(quote.len()).ok()?;
    match quote.first()? >> 4 {
        4 if quote.len() >= 20 => {
            let fragment = u16::from_be_bytes([quote[6], quote[7]]);
            if fragment & 0x1fff != 0 {
                return None;
            }
            // A first fragment still quotes the Echo header. The common
            // parser requires a complete transport; normalize MF only in
            // this private quote copy, never in the forwarded packet.
            quote[6] &= !0x20;
            quote[2..4].copy_from_slice(&length.to_be_bytes());
        }
        6 if quote.len() >= 40 => quote[4..6].copy_from_slice(&(length - 40).to_be_bytes()),
        _ => return None,
    }
    let inner = transport_header(&quote)?;
    let echo = quote.get(inner.offset..inner.end)?;
    if inner.src != outer.dst
        || echo.len() < 8
        || echo[1] != 0
        || !matches!(
            (inner.protocol, echo[0]),
            (IPPROTO_ICMP, 8) | (IPPROTO_ICMPV6, 128)
        )
    {
        return None;
    }
    Some((
        inner.src,
        inner.dst,
        u16::from_be_bytes([echo[4], echo[5]]),
        [echo[6], echo[7]],
    ))
}

fn set_address(packet: &mut [u8], address: IpAddr, source: bool) -> Option<()> {
    match (packet[0] >> 4, address) {
        (4, IpAddr::V4(address)) => {
            let offset = if source { 12 } else { 16 };
            packet[offset..offset + 4].copy_from_slice(&address.octets());
        }
        (6, IpAddr::V6(address)) => {
            let offset = if source { 8 } else { 24 };
            packet[offset..offset + 16].copy_from_slice(&address.octets());
        }
        _ => return None,
    }
    Some(())
}

fn repair_checksums(packet: &mut [u8]) -> Option<()> {
    let header = transport_header(packet)?;
    if packet[0] >> 4 == 4 {
        let length = usize::from(packet[0] & 15) * 4;
        packet[10..12].fill(0);
        let value = checksum(&packet[..length]);
        packet[10..12].copy_from_slice(&value.to_be_bytes());
    }
    packet[header.offset + 2..header.offset + 4].fill(0);
    let message = &packet[header.offset..header.end];
    let value = match (header.src, header.dst, header.protocol) {
        (IpAddr::V4(_), IpAddr::V4(_), IPPROTO_ICMP) => checksum(message),
        (IpAddr::V6(source), IpAddr::V6(destination), IPPROTO_ICMPV6) => {
            icmpv6_checksum(source, destination, message)
        }
        _ => return None,
    };
    packet[header.offset + 2..header.offset + 4].copy_from_slice(&value.to_be_bytes());
    Some(())
}
