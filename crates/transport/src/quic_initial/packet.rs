use std::collections::HashMap;

use super::crypto::InitialKeys;
use super::frame::read_varint;

pub(super) const QUIC_V1: u32 = 0x0000_0001;
pub(super) const QUIC_V2: u32 = 0x6b33_43cf;

struct InitialHeader<'a> {
    version: u32,
    destination_connection_id: &'a [u8],
    packet_number_offset: usize,
    packet_end: usize,
}

pub(super) fn decrypt_client_initial(
    datagram: &[u8],
    largest_packet_numbers: &mut HashMap<Vec<u8>, u64>,
) -> Result<Option<Vec<Vec<u8>>>, ()> {
    let mut offset = 0;
    let mut plaintexts = Vec::new();
    while offset < datagram.len() && super::looks_like_client_initial(&datagram[offset..]) {
        let (plaintext, consumed) = decrypt_one(
            &datagram[offset..],
            largest_packet_numbers,
        )?;
        plaintexts.push(plaintext);
        offset = offset.checked_add(consumed).ok_or(())?;
    }
    Ok((!plaintexts.is_empty()).then_some(plaintexts))
}

fn decrypt_one(
    packet: &[u8],
    largest_packet_numbers: &mut HashMap<Vec<u8>, u64>,
) -> Result<(Vec<u8>, usize), ()> {
    let header = parse_initial_header(packet)?;
    let keys = InitialKeys::derive(header.version, header.destination_connection_id)?;
    let sample_offset = header.packet_number_offset.checked_add(4).ok_or(())?;
    let sample_end = sample_offset.checked_add(16).ok_or(())?;
    let mask = keys.header_mask(packet.get(sample_offset..sample_end).ok_or(())?)?;

    let first = packet[0] ^ (mask[0] & 0x0f);
    let packet_number_length = usize::from((first & 0x03) + 1);
    let packet_number_end = header
        .packet_number_offset
        .checked_add(packet_number_length)
        .ok_or(())?;
    if packet_number_end > header.packet_end {
        return Err(());
    }
    let mut packet_number_bytes = [0_u8; 4];
    for index in 0..packet_number_length {
        packet_number_bytes[4 - packet_number_length + index] =
            packet[header.packet_number_offset + index] ^ mask[index + 1];
    }
    let truncated = u64::from(u32::from_be_bytes(packet_number_bytes));
    let connection_id = header.destination_connection_id.to_vec();
    let expected = largest_packet_numbers
        .get(&connection_id)
        .copied()
        .map_or(0, |largest| largest.saturating_add(1));
    let packet_number = decode_packet_number(expected, truncated, packet_number_length);

    let mut unprotected_header = packet[..packet_number_end].to_vec();
    unprotected_header[0] = first;
    unprotected_header[header.packet_number_offset..packet_number_end]
        .copy_from_slice(&packet_number_bytes[4 - packet_number_length..]);
    let mut ciphertext = packet[packet_number_end..header.packet_end].to_vec();
    let plaintext = keys.decrypt(packet_number, &unprotected_header, &mut ciphertext)?;
    largest_packet_numbers
        .entry(connection_id)
        .and_modify(|largest| *largest = (*largest).max(packet_number))
        .or_insert(packet_number);
    Ok((plaintext, header.packet_end))
}

fn parse_initial_header(datagram: &[u8]) -> Result<InitialHeader<'_>, ()> {
    if datagram.len() < 7 || datagram[0] & 0xc0 != 0xc0 {
        return Err(());
    }
    let version = u32::from_be_bytes([datagram[1], datagram[2], datagram[3], datagram[4]]);
    let initial_type = match version {
        QUIC_V1 => 0,
        QUIC_V2 => 1,
        _ => return Err(()),
    };
    if (datagram[0] >> 4) & 0x03 != initial_type {
        return Err(());
    }
    let mut offset = 5;
    let destination_length = usize::from(*datagram.get(offset).ok_or(())?);
    offset += 1;
    let destination_end = offset.checked_add(destination_length).ok_or(())?;
    let destination_connection_id = datagram.get(offset..destination_end).ok_or(())?;
    offset = destination_end;
    let source_length = usize::from(*datagram.get(offset).ok_or(())?);
    offset += 1;
    offset = offset.checked_add(source_length).ok_or(())?;
    if offset > datagram.len() {
        return Err(());
    }
    let token_length = usize::try_from(read_varint(datagram, &mut offset)?).map_err(|_| ())?;
    offset = offset.checked_add(token_length).ok_or(())?;
    if offset > datagram.len() {
        return Err(());
    }
    let protected_length = usize::try_from(read_varint(datagram, &mut offset)?).map_err(|_| ())?;
    let packet_end = offset.checked_add(protected_length).ok_or(())?;
    if packet_end > datagram.len() || protected_length < 17 {
        return Err(());
    }
    Ok(InitialHeader {
        version,
        destination_connection_id,
        packet_number_offset: offset,
        packet_end,
    })
}

fn decode_packet_number(expected: u64, truncated: u64, encoded_length: usize) -> u64 {
    let bits = encoded_length * 8;
    let window = 1_u64 << bits;
    let half_window = window / 2;
    let mask = window - 1;
    let candidate = (expected & !mask) | truncated;
    if candidate.saturating_add(half_window) <= expected
        && candidate < (1_u64 << 62).saturating_sub(window)
    {
        candidate + window
    } else if candidate > expected.saturating_add(half_window) && candidate >= window {
        candidate - window
    } else {
        candidate
    }
}
