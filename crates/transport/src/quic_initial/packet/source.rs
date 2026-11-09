use super::*;
use crate::quic_initial::source::{Number, Plain, Source};
use zero_traits::packet_metadata::PacketMemoryError;
pub(in crate::quic_initial) enum SourceFailure {
    Memory(PacketMemoryError),
    Rejected,
}
impl From<()> for SourceFailure {
    fn from(_: ()) -> Self {
        Self::Rejected
    }
}
impl From<PacketMemoryError> for SourceFailure {
    fn from(value: PacketMemoryError) -> Self {
        Self::Memory(value)
    }
}
pub(in crate::quic_initial) fn fill_source_initial(
    datagram: &[u8],
    source: &mut Source,
) -> Result<(), SourceFailure> {
    if datagram.len() > 65535 {
        return Err(PacketMemoryError::Capacity.into());
    }
    let mut inflight = source
        .inflight
        .take()
        .expect("same original passive input continuation");
    let result = (|| {
        let bound = datagram.len() / 17;
        while inflight.offset < datagram.len()
            && super::super::looks_like_client_initial(&datagram[inflight.offset..])
        {
            inflight
                .plaintexts
                .try_reserve_one_with_metadata(&source.memory, bound)?;
            let (plain, consumed) = decrypt_one_source(&datagram[inflight.offset..], source)?;
            inflight
                .plaintexts
                .push_reserved(plain)
                .unwrap_or_else(|_| unreachable!("same already reserved plaintext row"));
            inflight.offset = inflight.offset.checked_add(consumed).ok_or(())?;
        }
        inflight.decrypt_complete = true;
        Ok(())
    })();
    // A transient refusal retains the SAME concrete paid array, decrypted
    // prefixes and byte cursor. No repeated allocation/refund self-wake loop,
    // reauthentication of committed coalesced prefixes, task or new queue.
    source.inflight = Some(inflight);
    result
}

fn decrypt_one_source(packet: &[u8], source: &mut Source) -> Result<(Plain, usize), SourceFailure> {
    let header = parse_initial_header(packet)?;
    let keys = InitialKeys::derive(header.version, header.destination_connection_id)?;
    let sample = header.packet_number_offset.checked_add(4).ok_or(())?;
    let sample_end = sample.checked_add(16).ok_or(())?;
    let mask = keys.header_mask(packet.get(sample..sample_end).ok_or(())?)?;
    let first = packet[0] ^ (mask[0] & 0x0f);
    let pn_length = usize::from((first & 3) + 1);
    let pn_end = header
        .packet_number_offset
        .checked_add(pn_length)
        .ok_or(())?;
    if pn_end > header.packet_end {
        return Err(().into());
    }
    let mut pn_bytes = [0u8; 4];
    for index in 0..pn_length {
        pn_bytes[4 - pn_length + index] =
            packet[header.packet_number_offset + index] ^ mask[index + 1];
    }
    let cid = header.destination_connection_id;
    let matched = source
        .numbers
        .iter()
        .position(|entry| &entry.cid[..usize::from(entry.length)] == cid);
    let expected = matched.map_or(0, |index| source.numbers[index].largest.saturating_add(1));
    if matched.is_none() {
        source.numbers.try_reserve_one_with_metadata(
            &source.memory,
            crate::quic_initial::MAX_CONNECTION_IDS,
        )?;
    }
    let pn = decode_packet_number(expected, u64::from(u32::from_be_bytes(pn_bytes)), pn_length);
    source.header.resize(pn_end, 0, 65535)?;
    source.header[..pn_end].copy_from_slice(&packet[..pn_end]);
    source.header[0] = first;
    source.header[header.packet_number_offset..pn_end].copy_from_slice(&pn_bytes[4 - pn_length..]);
    let mut plaintext = Plain::copy(&source.memory, &packet[pn_end..header.packet_end])?;
    plaintext.length = keys.decrypt_into(pn, &source.header[..pn_end], &mut plaintext.bytes)?;
    if let Some(index) = matched {
        source.numbers[index].largest = source.numbers[index].largest.max(pn);
    } else {
        let mut entry = Number {
            cid: [0; 255],
            length: u8::try_from(cid.len()).map_err(|_| ())?,
            largest: pn,
        };
        entry.cid[..cid.len()].copy_from_slice(cid);
        source
            .numbers
            .push_reserved(entry)
            .unwrap_or_else(|_| unreachable!("same CID row reserved before authentication"));
    }
    Ok((plaintext, header.packet_end))
}
