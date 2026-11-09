//! Existing Mieru flow framing using only caller-admitted fixed storage.
use super::*;
use alloc::vec;
use zero_traits::{
    DatagramCodecBufferError as Failure, DatagramCodecOutput as Output, PacketBuffer,
    PacketDeriveError,
};
type Result<T> = core::result::Result<T, Failure<Error>>;
fn invalid() -> Failure<Error> {
    Failure::Protocol(Error::Protocol("mieru udp: invalid flow packet"))
}
fn check(buffers: &[PacketBuffer], lengths: &[usize]) -> Result<()> {
    if buffers.len() != lengths.len() || buffers.iter().zip(lengths).any(|(b, n)| b.len() < *n) {
        return Err(Failure::Storage(PacketDeriveError::LayoutMismatch));
    }
    Ok(())
}
impl MieruUdpFlowCodec {
    pub(super) fn encoded_lengths(&self, target: &Address, payload: usize) -> Result<Vec<usize>> {
        let address = match target {
            Address::Ipv4(_) => 5,
            Address::Ipv6(_) => 17,
            Address::Domain(name) if !name.is_empty() && name.len() <= 255 => 2 + name.len(),
            _ => return Err(invalid()),
        };
        let packet = payload
            .checked_add(address + 5)
            .ok_or(Failure::Storage(PacketDeriveError::Overflow))?;
        if packet > u16::MAX as usize {
            return Err(invalid());
        }
        Ok(vec![packet + 4])
    }
    pub(super) fn decoded_lengths(&self, wire: usize) -> Result<Vec<usize>> {
        Ok(vec![wire, 255])
    }
    pub(super) fn encode_reserved(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [PacketBuffer],
    ) -> Result<Output> {
        let lengths = self.encoded_lengths(target, payload.len())?;
        check(buffers, &lengths)?;
        let wire = &mut buffers[0];
        let end = lengths[0] - 1;
        wire[0] = 0;
        wire[1..3].copy_from_slice(&((end - 3) as u16).to_be_bytes());
        wire[3..6].fill(0);
        let cursor = match target {
            Address::Ipv4(ip) => {
                wire[6] = 1;
                wire[7..11].copy_from_slice(ip);
                11
            }
            Address::Ipv6(ip) => {
                wire[6] = 4;
                wire[7..23].copy_from_slice(ip);
                23
            }
            Address::Domain(name) => {
                wire[6] = 3;
                wire[7] = name.len() as u8;
                wire[8..8 + name.len()].copy_from_slice(name.as_bytes());
                8 + name.len()
            }
        };
        wire[cursor..cursor + 2].copy_from_slice(&port.to_be_bytes());
        wire[cursor + 2..end].copy_from_slice(payload);
        wire[end] = 255;
        Ok(Output {
            buffer: 0,
            range: 0..lengths[0],
        })
    }
    pub(super) fn decode_reserved(
        &self,
        wire: &[u8],
        buffers: &mut [PacketBuffer],
    ) -> Result<Option<(Address, u16, Output)>> {
        check(buffers, &self.decoded_lengths(wire.len())?)?;
        match decode(wire, buffers) {
            Err(Failure::Protocol(_)) => Ok(None),
            other => other,
        }
    }
}
fn decode(wire: &[u8], buffers: &mut [PacketBuffer]) -> Result<Option<(Address, u16, Output)>> {
    if wire.len() < 8 || wire[0] != 0 {
        return Err(invalid());
    }
    let end = 3 + u16::from_be_bytes([wire[1], wire[2]]) as usize;
    if wire.get(end) != Some(&255) || wire.get(3..6) != Some(&[0, 0, 0][..]) {
        return Err(invalid());
    }
    let packet = &wire[..end];
    let (target, cursor) = match packet.get(6) {
        Some(1) => {
            let mut ip = [0; 4];
            ip.copy_from_slice(packet.get(7..11).ok_or_else(invalid)?);
            (Address::Ipv4(ip), 11)
        }
        Some(4) => {
            let mut ip = [0; 16];
            ip.copy_from_slice(packet.get(7..23).ok_or_else(invalid)?);
            (Address::Ipv6(ip), 23)
        }
        Some(3) => {
            let n = *packet.get(7).ok_or_else(invalid)? as usize;
            if n == 0 {
                return Err(invalid());
            }
            let bytes = packet.get(8..8 + n).ok_or_else(invalid)?;
            core::str::from_utf8(bytes).map_err(|_| invalid())?;
            let mut owned = buffers[1]
                .take_vec_allocation()
                .ok_or(Failure::Storage(PacketDeriveError::Unreserved))?;
            owned[..n].copy_from_slice(bytes);
            owned.truncate(n);
            (
                Address::Domain(String::from_utf8(owned).map_err(|_| invalid())?),
                8 + n,
            )
        }
        _ => return Err(invalid()),
    };
    let port = packet.get(cursor..cursor + 2).ok_or_else(invalid)?;
    let port = u16::from_be_bytes([port[0], port[1]]);
    buffers[0][..wire.len()].copy_from_slice(wire);
    Ok(Some((
        target,
        port,
        Output {
            buffer: 0,
            range: cursor + 2..end,
        },
    )))
}

impl DatagramCodec<Address> for MieruUdpFlowCodec {
    type Error = Error;
    fn encode_buffer_lengths(
        &self,
        target: &Address,
        _port: u16,
        payload_len: usize,
    ) -> core::result::Result<Vec<usize>, zero_traits::DatagramCodecBufferError<Error>> {
        self.encoded_lengths(target, payload_len)
    }
    fn encode_buffered(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [zero_traits::PacketBuffer],
    ) -> core::result::Result<
        zero_traits::DatagramCodecOutput,
        zero_traits::DatagramCodecBufferError<Error>,
    > {
        self.encode_reserved(target, port, payload, buffers)
    }
    fn decode_buffer_lengths(
        &self,
        wire_len: usize,
    ) -> core::result::Result<Vec<usize>, zero_traits::DatagramCodecBufferError<Error>> {
        self.decoded_lengths(wire_len)
    }
    fn decode_buffered(
        &self,
        wire: &[u8],
        buffers: &mut [zero_traits::PacketBuffer],
    ) -> zero_traits::DatagramCodecDecodeResult<Address, Error> {
        self.decode_reserved(wire, buffers)
    }

    fn encode(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> core::result::Result<Vec<u8>, Self::Error> {
        encode_udp_flow_packet(target, port, payload)
    }

    fn decode(&self, data: &[u8]) -> Option<(Address, u16, Vec<u8>)> {
        let decoded = decode_udp_flow_packet(data).ok()?;
        Some(decoded.into_parts())
    }
}
