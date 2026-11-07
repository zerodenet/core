//! Existing unfragmented flow codec; storage is admitted by its actual caller.
use super::{Address, Error, Hysteria2DatagramCodec};
use alloc::{string::String, vec, vec::Vec};
use core::fmt::{self, Write};
use zero_traits::{
    DatagramCodecBufferError as Failure, DatagramCodecOutput as Output, PacketBuffer,
    PacketDeriveError,
};
type Result<T> = core::result::Result<T, Failure<Error>>;
fn invalid() -> Failure<Error> {
    Failure::Protocol(Error::Protocol("hysteria2: invalid UDP address"))
}
fn overflow() -> Failure<Error> {
    Failure::Storage(PacketDeriveError::Overflow)
}
struct Authority<'a> {
    bytes: Option<&'a mut [u8]>,
    length: usize,
}
impl Write for Authority<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let end = self.length.checked_add(text.len()).ok_or(fmt::Error)?;
        if let Some(bytes) = &mut self.bytes {
            bytes
                .get_mut(self.length..end)
                .ok_or(fmt::Error)?
                .copy_from_slice(text.as_bytes());
        }
        self.length = end;
        Ok(())
    }
}
fn authority(target: &Address, port: u16, output: &mut Authority<'_>) -> Result<()> {
    let result = match target {
        Address::Ipv4(ip) => write!(output, "{}.{}.{}.{}:{port}", ip[0], ip[1], ip[2], ip[3]),
        Address::Ipv6(ip) => write!(output, "[{}]:{port}", core::net::Ipv6Addr::from(*ip)),
        Address::Domain(name) if !name.is_empty() => write!(output, "{name}:{port}"),
        _ => return Err(invalid()),
    };
    result.map_err(|_| overflow())
}
fn width(value: usize) -> Result<usize> {
    match value as u64 {
        0..=63 => Ok(1),
        64..=16383 => Ok(2),
        16384..=1073741823 => Ok(4),
        1073741824..=4611686018427387903 => Ok(8),
        _ => Err(overflow()),
    }
}
fn check(buffers: &[PacketBuffer], lengths: &[usize]) -> Result<()> {
    if buffers.len() != lengths.len() || buffers.iter().zip(lengths).any(|(b, n)| b.len() < *n) {
        return Err(Failure::Storage(PacketDeriveError::LayoutMismatch));
    }
    Ok(())
}
impl Hysteria2DatagramCodec {
    pub(super) fn encoded_lengths(
        &self,
        target: &Address,
        port: u16,
        payload: usize,
    ) -> Result<Vec<usize>> {
        let mut text = Authority {
            bytes: None,
            length: 0,
        };
        authority(target, port, &mut text)?;
        let wire = text
            .length
            .checked_add(8 + width(text.length)?)
            .and_then(|n| n.checked_add(payload))
            .ok_or_else(overflow)?;
        Ok(vec![text.length, wire])
    }
    pub(super) fn decoded_lengths(&self, wire: usize) -> Result<Vec<usize>> {
        Ok(vec![wire, wire])
    }
    pub(super) fn encode_reserved(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
        buffers: &mut [PacketBuffer],
    ) -> Result<Output> {
        let lengths = self.encoded_lengths(target, port, payload.len())?;
        check(buffers, &lengths)?;
        let (text, wire) = buffers.split_at_mut(1);
        authority(
            target,
            port,
            &mut Authority {
                bytes: Some(&mut text[0]),
                length: 0,
            },
        )?;
        let width = width(lengths[0])?;
        wire[0][..8].fill(0);
        wire[0][7] = 1;
        let value = (lengths[0] as u64)
            | match width {
                1 => 0,
                2 => 0x4000,
                4 => 0x80000000,
                _ => 0xc000000000000000,
            };
        wire[0][8..8 + width].copy_from_slice(&value.to_be_bytes()[8 - width..]);
        let end = 8 + width + lengths[0];
        wire[0][8 + width..end].copy_from_slice(&text[0][..lengths[0]]);
        wire[0][end..lengths[1]].copy_from_slice(payload);
        Ok(Output {
            buffer: 1,
            range: 0..lengths[1],
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
    if wire.len() < 9 || wire[7] == 0 || wire[6] >= wire[7] {
        return Err(invalid());
    }
    let (length, width) = crate::shared::decode_varint(&wire[8..]).map_err(Failure::Protocol)?;
    let length = usize::try_from(length).map_err(|_| invalid())?;
    let end = (8 + width).checked_add(length).ok_or_else(invalid)?;
    let text = core::str::from_utf8(wire.get(8 + width..end).ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    let (host, port) = if let Some(rest) = text.strip_prefix('[') {
        rest.split_once("]:").ok_or_else(invalid)?
    } else {
        text.rsplit_once(':').ok_or_else(invalid)?
    };
    if host.is_empty() {
        return Err(invalid());
    }
    let port = port.parse::<u16>().map_err(|_| invalid())?;
    let target = if let Ok(ip) = host.parse::<core::net::Ipv4Addr>() {
        Address::Ipv4(ip.octets())
    } else if let Ok(ip) = host.parse::<core::net::Ipv6Addr>() {
        Address::Ipv6(ip.octets())
    } else {
        let mut bytes = buffers[1]
            .take_vec_allocation()
            .ok_or(Failure::Storage(PacketDeriveError::Unreserved))?;
        bytes[..host.len()].copy_from_slice(host.as_bytes());
        bytes.truncate(host.len());
        Address::Domain(String::from_utf8(bytes).map_err(|_| invalid())?)
    };
    buffers[0][..wire.len()].copy_from_slice(wire);
    Ok(Some((
        target,
        port,
        Output {
            buffer: 0,
            range: end..wire.len(),
        },
    )))
}
