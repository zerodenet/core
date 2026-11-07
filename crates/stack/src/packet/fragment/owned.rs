//! Actual fragmentation claims storage already reserved by this original owner.
use super::*;
use std::sync::Arc;
use zero_traits::{PacketBuffer, PacketDeriveError, PacketStorage};
#[derive(Clone, Copy)]
struct Layout {
    header: usize,
    payload: usize,
    end: usize,
    chunk: usize,
    v6: bool,
}
fn layout(packet: &[u8], mtu: usize) -> Option<Layout> {
    match packet.first().map(|byte| byte >> 4) {
        Some(4) if packet.len() >= 20 => {
            let header = usize::from(packet[0] & 15) * 4;
            let end = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
            let chunk = (mtu.checked_sub(header)? / 8) * 8;
            (header >= 20 && end > header && end <= packet.len() && chunk != 0).then_some(Layout {
                header,
                payload: header,
                end,
                chunk,
                v6: false,
            })
        }
        Some(6) if packet.len() >= 40 => {
            let end = 40 + usize::from(u16::from_be_bytes([packet[4], packet[5]]));
            let chunk = (mtu.checked_sub(48)? / 8) * 8;
            (end > 40 && end <= packet.len() && chunk != 0).then_some(Layout {
                header: 48,
                payload: 40,
                end,
                chunk,
                v6: true,
            })
        }
        _ => None,
    }
}
/// Pure lengths only: no payload allocation or budget/native/task change.
/// An unchanged packet needs no second buffer or derivation reservation.
pub fn fragment_packet_lengths(packet: &[u8], mtu: usize) -> Result<Vec<usize>, PacketDeriveError> {
    if packet.len() <= mtu {
        return Ok(Vec::new());
    }
    let plan = layout(packet, mtu).ok_or(PacketDeriveError::LayoutMismatch)?;
    let length = plan.end - plan.payload;
    Ok((0..length)
        .step_by(plan.chunk)
        .map(|offset| plan.header + (length - offset).min(plan.chunk))
        .collect())
}
/// Fill only the exact already admitted derived buffers. No byte allocation
/// or credit changes occur here; the caller retains the original until every
/// actual asynchronous send using these fragments ends.
pub fn write_packet_fragments(
    packet: &[u8],
    mtu: usize,
    identification: u32,
    outputs: &mut [PacketBuffer],
) -> Result<(), PacketDeriveError> {
    if packet.len() <= mtu {
        return if outputs.is_empty() {
            Ok(())
        } else {
            Err(PacketDeriveError::LayoutMismatch)
        };
    }
    let plan = layout(packet, mtu).ok_or(PacketDeriveError::LayoutMismatch)?;
    let length = plan.end - plan.payload;
    if outputs.len() != length.div_ceil(plan.chunk)
        || !outputs.iter().enumerate().all(|(index, output)| {
            output.len() == plan.header + (length - index * plan.chunk).min(plan.chunk)
        })
    {
        return Err(PacketDeriveError::LayoutMismatch);
    }
    for (index, output) in outputs.iter_mut().enumerate() {
        let offset = index * plan.chunk;
        let payload =
            &packet[plan.payload + offset..plan.end.min(plan.payload + offset + plan.chunk)];
        let more = plan.payload + offset + payload.len() < plan.end;
        if plan.v6 {
            output[..40].copy_from_slice(&packet[..40]);
            output[6] = 44;
            output[40..48].fill(0);
            output[40] = packet[6];
            output[42..44].copy_from_slice(&((offset as u16) | u16::from(more)).to_be_bytes());
            output[44..48].copy_from_slice(&identification.to_be_bytes());
            output[48..].copy_from_slice(payload);
            output[4..6].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
        } else {
            output[..plan.header].copy_from_slice(&packet[..plan.header]);
            output[plan.header..].copy_from_slice(payload);
            let length = output.len() as u16;
            output[2..4].copy_from_slice(&length.to_be_bytes());
            output[4..6].copy_from_slice(&(identification as u16).to_be_bytes());
            output[6..8].copy_from_slice(
                &(((offset / 8) as u16) | if more { 0x2000 } else { 0 }).to_be_bytes(),
            );
            output[10..12].fill(0);
            let sum = checksum(&output[..plan.header]);
            output[10..12].copy_from_slice(&sum.to_be_bytes());
        }
    }
    Ok(())
}
#[derive(Debug)]
pub struct PacketFragmentFailure {
    pub cause: PacketDeriveError,
    pub original: PacketBuffer,
}
impl std::fmt::Display for PacketFragmentFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for PacketFragmentFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}
/// The original allocation/credit remains owned by every actual derived view
/// until the last one leaves its real asynchronous send continuation.
pub async fn fragment_ip_packet_accounted(
    packet: PacketBuffer,
    mtu: usize,
    identification: u32,
) -> Result<Vec<PacketBuffer>, PacketFragmentFailure> {
    if packet.len() <= mtu {
        return Ok(vec![packet]);
    }
    let lengths = match fragment_packet_lengths(&packet, mtu) {
        Ok(lengths) => lengths,
        Err(cause) => {
            return Err(PacketFragmentFailure {
                cause,
                original: packet,
            })
        }
    };
    let mut outputs = match packet.derive_zeroed(&lengths).await {
        Ok(outputs)
            if outputs.len() == lengths.len()
                && outputs
                    .iter()
                    .zip(&lengths)
                    .all(|(output, length)| output.len() == *length) =>
        {
            outputs
        }
        Ok(_) => {
            return Err(PacketFragmentFailure {
                cause: PacketDeriveError::LayoutMismatch,
                original: packet,
            })
        }
        Err(cause) => {
            return Err(PacketFragmentFailure {
                cause,
                original: packet,
            })
        }
    };
    if let Err(cause) = write_packet_fragments(&packet, mtu, identification, &mut outputs) {
        return Err(PacketFragmentFailure {
            cause,
            original: packet,
        });
    }
    let original = Arc::new(packet);
    Ok(outputs
        .into_iter()
        .map(|packet| {
            PacketBuffer::from_owner(Derived {
                packet,
                _original: original.clone(),
            })
        })
        .collect())
}
/// Production TX only claims storage prepaid before the FIFO. Unlike the
/// explicit asynchronous library derivation, this can never reacquire Root
/// capacity while the wire driver owns the queue head.
pub fn fragment_ip_packet_prepaid(
    packet: PacketBuffer,
    mtu: usize,
    identification: u32,
) -> Result<Vec<PacketBuffer>, PacketFragmentFailure> {
    if packet.len() <= mtu {
        return Ok(vec![packet]);
    }
    let result = (|| {
        let lengths = fragment_packet_lengths(&packet, mtu)?;
        let mut outputs = packet.try_derive_zeroed(&lengths)?;
        write_packet_fragments(&packet, mtu, identification, &mut outputs)?;
        Ok(outputs)
    })();
    let outputs = match result {
        Ok(outputs) => outputs,
        Err(cause) => {
            return Err(PacketFragmentFailure {
                cause,
                original: packet,
            })
        }
    };
    let original = Arc::new(packet);
    Ok(outputs
        .into_iter()
        .map(|packet| {
            PacketBuffer::from_owner(Derived {
                packet,
                _original: original.clone(),
            })
        })
        .collect())
}

struct Derived {
    packet: PacketBuffer,
    _original: Arc<PacketBuffer>,
}
impl AsRef<[u8]> for Derived {
    fn as_ref(&self) -> &[u8] {
        &self.packet
    }
}
impl AsMut<[u8]> for Derived {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.packet
    }
}
impl PacketStorage for Derived {
    fn prepare_transmit(&mut self, frames: usize, bytes: usize) -> Result<(), PacketDeriveError> {
        self.packet.prepare_transmit(frames, bytes)
    }
    fn try_derive_zeroed(&self, lengths: &[usize]) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
        self.packet.try_derive_zeroed(lengths)
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self.packet.derive_zeroed(lengths)
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.packet.take_vec_allocation()
    }
    fn allocation_bytes(&self) -> usize {
        self.packet
            .allocation_bytes()
            .saturating_add(self._original.allocation_bytes())
    }
}
