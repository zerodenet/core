//! The retransmission cache keeps one immutable actual original. Each output
//! claims its own exclusively mutable copy from that same prepaid envelope.
use crate::{packet::TcpPacketSpec, packet_output::PacketSender};
#[cfg(feature = "packet-memory")]
use std::sync::Arc;
#[cfg(feature = "packet-memory")]
use zero_traits::PacketStorage;
use zero_traits::{PacketBuffer, PacketDeriveError};

#[derive(Clone)]
pub(super) enum CachedPacket {
    Library(Vec<u8>),
    #[cfg(feature = "packet-memory")]
    Owned(Arc<PacketBuffer>),
}
impl CachedPacket {
    pub(super) fn build(
        spec: TcpPacketSpec<'_>,
        output: &PacketSender,
    ) -> Result<Self, PacketDeriveError> {
        let length = spec.length()?;
        #[cfg(feature = "packet-memory")]
        if output.has_packet_memory() {
            return allocated(spec, output, true).map(|packet| Self::Owned(Arc::new(packet)));
        }
        #[cfg(not(feature = "packet-memory"))]
        let _ = output;
        let mut bytes = vec![0; length];
        spec.write_into(&mut bytes)?;
        Ok(Self::Library(bytes))
    }
    pub(super) fn update(&mut self, spec: TcpPacketSpec<'_>) -> Result<bool, PacketDeriveError> {
        let length = spec.length()?;
        let bytes = match self {
            Self::Library(bytes) => &mut bytes[..],
            #[cfg(feature = "packet-memory")]
            Self::Owned(packet) => match Arc::get_mut(packet) {
                Some(packet) => packet.as_mut(),
                None => return Ok(false),
            },
        };
        if length != bytes.len() {
            return Ok(false);
        }
        spec.write_into(bytes)?;
        Ok(true)
    }
    pub(super) fn outgoing(&self) -> Result<PacketBuffer, PacketDeriveError> {
        match self {
            Self::Library(bytes) => Ok(bytes.clone().into()),
            #[cfg(feature = "packet-memory")]
            Self::Owned(original) => {
                let mut outputs = original.try_derive_owned(&[original.len()])?;
                if outputs.len() != 1 || outputs[0].len() != original.len() {
                    return Err(PacketDeriveError::LayoutMismatch);
                }
                let mut packet = outputs.pop().expect("same prepaid outgoing TCP copy");
                packet.copy_from_slice(original);
                Ok(PacketBuffer::from_owner(Outgoing {
                    packet,
                    original: original.clone(),
                }))
            }
        }
    }
}
pub(super) fn build_packet(
    spec: TcpPacketSpec<'_>,
    output: &PacketSender,
) -> Result<PacketBuffer, PacketDeriveError> {
    #[cfg(feature = "packet-memory")]
    if output.has_packet_memory() {
        return allocated(spec, output, false);
    }
    #[cfg(not(feature = "packet-memory"))]
    let _ = output;
    let mut bytes = vec![0; spec.length()?];
    spec.write_into(&mut bytes)?;
    Ok(bytes.into())
}
#[cfg(feature = "packet-memory")]
fn allocated(
    spec: TcpPacketSpec<'_>,
    output: &PacketSender,
    cache: bool,
) -> Result<PacketBuffer, PacketDeriveError> {
    let length = spec.length()?;
    let mut groups = if cache {
        vec![vec![length]]
    } else {
        Vec::new()
    };
    // The existing TCP PMTU receiver clamps to these real protocol
    // floors. Pay the maximum complete fragment/codec footprint now;
    // its actual driver still uses the latest learned MTU at dequeue.
    let (header, fragment_header, mtu) = if spec.source.is_ipv4() {
        (20, 20, 68)
    } else {
        (40, 48, 1280)
    };
    let wire = output.wire_memory().ok_or(PacketDeriveError::Unreserved)?;
    if length > mtu {
        let chunk = ((mtu - fragment_header) / 8) * 8;
        let payload = length - header;
        let fragments = (0..payload)
            .step_by(chunk)
            .map(|offset| fragment_header + (payload - offset).min(chunk))
            .collect::<Vec<_>>();
        groups.push(fragments.clone());
        for fragment in fragments {
            groups.push(wire.workspace_lengths(fragment)?);
        }
    } else {
        groups.push(wire.workspace_lengths(length)?);
    }
    let mut original = output.allocate_transmit_groups(length, groups)?;
    spec.write_into(&mut original)?;
    Ok(original)
}
impl From<Vec<u8>> for CachedPacket {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Library(bytes)
    }
}
#[cfg(feature = "packet-memory")]
struct Outgoing {
    packet: PacketBuffer,
    original: Arc<PacketBuffer>,
}
#[cfg(feature = "packet-memory")]
impl AsRef<[u8]> for Outgoing {
    fn as_ref(&self) -> &[u8] {
        &self.packet
    }
}
#[cfg(feature = "packet-memory")]
impl AsMut<[u8]> for Outgoing {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.packet
    }
}
#[cfg(feature = "packet-memory")]
impl PacketStorage for Outgoing {
    fn allocation_bytes(&self) -> usize {
        self.packet
            .allocation_bytes()
            .saturating_add(self.original.allocation_bytes())
    }
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.packet.take_vec_allocation()
    }
    fn prepare_transmit(&mut self, frames: usize, bytes: usize) -> Result<(), PacketDeriveError> {
        self.packet.prepare_transmit(frames, bytes)
    }
    fn try_derive_owned(
        &self,
        lengths: &[usize],
    ) -> Result<zero_traits::PacketBatch, zero_traits::PacketDeriveError> {
        self.packet.try_derive_owned(lengths)
    }
    fn try_derive_zeroed(&self, lengths: &[usize]) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
        self.packet.try_derive_zeroed(lengths)
    }
    fn derive_zeroed<'a>(&'a self, lengths: &'a [usize]) -> zero_traits::PacketDerivation<'a> {
        self.packet.derive_zeroed(lengths)
    }
}

#[cfg(all(test, feature = "packet-memory"))]
mod tests;
