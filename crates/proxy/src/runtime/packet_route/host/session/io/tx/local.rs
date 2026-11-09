//! Same owned submission pays and retains its local return before acceptance.
use super::*;
use zero_stack::packet::IcmpErrorSpec;
use zero_traits::{PacketBuffer, PacketDeriveError, PacketStorage};
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Mtu,
    Hop,
}
fn spec(
    kind: Kind,
    packet: &[u8],
    router: std::net::IpAddr,
    mtu: usize,
) -> Option<IcmpErrorSpec<'_>> {
    match kind {
        Kind::Mtu => IcmpErrorSpec::for_mtu_error(packet, router, mtu),
        Kind::Hop => IcmpErrorSpec::for_time_exceeded(packet, router, mtu),
    }
}
pub(super) fn respond(
    session: &Session,
    slot: &mut Option<IpPacket>,
    kind: Kind,
    router: Option<std::net::IpAddr>,
    mtu: usize,
) -> Result<SendReceipt, SendFailure> {
    let frame = slot.as_ref().expect("same validated local original");
    let bytes = frame.payload().len();
    let Some(router) = router else {
        drop(slot.take());
        return Ok(SendReceipt::accepted(bytes));
    };
    let Some(length) = spec(kind, frame.payload(), router, mtu).map(|spec| spec.length()) else {
        drop(slot.take());
        return Ok(SendReceipt::accepted(bytes));
    };
    let Some(parent) = session.parent.upgrade() else {
        return Err(failure(
            Reason::Revoked,
            "original Packet owner retired",
            DeliveryState::NotSubmitted,
        ));
    };
    let _notifications = parent.budget.defer_notifications();
    let mut lengths = vec![length];
    lengths.extend(
        session
            .return_memory
            .return_workspace_lengths(
                length,
                usize::from(session.return_mtu),
                frame.payload()[0] >> 4 == 4,
            )
            .map_err(memory_failure)?,
    );
    let total = lengths
        .iter()
        .try_fold(0usize, |sum, n| sum.checked_add(*n))
        .ok_or_else(|| memory_failure(PacketDeriveError::Overflow))?;
    let frame = slot.as_mut().expect("same original before preparation");
    frame
        .prepare_transmit_buffers(lengths.len(), total)
        .map_err(memory_failure)?;
    let mut outputs = frame
        .try_derive_payload_batch(&[length])
        .map_err(memory_failure)?;
    if outputs.len() != 1 {
        return Err(memory_failure(PacketDeriveError::LayoutMismatch));
    }
    let mut output = outputs.pop().expect("one prepaid local response");
    spec(kind, frame.payload(), router, mtu)
        .expect("same unchanged original")
        .write_into(&mut output)
        .map_err(memory_failure)?;
    let frame = slot
        .take()
        .expect("same original retained before publishing response");
    let association = frame.return_association.clone();
    let ingress = frame.ingress();
    let original = Arc::new(frame.into_payload());
    let response = IpPacket::new(
        PacketBuffer::from_owner(Retained {
            packet: output,
            original: original.clone(),
        }),
        association.clone(),
    )
    .expect("same pure ICMP writer envelope");
    if !session.deliver(response, false) {
        // Failure means this exact response never entered its return FIFO;
        // no escaped view can retain the original. Restore all source facts.
        let payload =
            Arc::try_unwrap(original).expect("rejected local reply has no escaped original");
        let mut frame = IpPacket::new(payload, association).expect("same original envelope");
        if let Some(ingress) = ingress {
            frame = frame.with_ingress(ingress);
        }
        *slot = Some(frame);
        return Err(failure(
            Reason::Capacity,
            "same Packet reply budget full",
            DeliveryState::NotSubmitted,
        ));
    }
    Ok(SendReceipt::accepted(bytes))
}
struct Retained {
    packet: PacketBuffer,
    original: Arc<PacketBuffer>,
}
impl AsRef<[u8]> for Retained {
    fn as_ref(&self) -> &[u8] {
        &self.packet
    }
}
impl AsMut<[u8]> for Retained {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.packet
    }
}
impl PacketStorage for Retained {
    fn allocation_bytes(&self) -> usize {
        self.packet
            .allocation_bytes()
            .saturating_add(self.original.allocation_bytes())
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
    fn take_vec_allocation(&mut self) -> Option<Vec<u8>> {
        self.packet.take_vec_allocation()
    }
}
