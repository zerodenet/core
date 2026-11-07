//! These concrete buffers belong to this one already bounded echo operation.
use super::*;
use zero_traits::{PacketBuffer, PacketDeriveError};
pub(in crate::runtime::inbound_operation::raw_ip::icmp) struct Prepared {
    pub(super) read: zero_tun::packet_memory::NativeReadGrant,
    message: PacketBuffer,
    reply: PacketBuffer,
}
pub(in crate::runtime::inbound_operation::raw_ip::icmp) fn prepare(
    packet: &mut PacketBuffer,
    mtu: u16,
    responses: &zero_stack::packet_output::PacketSender,
) -> std::io::Result<Prepared> {
    let request = packet::parse_icmp_echo_request(packet)
        .ok_or_else(|| std::io::Error::other("invalid owned Echo"))?;
    let message = request.message.len();
    let length = message
        .checked_add(if request.source.is_ipv4() { 20 } else { 40 })
        .filter(|n| *n <= usize::from(mtu))
        .ok_or_else(|| memory(PacketDeriveError::Capacity))?;
    let view = responses
        .packet_memory_view()
        .ok_or_else(|| memory(PacketDeriveError::Unreserved))?;
    let wire = responses
        .wire_memory()
        .ok_or_else(|| memory(PacketDeriveError::Unreserved))?;
    let mut lengths = vec![message, length];
    lengths.extend(wire.workspace_lengths(length).map_err(memory)?);
    let bytes = lengths
        .iter()
        .try_fold(0usize, |sum, n| sum.checked_add(*n))
        .ok_or_else(|| memory(PacketDeriveError::Overflow))?;
    packet
        .prepare_transmit(lengths.len(), bytes)
        .map_err(memory)?;
    let mut buffers = packet
        .try_derive_zeroed(&[message, length])
        .map_err(memory)?
        .into_iter();
    let message = buffers
        .next()
        .ok_or_else(|| memory(PacketDeriveError::LayoutMismatch))?;
    let reply = buffers
        .next()
        .ok_or_else(|| memory(PacketDeriveError::LayoutMismatch))?;
    if buffers.next().is_some() {
        return Err(memory(PacketDeriveError::LayoutMismatch));
    }
    let read = view
        .native_workspace(usize::from(mtu).max(1280))
        .map_err(std::io::Error::other)?;
    Ok(Prepared {
        read,
        message,
        reply,
    })
}
pub(super) async fn exchange(
    socket: &mut ProbeSocket,
    packet: &[u8],
    mtu: u16,
    mut prepaid: Prepared,
    mut read: zero_tun::packet_memory::NativeReadBuffer,
) -> std::io::Result<Option<PacketBuffer>> {
    let Some(request) = packet::parse_icmp_echo_request(packet) else {
        return Ok(None);
    };
    let local = socket.connect(request.destination).await?;
    let id = NEXT_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    if packet::write_icmp_echo_probe(&request, id, local, &mut prepaid.message).is_none() {
        return Ok(None);
    }
    socket.send(&prepaid.message).await?;
    let id = socket.reply_identifier(id)?;
    let end = tokio::time::Instant::now() + ECHO_TIMEOUT;
    loop {
        let (size, source) =
            match tokio::time::timeout_at(end, socket.recv_from(read.as_mut_bytes())).await {
                Ok(Ok(actual)) => actual,
                Ok(Err(source)) => return Err(source),
                Err(_) => return Ok(None),
            };
        if source != request.destination {
            continue;
        }
        let Some(spec) = packet::IcmpEchoPacketSpec::probe_reply(
            &request,
            id,
            &read.as_bytes()[..size],
            local,
            usize::from(mtu),
        ) else {
            continue;
        };
        spec.write_into(&mut prepaid.reply).map_err(memory)?;
        return Ok(Some(prepaid.reply));
    }
}
pub(in crate::runtime::inbound_operation::raw_ip::icmp) fn retain_reply(
    packet: PacketBuffer,
    original: Arc<PacketBuffer>,
) -> PacketBuffer {
    PacketBuffer::from_owner(Retained { packet, original })
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
impl zero_traits::PacketStorage for Retained {
    fn allocation_bytes(&self) -> usize {
        self.packet
            .allocation_bytes()
            .saturating_add(self.original.allocation_bytes())
    }
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
}
fn memory(source: PacketDeriveError) -> std::io::Error {
    std::io::Error::new(
        if source == PacketDeriveError::Busy {
            std::io::ErrorKind::WouldBlock
        } else {
            std::io::ErrorKind::Other
        },
        source,
    )
}
