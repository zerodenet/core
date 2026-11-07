//! Native RX custody prepays both the same return FIFO and actual source codec
//! before enqueue. Its producing driver parks one original while TX still runs.
use super::*;
use derived::ReturnAdmissionFailure;
use zero_stack::packet_output::memory::{prepare_transmit_layout, PacketWireMemory};
use zero_traits::PacketDeriveError;
impl ReturnBudget {
    pub(in crate::runtime::raw_ip::device::returns) fn retain_transmit_return(
        &self,
        mut packet: PacketBuffer,
        mtu: u16,
        layout: PacketWireMemory,
    ) -> Result<PacketBuffer, ReturnAdmissionFailure> {
        if let Err(cause) = prepare_transmit_layout(&mut packet, usize::from(mtu), layout) {
            return Err(ReturnAdmissionFailure {
                original: packet,
                cause: if cause == PacketDeriveError::Busy {
                    None
                } else {
                    Some(cause)
                },
            });
        }
        let lengths = match zero_stack::packet::fragment_packet_lengths(&packet, usize::from(mtu)) {
            Ok(lengths) => lengths,
            Err(cause) => {
                return Err(ReturnAdmissionFailure {
                    original: packet,
                    cause: Some(cause),
                })
            }
        };
        self.retain_planned(packet, lengths, true)
    }
    pub(in crate::runtime::raw_ip::device::returns) async fn retain_transmit_return_wait(
        &self,
        mut packet: PacketBuffer,
        mtu: u16,
        layout: PacketWireMemory,
    ) -> Result<PacketBuffer, ReturnAdmissionFailure> {
        loop {
            let local = self.0.changed.notified();
            let total = self.0.totals.notified();
            tokio::pin!(local);
            tokio::pin!(total);
            local.as_mut().enable();
            total.as_mut().enable();
            match self.retain_transmit_return(packet, mtu, layout) {
                Ok(packet) => return Ok(packet),
                Err(failure) if failure.cause.is_some() => return Err(failure),
                Err(failure) => packet = failure.original,
            }
            tokio::select! {_=local=>{},_=total=>{}}
        }
    }
}
