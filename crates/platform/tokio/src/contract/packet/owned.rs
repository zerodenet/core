//! Owned admission on the existing Packet role, without a second protocol SPI.
use super::*;
use crate::contract::{DeliveryState, Impact, Reason, Stage};

/// The unconsumed original is returned whenever no submission is proven.
/// An original returned with Unknown is still not permission to retry.
#[derive(Debug)]
pub struct OwnedSendFailure {
    pub failure: Box<SendFailure>,
    pub packet: Option<IpPacket>,
}

pub async fn send_owned<T: PacketTx + ?Sized>(
    mut tx: Pin<&mut T>,
    control: &mut dyn DynChannelControl,
    packet: IpPacket,
    limits: &PacketLimits,
) -> Result<SendReceipt, OwnedSendFailure> {
    if let Err(failure) = limits.validate(&packet) {
        return Err(OwnedSendFailure {
            failure: Box::new(failure),
            packet: Some(packet),
        });
    }
    // Non-owning identity facts only: no reference/alias survives the transfer.
    let pointer = packet.payload().as_ptr() as usize;
    let length = packet.payload().len();
    let family = packet.family();
    let ingress = packet.ingress();
    let association = packet.return_association.clone();
    let mut packet = Some(packet);
    let mut guard = PendingSend {
        control,
        armed: false,
    };
    poll_fn(|cx| {
        guard.armed = true;
        let result = tx.as_mut().poll_send_owned(cx, &mut packet);
        let original_returned = packet.as_ref().is_some_and(|actual| {
            actual.payload().as_ptr() as usize == pointer
                && actual.payload().len() == length
                && actual.family() == family
                && actual.ingress() == ingress
                && actual.return_association == association
        });
        let result = match result {
            Poll::Pending if packet.is_none() => return Poll::Pending,
            // Pending without custody, acceptance without consumption, and
            // NotSubmitted without the same original are contract violations.
            // Preserve any returned packet and fence this logical session.
            Poll::Pending => Err(unknown()),
            Poll::Ready(Ok(_)) if packet.is_some() => Err(unknown()),
            Poll::Ready(Ok(receipt)) => {
                super::super::datagram::check_receipt(receipt, length, guard.control)
            }
            Poll::Ready(Err(failure))
                if failure.delivery == DeliveryState::NotSubmitted && !original_returned =>
            {
                Err(unknown())
            }
            Poll::Ready(Err(failure)) => Err(failure),
        };
        guard.armed = false;
        Poll::Ready(result.map_err(|failure| OwnedSendFailure {
            failure: Box::new(super::super::datagram::check_failure(failure, guard.control)),
            packet: packet.take(),
        }))
    })
    .await
}
fn unknown() -> SendFailure {
    SendFailure::new(
        ChannelError::new(Stage::Send, Reason::Internal, Impact::Session),
        DeliveryState::Unknown,
    )
}

#[cfg(test)]
mod tests;
