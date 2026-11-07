//! The same wire loop holds one received frame during return/main admission.
//! There is no receiver task/pump; TX, timers and stop remain in its select.
use super::*;
pub(super) struct ReadMemory(
    pub [zero_tun::packet_memory::NativeReadGrant; 2],
    pub zero_tun::packet_memory::NativeReadGrant,
);
impl Drop for ReadMemory {
    fn drop(&mut self) {
        for grant in self.0.iter().chain(std::iter::once(&self.1)) {
            grant.revoke();
        }
    }
}
pub(super) struct Delivery {
    pub packet: zero_traits::PacketBuffer,
    pub device: Option<Arc<crate::runtime::raw_ip::SharedRawIpDevice>>,
    pub local_destination: bool,
    pub mtu: u16,
    pub peer: Option<usize>,
    pub traffic: WireTraffic,
    pub facts: zero_core::packet::PacketFacts,
    pub packets: mpsc::Sender<zero_core::packet::IpPacket>,
}
impl Delivery {
    pub async fn run(self) -> Result<(), EngineError> {
        let Self {
            mut packet,
            device,
            local_destination,
            mtu,
            peer,
            traffic,
            facts,
            packets,
        } = self;
        let allowed = traffic.allowed();
        if let Some(device) = device {
            let local_echo =
                local_destination && packet::parse_icmp_echo_request(&packet).is_some();
            if !local_echo {
                match device.deliver_decrypted_owned_wait(packet, allowed).await? {
                    None => return Ok(()),
                    Some(unmatched) => packet = unmatched,
                }
            }
        }
        if !allowed {
            traffic.peer(peer).dropped_reason(
                TrafficPlane::Inner,
                true,
                zero_api::TrafficDropReason::PolicyRejected,
            );
            return Ok(());
        }
        // Snapshot facts/peer identity/generation were captured from this same
        // authenticated receive. Never bind a parked frame to a later peer.
        deliver(
            &packets,
            packet,
            local_destination,
            mtu,
            peer,
            &traffic,
            &facts,
        )
        .await;
        Ok(())
    }
}

async fn deliver(
    sender: &mpsc::Sender<zero_core::packet::IpPacket>,
    packet: zero_traits::PacketBuffer,
    local_destination: bool,
    mtu: u16,
    peer: Option<usize>,
    traffic: &WireTraffic,
    facts: &zero_core::packet::PacketFacts,
) {
    let association = zero_core::packet::ReturnAssociation {
        endpoint: facts.endpoint,
        generation: facts.inbound.generation,
        peer: traffic.identity(peer).map(zero_core::component::PeerId),
        correlation: traffic.generation(),
    };
    let value = zero_core::packet::IpPacket::new(packet, association).map(|packet| {
        packet.with_ingress(zero_core::packet::PacketIngress {
            local_destination,
            mtu,
        })
    });
    match value {
        Ok(value) => {
            if sender.send(value).await.is_err() {
                traffic.peer(peer).dropped_reason(
                    TrafficPlane::Inner,
                    peer.is_some(),
                    zero_api::TrafficDropReason::QueueClosed,
                );
            }
        }
        Err(_) => traffic.peer(peer).dropped_reason(
            TrafficPlane::Inner,
            peer.is_some(),
            zero_api::TrafficDropReason::InvalidPacket,
        ),
    }
}

/// This same wire caller receives the unchanged original on failure. Releasing
/// that failed operation releases only memory, never native/Scope obligations.
pub(super) fn fragment_failure(
    failure: zero_stack::packet::PacketFragmentFailure,
) -> std::io::Error {
    use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
    use zero_traits::PacketDeriveError;
    let zero_stack::packet::PacketFragmentFailure { original, cause } = failure;
    drop(original);
    let reason = match cause {
        PacketDeriveError::Capacity | PacketDeriveError::Busy | PacketDeriveError::Overflow => {
            Reason::Capacity
        }
        PacketDeriveError::Revoked => Reason::Revoked,
        PacketDeriveError::Unreserved => Reason::InvalidConfig,
        PacketDeriveError::LayoutMismatch | PacketDeriveError::AlreadyDerived => Reason::Protocol,
    };
    std::io::Error::other(ChannelError::new(Stage::Send, reason, Impact::Packet).with_source(cause))
}
