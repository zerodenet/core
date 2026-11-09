use super::super::model::Entry;
use crate::runtime::udp_flow::packet_path::{UdpFlowContext, UdpPacketRef};
use crate::runtime::udp_flow::result::FlowFailure;

/// The same registered association receiver forwards data to its original
/// response owner. No new task or oneshot bridge is started for a packet.
pub(in crate::runtime::udp_flow::packet_path_chain) async fn dispatch_via_entry(
    entry: &Entry,
    ctx: UdpFlowContext<'_>,
    packet_ref: UdpPacketRef<'_>,
) -> Result<usize, FlowFailure> {
    let failure = |stage, error| FlowFailure {
        stage,
        error,
        upstream: Some(entry.datagram_endpoint.upstream()),
    };
    if let Some(source) = entry.receiving.failure() {
        return Err(failure(
            "packet_path_receive",
            std::io::Error::other(source).into(),
        ));
    }
    let path = entry.path.as_ref().ok_or_else(|| {
        failure(
            "packet_path_retiring",
            std::io::Error::other("same original packet path is still retiring").into(),
        )
    })?;
    let packet = entry
        .memory
        .encode(
            entry.codec.as_ref(),
            packet_ref.target,
            packet_ref.port,
            packet_ref.payload,
        )
        .map_err(|error| failure("packet_path_encode", error))?;
    let reply = ctx
        .chain_tasks
        .packet_sender(&entry.memory)
        .map_err(|error| failure("packet_path_response", error))?;
    let waiter = entry
        .waiters
        .insert(packet_ref.target, packet_ref.port, reply, ctx.session_id)
        .map_err(|error| failure("packet_path_response", error))?;
    let datagram_target = entry.datagram_endpoint.target();
    path.send_to(&datagram_target, entry.datagram_endpoint.port(), &packet)
        .await
        .map_err(|error| failure("packet_path_send", error))?;
    waiter.commit();
    Ok(packet_ref.payload.len())
}
