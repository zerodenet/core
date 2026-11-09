use super::bridge::dispatch_via_entry;
use super::model::PacketPathStartRequest;
use super::state::PacketPathManager;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_flow::packet_path::UdpFlowContext;
use crate::runtime::udp_flow::result::FlowFailure;

impl PacketPathManager {
    /// Start path: resolve carrier+datagram via the adapter registry, build on
    /// cache miss, encode + send. Takes the resolved leaves directly.
    pub(crate) async fn send(
        &mut self,
        ctx: UdpFlowContext<'_>,
        services: UdpAdapterContext<'_>,
        request: PacketPathStartRequest<'_>,
    ) -> Result<usize, FlowFailure> {
        let PacketPathStartRequest {
            carrier,
            datagram,
            packet,
            ..
        } = request;
        let upstream = carrier.upstream();
        let entry = self
            .ensure_entry(services, carrier, datagram, &mut *ctx.chain_tasks)
            .await
            .map_err(|error| FlowFailure {
                stage: "packet_path_establish",
                error,
                upstream: Some(upstream),
            })?;
        dispatch_via_entry(entry, ctx, packet).await
    }
}
