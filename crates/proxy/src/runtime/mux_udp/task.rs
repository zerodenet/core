use zero_core::InboundMuxUdpRelay;

use super::relay::run_protocol_mux_udp_relay;
use crate::runtime::route_runtime::MuxSubstreamRuntime;

#[cfg(feature = "managed-stream-runtime")]
pub(crate) async fn run_protocol_mux_udp_task_with_sniffing<R>(
    runtime: MuxSubstreamRuntime,
    relay: R,
    protocol: &'static str,
    sniffing: Option<crate::runtime::sniff::SniffingPolicy>,
) where
    R: InboundMuxUdpRelay,
{
    run_protocol_mux_udp_relay(
        runtime.udp_runtime(),
        runtime.mux_udp_continuity(),
        relay,
        runtime.inbound_tag().to_owned(),
        protocol,
        sniffing,
    )
    .await;
}
