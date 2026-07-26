use tracing::info;
use zero_core::InboundMuxUdpRelay;

use super::relay::run_protocol_mux_udp_relay;
use crate::runtime::route_runtime::MuxSubstreamRuntime;

#[cfg(feature = "managed-stream-runtime")]
pub(crate) async fn run_protocol_mux_udp_task<R>(
    runtime: MuxSubstreamRuntime,
    relay: R,
    protocol: &'static str,
) where
    R: InboundMuxUdpRelay,
{
    run_protocol_mux_udp_relay(
        runtime.udp_runtime(),
        runtime.mux_udp_continuity(),
        relay,
        runtime.inbound_tag(),
        protocol,
    )
    .await;
}

#[cfg(feature = "managed-stream-runtime")]
pub(crate) async fn run_protocol_mux_udp_task_with_accept_log<R>(
    runtime: MuxSubstreamRuntime,
    relay: R,
    protocol: &'static str,
    accept_log_message: Option<&'static str>,
) where
    R: InboundMuxUdpRelay,
{
    if let Some(message) = accept_log_message {
        info!(
            inbound_tag = runtime.inbound_tag(),
            network = "udp",
            "{message}"
        );
    }
    run_protocol_mux_udp_relay(
        runtime.udp_runtime(),
        runtime.mux_udp_continuity(),
        relay,
        runtime.inbound_tag(),
        protocol,
    )
    .await;
}
