use std::time::Duration;

use tokio::time::Instant as TokioInstant;
use tracing::info;
use zero_core::SessionAuth;
use zero_engine::EngineError;

#[cfg(feature = "upstream-association-runtime")]
use super::with_upstream::run_loop;
#[cfg(not(feature = "upstream-association-runtime"))]
use super::without_upstream::run_loop;
use crate::runtime::packet_session_udp::contract::{
    PacketSessionUdpFailurePolicy, PacketSessionUdpHandler, PacketSessionUdpRelayRequest,
};
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_ingress::UdpIngressRuntime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PacketSessionUdpLoopExit {
    InboundEnded,
    IdleTimeout,
    AssociationCancelled,
}

pub(crate) struct PacketSessionUdpRelayExit<H> {
    pub(crate) handler: H,
    pub(crate) dispatch: UdpDispatch,
    pub(crate) outcome: Result<PacketSessionUdpLoopExit, EngineError>,
}

pub(super) struct PacketSessionUdpLoopContext<'a> {
    pub(super) runtime: &'a UdpIngressRuntime,
    pub(super) inbound_tag: &'a str,
    pub(super) protocol: &'static str,
    pub(super) auth: Option<&'a SessionAuth>,
    pub(super) failure_policy: PacketSessionUdpFailurePolicy,
    pub(super) timeout: Duration,
}

pub(crate) async fn run_packet_session_udp_relay_with_dispatch<H>(
    runtime: UdpIngressRuntime,
    request: PacketSessionUdpRelayRequest<'_, H>,
    mut dispatch: UdpDispatch,
) -> PacketSessionUdpRelayExit<H>
where
    H: PacketSessionUdpHandler,
{
    let PacketSessionUdpRelayRequest {
        mut handler,
        inbound_tag,
        protocol,
        auth,
        failure_policy,
    } = request;

    let timeout = runtime.services().udp_upstream_idle_timeout();
    let mut last_activity = TokioInstant::now();
    let mut direct_buf = vec![0_u8; 64 * 1024];
    #[cfg(feature = "upstream-association-runtime")]
    let mut upstream_buf = vec![0_u8; 64 * 1024];

    let context = PacketSessionUdpLoopContext {
        runtime: &runtime,
        inbound_tag,
        protocol,
        auth: auth.as_ref(),
        failure_policy,
        timeout,
    };

    info!(
        inbound_tag = inbound_tag,
        protocol = protocol,
        "packet session udp relay started"
    );

    #[cfg(feature = "upstream-association-runtime")]
    let outcome = run_loop(
        &context,
        &mut handler,
        &mut dispatch,
        &mut last_activity,
        direct_buf.as_mut_slice(),
        upstream_buf.as_mut_slice(),
    )
    .await;

    #[cfg(not(feature = "upstream-association-runtime"))]
    let outcome = run_loop(
        &context,
        &mut handler,
        &mut dispatch,
        &mut last_activity,
        direct_buf.as_mut_slice(),
    )
    .await;

    PacketSessionUdpRelayExit {
        handler,
        dispatch,
        outcome,
    }
}
