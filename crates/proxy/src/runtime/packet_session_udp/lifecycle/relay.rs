use std::time::Duration;

use tokio::time::Instant as TokioInstant;
use tracing::info;
use zero_core::SessionAuth;
use zero_engine::EngineError;

use super::failure::handle_runtime_failure;
#[cfg(feature = "upstream-association-runtime")]
use super::with_upstream::run_loop;
#[cfg(not(feature = "upstream-association-runtime"))]
use super::without_upstream::run_loop;
use crate::runtime::packet_session_udp::contract::{
    PacketSessionUdpFailurePolicy, PacketSessionUdpHandler, PacketSessionUdpRelayRequest,
};
use crate::runtime::udp_delivery::log_completed_udp_flow;
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

pub(crate) async fn run_packet_session_udp_relay<H>(
    runtime: UdpIngressRuntime,
    request: PacketSessionUdpRelayRequest<'_, H>,
) -> Result<(), EngineError>
where
    H: PacketSessionUdpHandler,
{
    let PacketSessionUdpRelayRequest {
        handler,
        inbound_tag,
        protocol,
        auth,
        failure_policy,
    } = request;

    let mut dispatch = match runtime.new_dispatch(inbound_tag).await {
        Ok(dispatch) => dispatch,
        Err(error) => {
            let mut handler = handler;
            return handle_runtime_failure(
                &mut handler,
                failure_policy,
                inbound_tag,
                protocol,
                "packet session udp dispatch init failed",
                error,
            )
            .await;
        }
    };

    let exit = run_packet_session_udp_relay_with_dispatch(
        runtime,
        PacketSessionUdpRelayRequest {
            handler,
            inbound_tag,
            protocol,
            auth,
            failure_policy,
        },
        dispatch,
    )
    .await;

    dispatch = exit.dispatch;
    for completed in dispatch.finish_all() {
        log_completed_udp_flow(completed);
    }

    let mut handler = exit.handler;
    let _ = handler.finish().await;

    info!(
        inbound_tag = inbound_tag,
        protocol = protocol,
        "packet session udp relay ended"
    );

    exit.outcome.map(|_| ())
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
