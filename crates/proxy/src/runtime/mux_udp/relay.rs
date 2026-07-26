use tracing::{info, warn};
use zero_core::{InboundMuxUdpRelay, InboundMuxUdpTermination};

use super::handler::MuxPacketSessionUdpHandler;
use super::{
    MuxUdpContinuityAttach, MuxUdpContinuityRegistry, MuxUdpContinuityScope,
    MuxUdpDetachedCancellation,
};
use crate::runtime::packet_session_udp::{
    run_packet_session_udp_relay, run_packet_session_udp_relay_with_dispatch,
    PacketSessionUdpFailurePolicy, PacketSessionUdpHandler, PacketSessionUdpLoopExit,
    PacketSessionUdpRelayRequest,
};
use crate::runtime::udp_delivery::log_completed_udp_flow;
use crate::runtime::udp_dispatch::UdpDispatch;
use crate::runtime::udp_ingress::UdpIngressRuntime;

const CONTINUITY_HANDOVER_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
const CONTINUITY_HANDOVER_POLL: std::time::Duration = std::time::Duration::from_millis(20);

pub(crate) async fn run_protocol_mux_udp_relay<R>(
    runtime: UdpIngressRuntime,
    continuity_registry: MuxUdpContinuityRegistry,
    mut relay: R,
    inbound_tag: &str,
    protocol: &'static str,
) where
    R: InboundMuxUdpRelay,
{
    let mux_session_id = relay.mux_session_id();
    let auth = relay.auth().cloned();
    let termination_probe = relay.termination_probe();
    let continuity_key = relay.continuity_key().cloned();
    let reconnectable = continuity_key.is_some() && termination_probe.is_some();
    let retention = runtime.services().udp_upstream_idle_timeout();

    if continuity_key.is_some() && !reconnectable {
        warn!(
            inbound_tag,
            protocol,
            mux_session_id,
            "mux udp continuity key ignored because no termination probe is available"
        );
    }

    if !reconnectable {
        run_non_reconnectable_relay(runtime, relay, inbound_tag, protocol, auth).await;
        return;
    }

    let pruned = continuity_registry.prune_expired();
    for dispatch in pruned.dispatches {
        settle_dispatch(dispatch);
    }
    if pruned.removed > 0 {
        info!(
            inbound_tag,
            protocol,
            continuity_pruned = pruned.removed,
            "expired mux udp continuity sessions settled"
        );
    }

    let scope = MuxUdpContinuityScope::new(
        inbound_tag,
        protocol,
        auth.as_ref().and_then(|auth| auth.principal_key.as_deref()),
        continuity_key.expect("reconnectable relay has continuity key"),
    );
    let (generation, dispatch) = match attach_continuity_with_grace(
        &continuity_registry,
        &scope,
        retention,
        inbound_tag,
        protocol,
        mux_session_id,
    )
    .await
    {
        MuxUdpContinuityAttach::Conflict { generation } => {
            warn!(
                inbound_tag,
                protocol, mux_session_id, generation, "mux udp continuity conflict rejected"
            );
            let _ = relay.end_inbound_stream();
            return;
        }
        MuxUdpContinuityAttach::New { generation } => {
            info!(
                inbound_tag,
                protocol, mux_session_id, generation, "mux udp continuity registered"
            );
            (generation, None)
        }
        MuxUdpContinuityAttach::Reattached {
            generation,
            dispatch,
        } => {
            info!(
                inbound_tag,
                protocol, mux_session_id, generation, "mux udp continuity transport reattached"
            );
            (generation, dispatch)
        }
    };

    let dispatch = match dispatch {
        Some(dispatch) => dispatch,
        None => match runtime.new_dispatch(inbound_tag).await {
            Ok(dispatch) => dispatch,
            Err(error) => {
                warn!(
                    inbound_tag,
                    protocol,
                    mux_session_id,
                    generation,
                    error = %error,
                    "mux udp continuity dispatch init failed"
                );
                let _ = continuity_registry.finish(&scope, generation);
                let _ = relay.end_inbound_stream();
                return;
            }
        },
    };

    info!(
        inbound_tag = inbound_tag,
        protocol = protocol,
        mux_session_id,
        reconnectable,
        generation,
        "mux udp sub-stream started"
    );

    let handler = MuxPacketSessionUdpHandler { relay };
    let exit = run_packet_session_udp_relay_with_dispatch(
        runtime,
        PacketSessionUdpRelayRequest {
            handler,
            inbound_tag,
            protocol,
            auth,
            failure_policy: PacketSessionUdpFailurePolicy::ReturnError,
        },
        dispatch,
    )
    .await;

    let explicit_end = termination_probe
        .is_some_and(|probe| probe.reason() == InboundMuxUdpTermination::ExplicitEnd);
    let terminal = explicit_end
        || matches!(
            &exit.outcome,
            Ok(PacketSessionUdpLoopExit::IdleTimeout)
                | Ok(PacketSessionUdpLoopExit::AssociationCancelled)
        );

    if let Err(error) = &exit.outcome {
        warn!(
            inbound_tag,
            protocol,
            mux_session_id,
            generation,
            error = %error,
            "mux udp relay transport exited with error"
        );
    }

    let mut handler = exit.handler;
    if terminal {
        let _ = continuity_registry.finish(&scope, generation);
        settle_dispatch(exit.dispatch);
        let _ = handler.finish().await;
    } else if let Err(dispatch) =
        continuity_registry.detach(&scope, generation, retention, Some(exit.dispatch))
    {
        if let Some(dispatch) = dispatch {
            settle_dispatch(dispatch);
        }
        warn!(
            inbound_tag,
            protocol,
            mux_session_id,
            generation,
            "mux udp continuity detach lost its active generation"
        );
    } else {
        schedule_continuity_expiry(
            continuity_registry.clone(),
            scope.clone(),
            generation,
            retention,
            inbound_tag.to_owned(),
            protocol,
        );
    }

    let snapshot = continuity_registry.snapshot();
    info!(
        inbound_tag,
        protocol,
        mux_session_id,
        generation,
        continuity_attached = snapshot.attached,
        continuity_retained = snapshot.retained,
        explicit_end,
        terminal,
        "mux udp continuity relay ended"
    );
}

async fn run_non_reconnectable_relay<R>(
    runtime: UdpIngressRuntime,
    relay: R,
    inbound_tag: &str,
    protocol: &'static str,
    auth: Option<zero_core::SessionAuth>,
) where
    R: InboundMuxUdpRelay,
{
    let mux_session_id = relay.mux_session_id();
    info!(
        inbound_tag = inbound_tag,
        protocol = protocol,
        mux_session_id,
        reconnectable = false,
        "mux udp sub-stream started"
    );

    let handler = MuxPacketSessionUdpHandler { relay };
    let _ = run_packet_session_udp_relay(
        runtime,
        PacketSessionUdpRelayRequest {
            handler,
            inbound_tag,
            protocol,
            auth,
            failure_policy: PacketSessionUdpFailurePolicy::LogAndBreak,
        },
    )
    .await;
}

fn settle_dispatch(dispatch: UdpDispatch) {
    for completed in dispatch.finish_all() {
        log_completed_udp_flow(completed);
    }
}

async fn attach_continuity_with_grace(
    registry: &MuxUdpContinuityRegistry,
    scope: &MuxUdpContinuityScope,
    retention: std::time::Duration,
    inbound_tag: &str,
    protocol: &'static str,
    mux_session_id: u16,
) -> MuxUdpContinuityAttach {
    let deadline = tokio::time::Instant::now() + CONTINUITY_HANDOVER_GRACE;
    let mut waiting_logged = false;
    loop {
        let attach = registry.attach(scope.clone(), retention);
        if !matches!(&attach, MuxUdpContinuityAttach::Conflict { .. }) {
            return attach;
        }
        if tokio::time::Instant::now() >= deadline {
            return attach;
        }
        if !waiting_logged {
            info!(
                inbound_tag,
                protocol,
                mux_session_id,
                handover_grace_ms = CONTINUITY_HANDOVER_GRACE.as_millis(),
                "mux udp continuity waiting for previous transport to detach"
            );
            waiting_logged = true;
        }
        tokio::time::sleep(CONTINUITY_HANDOVER_POLL).await;
    }
}

fn schedule_continuity_expiry(
    registry: MuxUdpContinuityRegistry,
    scope: MuxUdpContinuityScope,
    generation: u64,
    retention: std::time::Duration,
    inbound_tag: String,
    protocol: &'static str,
) {
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + retention;
        let mut cancellation_poll = tokio::time::interval(std::time::Duration::from_millis(100));
        cancellation_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    let Some(dispatch) = registry.expire(&scope, generation) else {
                        return;
                    };
                    settle_dispatch(dispatch);
                    let snapshot = registry.snapshot();
                    info!(
                        inbound_tag,
                        protocol,
                        generation,
                        continuity_attached = snapshot.attached,
                        continuity_retained = snapshot.retained,
                        "mux udp continuity retention expired and dispatch settled"
                    );
                    return;
                }
                _ = cancellation_poll.tick() => {
                    match registry.poll_detached_cancellation(&scope, generation) {
                        MuxUdpDetachedCancellation::Retained => {}
                        MuxUdpDetachedCancellation::Cancelled(dispatch) => {
                            settle_dispatch(*dispatch);
                            let snapshot = registry.snapshot();
                            info!(
                                inbound_tag,
                                protocol,
                                generation,
                                continuity_attached = snapshot.attached,
                                continuity_retained = snapshot.retained,
                                "mux udp detached continuity session cancelled and settled"
                            );
                            return;
                        }
                        MuxUdpDetachedCancellation::Gone => return,
                    }
                }
            }
        }
    });
}
