use crate::runtime::route_runtime::MuxSubstreamRuntime;
use crate::runtime::sniff::{sniff_mux_tcp_session, SniffingPolicy};
use tracing::warn;
use zero_core::{InboundMuxTcpRelay, Session};

pub(crate) struct MuxTcpStreamTask<B> {
    pub(crate) session: Session,
    pub(crate) bridge: B,
    pub(crate) protocol: &'static str,
}

pub(crate) async fn run_mux_tcp_stream_task<B>(
    runtime: MuxSubstreamRuntime,
    request: MuxTcpStreamTask<B>,
    sniffing: Option<SniffingPolicy>,
) where
    B: InboundMuxTcpRelay,
{
    let MuxTcpStreamTask {
        mut session,
        mut bridge,
        protocol,
    } = request;
    let mux_session_id = bridge.mux_session_id();

    let replay_prefix = match sniffing {
        Some(policy) => {
            let fake_dns_fallback = runtime.apply_sniffing_metadata(&policy, &mut session).await;
            if session.target_host_source == Some(zero_core::TargetHostSource::FakeIp) {
                Vec::new()
            } else {
                sniff_mux_tcp_session(&policy, &mut session, &mut bridge, fake_dns_fallback).await
            }
        }
        None => Vec::new(),
    };

    let upstream = match runtime.open_tcp_upstream(&mut session).await {
        Ok(result) => result.upstream,
        Err(error) => {
            warn!(%error, mux_session_id, protocol, "mux tcp dispatch failed");
            bridge.close_stream().await;
            return;
        }
    };

    bridge
        .relay_stream_with_prefix(upstream, replay_prefix)
        .await;
}

pub(crate) async fn run_protocol_mux_tcp_task<B>(
    runtime: MuxSubstreamRuntime,
    session: Session,
    bridge: B,
    protocol: &'static str,
) where
    B: InboundMuxTcpRelay,
{
    run_protocol_mux_tcp_task_with_sniffing(runtime, session, bridge, protocol, None).await;
}

pub(crate) async fn run_protocol_mux_tcp_task_with_sniffing<B>(
    runtime: MuxSubstreamRuntime,
    session: Session,
    bridge: B,
    protocol: &'static str,
    sniffing: Option<SniffingPolicy>,
) where
    B: InboundMuxTcpRelay,
{
    run_mux_tcp_stream_task(
        runtime,
        MuxTcpStreamTask {
            session,
            bridge,
            protocol,
        },
        sniffing,
    )
    .await;
}
