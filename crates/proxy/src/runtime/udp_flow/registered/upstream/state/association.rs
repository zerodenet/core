//! One actual standard Datagram association, with typed generation identity.
use super::super::{
    contract::{DatagramAssociationTarget, UpstreamAssociationSend, UpstreamAssociationTarget},
    native::NativeAssociation,
    runtime::{association::UpstreamAssociationRuntime, upstream_flow_mismatch},
};
use crate::runtime::udp_flow::{response::UpstreamUdpResponse, result::FlowFailure};
use std::time::Duration;
use tokio::time::Instant as TokioInstant;
use zero_engine::EngineError;
#[derive(Default)]
pub(in crate::runtime::udp_flow::registered) struct UpstreamAssociationState {
    runtime: UpstreamAssociationRuntime<DatagramAssociationTarget, NativeAssociation>,
}
impl UpstreamAssociationState {
    pub(in crate::runtime::udp_flow::registered) async fn start_upstream_flow(
        &mut self,
        inbound_tag: &str,
        request: UpstreamAssociationSend<'_>,
    ) -> Result<usize, FlowFailure> {
        let Some(services) = request.services else {
            return Err(upstream_flow_mismatch(
                "udp_upstream_proxy",
                request.server,
                request.port,
                "expected proxy context for registered upstream UDP flow",
            ));
        };
        self.runtime
            .send_packet(
                &services,
                inbound_tag,
                request.target,
                request.session,
                request.payload,
            )
            .await
            .map_err(|error| FlowFailure {
                stage: "udp_upstream_send",
                error,
                upstream: Some((request.server.to_owned(), request.port)),
            })
    }
    pub(in crate::runtime::udp_flow::registered) async fn recv_upstream_response(
        &self,
        buf: &mut [u8],
    ) -> Result<UpstreamUdpResponse, EngineError> {
        self.runtime.recv_upstream_response(buf).await
    }
}

impl UpstreamAssociationState {
    pub(in crate::runtime::udp_flow::registered) fn upstream_outbound_tag(&self) -> Option<&str> {
        self.runtime.upstream_outbound_tag()
    }

    pub(in crate::runtime::udp_flow::registered) fn upstream_idle_deadline(
        &self,
    ) -> Option<TokioInstant> {
        self.runtime.idle_deadline()
    }
}

impl UpstreamAssociationState {
    pub(in crate::runtime::udp_flow::registered) fn touch_upstream_idle(
        &mut self,
        timeout: Duration,
    ) {
        if self.runtime.upstream_outbound_tag().is_some() {
            self.runtime.touch_idle(timeout);
        }
    }
    pub(in crate::runtime::udp_flow::registered) fn drop_upstream_association(
        &mut self,
    ) -> Option<(String, String, u16)> {
        self.runtime.close_dropped().map(log_parts)
    }
    pub(in crate::runtime::udp_flow::registered) fn close_idle_upstream(
        &mut self,
    ) -> Option<(String, String, u16)> {
        self.runtime.close_idle().map(log_parts)
    }
    pub(in crate::runtime::udp_flow::registered) fn close_all_upstreams(&mut self) {
        self.runtime.close_all_upstreams();
    }
}

fn log_parts(target: DatagramAssociationTarget) -> (String, String, u16) {
    let (tag, server, port) = target.log_parts();
    (tag.to_owned(), server.to_owned(), port)
}
