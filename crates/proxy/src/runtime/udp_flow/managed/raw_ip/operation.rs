use std::{future::Future, pin::Pin, sync::Arc};

use zero_core::Session;
use zero_engine::EngineError;

use crate::{
    protocol_registry::UdpAdapterContext,
    runtime::{
        raw_ip::{RawIpDevicePool, RawIpOutboundPlan},
        udp_dispatch::{operation::PreparedUdpFlowOperation, FlowFailure, UdpDispatch},
        udp_flow::{registered::LogicalConnection, result::FlowStartResult},
    },
};

use super::flow::RawIpUdpFlow;

pub(crate) struct RawIpUdpOperation {
    pub(crate) tag: String,
    pub(crate) identity: [u8; 32],
    pub(crate) plan: Arc<dyn RawIpOutboundPlan>,
    pub(crate) pool: Arc<RawIpDevicePool>,
}

impl PreparedUdpFlowOperation for RawIpUdpOperation {
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let services = ctx.runtime_services();
            let target = services
                .resolve_direct_targets(session)
                .await
                .map_err(|error| failure("raw_ip_resolve_target", error))?
                .udp_candidates()
                .first()
                .copied()
                .ok_or_else(|| failure("raw_ip_resolve_target", invalid("no target IP")))?;
            let peer = self
                .plan
                .peer_for_target(target.ip())
                .map_err(|error| failure("raw_ip_peer", error))?;
            let cell = self
                .pool
                .cell_for(
                    &self.tag,
                    peer.peer_index,
                    self.identity,
                    services.network().egress_generation(),
                )
                .map_err(|error| failure("raw_ip_device", error))?;
            let local_ip = peer.local_ip;
            if let Some(peer_id) = self.plan.peer_identity(peer.peer_index) {
                services.bind_outbound_peer_flow(session.id, &self.tag, &peer_id);
            }
            let device = cell
                .get()
                .ok_or_else(|| failure("raw_ip_device", invalid("peer device not initialized")))?;
            if !self.pool.is_current(
                &self.tag,
                peer.peer_index,
                self.identity,
                services.network().egress_generation(),
                device,
            ) {
                return Err(failure(
                    "raw_ip_device",
                    invalid("peer device changed during flow start"),
                ));
            }
            let flow = RawIpUdpFlow::open(
                device.clone(),
                session.target.clone(),
                session.port,
                target,
                local_ip,
                services.outbound_inner_io(&self.tag),
            )
            .map_err(|error| failure("raw_ip_stack", invalid(format!("{error:?}"))))?;
            dispatch
                .flow_start_context()
                .start_logical(
                    self.tag,
                    session,
                    payload,
                    LogicalConnection::from_flow(flow),
                )
                .await
        })
    }
}

fn invalid(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.to_string(),
    ))
}

fn failure(stage: &'static str, error: EngineError) -> FlowFailure {
    FlowFailure {
        stage,
        error,
        upstream: None,
    }
}
