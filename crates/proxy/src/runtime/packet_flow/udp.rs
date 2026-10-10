use std::{future::Future, pin::Pin};

use super::contract::invalid;
use zero_core::Session;
use zero_engine::EngineError;

use crate::{
    protocol_registry::UdpAdapterContext,
    runtime::{
        packet_flow::PacketFlowAdapter,
        udp_dispatch::{operation::PreparedUdpFlowOperation, FlowFailure, UdpDispatch},
        udp_flow::{registered::LogicalConnection, result::FlowStartResult},
    },
};

use crate::runtime::udp_flow::managed::packet::PacketUdpFlow;

pub(super) struct PacketUdpOperation(pub(super) PacketFlowAdapter);

impl PreparedUdpFlowOperation for PacketUdpOperation {
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
            let targets = services
                .resolve_packet_targets(session)
                .await
                .map_err(|error| failure("raw_ip_resolve_target", error))?;
            let network = services.network();
            let generation = network.egress_generation();
            let (target, binding) = self
                .0
                .select(&targets, generation)
                .map_err(|error| failure("raw_ip_peer", error))?;
            if let Some(peer_id) = &binding.peer_id {
                services.bind_outbound_peer_flow(session.id, &self.0.tag, peer_id);
            }
            let flow = PacketUdpFlow::open(
                binding.endpoint,
                session.target.clone(),
                session.port,
                target,
                binding.local_ip,
                services.outbound_inner_io(&self.0.tag),
            )
            .map_err(|error| failure("raw_ip_stack", invalid(format!("{error:?}"))))?;
            if network.egress_generation() != generation {
                return Err(failure(
                    "raw_ip_device",
                    invalid("network changed during flow admission"),
                ));
            }
            dispatch
                .flow_start_context()
                .start_logical(
                    self.0.tag.to_string(),
                    session,
                    payload,
                    LogicalConnection::from_flow(flow),
                )
                .await
        })
    }
}

fn failure(stage: &'static str, error: EngineError) -> FlowFailure {
    FlowFailure {
        stage,
        error,
        upstream: None,
    }
}
