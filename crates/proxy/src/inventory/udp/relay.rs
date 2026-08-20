use zero_engine::EngineError;

use super::super::ClaimedInventoryLeaf;
use super::super::{ClaimedRelayChain, ProtocolInventory};
use crate::protocol_registry::{OutboundAdapterContext, UdpAdapterContext};
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayChain;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::runtime::udp_flow::packet_path::{PacketPathFlowBinding, UdpPacketRef};
use crate::runtime::udp_flow::packet_path_chain::PacketPathStartRequest;

impl ProtocolInventory {
    pub(in crate::inventory) fn prepare_claimed_udp_packet_path_pair<'a>(
        &self,
        session_id: u64,
        carrier_leaf: &ClaimedInventoryLeaf<'a>,
        datagram_leaf: &ClaimedInventoryLeaf<'a>,
        packet: UdpPacketRef<'a>,
    ) -> Option<(PacketPathFlowBinding, PacketPathStartRequest<'a>)> {
        let carrier_operation = carrier_leaf.prepare_udp_packet_path()?;
        let datagram_operation = datagram_leaf.prepare_udp_packet_path()?;

        super::packet_path::build_udp_packet_path_pair(
            session_id,
            carrier_operation,
            datagram_operation,
            packet,
        )
    }
    pub(super) fn prepare_claimed_udp_relay_chain<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        session: &'a zero_core::Session,
        claimed_chain: &ClaimedRelayChain<'a>,
        payload: &'a [u8],
    ) -> Result<PreparedUdpRelayChain<'a>, FlowFailure> {
        self.validate_udp_relay_chain(ctx.clone(), claimed_chain)?;
        if claimed_chain.len() == 2 {
            if let Some((flow_binding, request)) = self.prepare_claimed_udp_packet_path_pair(
                session.id,
                claimed_chain.first(),
                claimed_chain.final_hop(),
                UdpPacketRef {
                    target: &session.target,
                    port: session.port,
                    payload,
                },
            ) {
                return Ok(PreparedUdpRelayChain::PacketPath {
                    flow_binding,
                    request: Box::new(request),
                });
            }
        }

        let final_hop = claimed_chain.final_hop().clone().into_claimed();
        let operation = final_hop.prepare_udp_relay(ctx.source_dir())?;
        let outbound_ctx = OutboundAdapterContext::new(ctx.config());

        if operation.needs_two_streams() {
            let post_prepared = self
                .prepare_claimed_tcp_relay_chain(outbound_ctx, claimed_chain)
                .map_err(flow_failure_from_tcp_outbound)?;
            let get_prepared = self
                .prepare_claimed_tcp_relay_chain(outbound_ctx, claimed_chain)
                .map_err(flow_failure_from_tcp_outbound)?;
            return Ok(PreparedUdpRelayChain::TwoStream {
                post_prefix: post_prepared,
                get_prefix: get_prepared,
                operation,
            });
        }

        let prepared_prefix = self
            .prepare_claimed_tcp_relay_chain(outbound_ctx, claimed_chain)
            .map_err(flow_failure_from_tcp_outbound)?;
        Ok(PreparedUdpRelayChain::FinalHop {
            prefix: prepared_prefix,
            operation,
        })
    }

    fn validate_udp_relay_chain(
        &self,
        ctx: UdpAdapterContext<'_>,
        chain: &ClaimedRelayChain<'_>,
    ) -> Result<(), FlowFailure> {
        for leaf in chain.leaves() {
            let runtime = leaf.runtime();
            if !ctx.udp_enabled_for_outbound(runtime.udp_policy_tag.as_deref()) {
                return Err(FlowFailure {
                    stage: "udp_policy",
                    error: EngineError::Io(std::io::Error::other(
                        "udp disabled for relay chain outbound",
                    )),
                    upstream: runtime.endpoint.map(|endpoint| endpoint.upstream()),
                });
            }
        }
        Ok(())
    }
}

fn flow_failure_from_tcp_outbound(failure: crate::transport::TcpOutboundFailure) -> FlowFailure {
    FlowFailure {
        stage: failure.stage,
        error: failure.error,
        upstream: failure.upstream_endpoint.map(|endpoint| *endpoint),
    }
}
