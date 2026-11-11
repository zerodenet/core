use super::super::ProtocolInventory;
use crate::protocol_registry::{OutboundAdapterContext, UdpAdapterContext};
use crate::runtime::udp_dispatch::relay::PreparedUdpRelayChain;
use crate::runtime::udp_dispatch::FlowFailure;
use crate::runtime::udp_flow::packet_path::UdpPacketRef;
use zero_engine::ResolvedLeafOutbound;

impl ProtocolInventory {
    pub(super) fn prepare_udp_relay_continuation<'a>(
        &self,
        session: &'a zero_core::Session,
        prepared: crate::inventory::tcp::PreparedTcpRelayChain,
        claimed: crate::inventory::ClaimedInventoryLeaf<'a>,
        payload: &'a [u8],
    ) -> Result<PreparedUdpRelayChain<'a>, FlowFailure> {
        let prefix = prepared.datagram_prefixes.last().cloned().flatten();
        if let (Some(prefix), Some(datagram)) = (prefix.clone(), claimed.udp_packet_path()) {
            if let Some((flow_binding, request)) = super::packet_path::build_udp_packet_path_pair(
                session.id,
                std::sync::Arc::new(prefix),
                datagram,
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

        let final_hop = claimed.into_claimed();
        let operation = final_hop.into_udp_relay()?;

        if operation.requires_datagram_carrier() {
            return super::datagram_relay::prepare(prefix, operation);
        }

        if operation.needs_two_streams() {
            return Ok(PreparedUdpRelayChain::TwoStream {
                post_prefix: prepared.clone(),
                get_prefix: prepared,
                operation,
            });
        }

        Ok(PreparedUdpRelayChain::FinalHop {
            prefix: prepared,
            operation,
        })
    }
}

impl ProtocolInventory {
    /// TCP and UDP share one prefix selector, including the actual legacy pool
    /// prefix retention rule. Only the final compatibility executor is claimed.
    pub(super) fn prepare_udp_relay_chain<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        session: &'a zero_core::Session,
        chain: &[ResolvedLeafOutbound<'a>],
        payload: &'a [u8],
    ) -> Result<crate::runtime::udp_dispatch::relay::PreparedUdpRelayChain<'a>, FlowFailure> {
        for leaf in chain {
            let (tag, upstream) = match leaf {
                ResolvedLeafOutbound::Proxy { identity } => {
                    let input = ctx
                        .config()
                        .outbounds
                        .get(identity.config_index())
                        .ok_or_else(|| FlowFailure {
                            stage: "relay_prepare",
                            error: std::io::Error::other("relay outbound retired").into(),
                            upstream: None,
                        })?;
                    (
                        Some(input.tag()),
                        input
                            .protocol
                            .endpoint()
                            .map(|(host, port)| (host.to_owned(), port)),
                    )
                }
                ResolvedLeafOutbound::Direct { tag, .. } | ResolvedLeafOutbound::Block { tag } => {
                    (*tag, None)
                }
            };
            if !ctx.udp_enabled_for_outbound(tag) {
                return Err(FlowFailure {
                    stage: "udp_policy",
                    error: std::io::Error::other("udp disabled for relay chain outbound").into(),
                    upstream,
                });
            }
        }
        let execution = ctx.packet_path_execution_services().tcp();
        // Match the TCP preparation boundary: installed roles require the
        // actual caller identity. A compatibility/library context must not be
        // silently promoted to a request-bound native prefix.
        let components = if execution.invocation.is_some() {
            Some(execution.components.as_deref().map_err(|source| {
                FlowFailure {
                    stage: "prepare_component",
                    error: std::io::Error::other(crate::component::instance::retained(
                        source.clone(),
                    ))
                    .into(),
                    upstream: None,
                }
            })?)
        } else {
            None
        };
        let last = chain.last().expect("validated relay has final hop");
        let claimed = self
            .claim_outbound_leaf(ctx.config(), last.clone())
            .map_err(|error| FlowFailure {
                stage: "relay_prepare",
                error,
                upstream: None,
            })?;
        let prepared = self
            .prepare_tcp_relay_chain(
                OutboundAdapterContext::new(ctx.config()),
                chain,
                components,
                Some(&claimed),
            )
            .map_err(|failure| FlowFailure {
                stage: failure.stage,
                error: failure.error,
                upstream: failure.upstream_endpoint.map(|endpoint| *endpoint),
            })?;
        self.prepare_udp_relay_continuation(session, prepared, claimed, payload)
    }
}
