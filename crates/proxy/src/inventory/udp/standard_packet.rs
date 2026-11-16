//! Existing UDP codec relays select one concrete standard-role graph before
//! any executable claim or compatibility TCP-prefix preparation can occur.
use super::super::ProtocolInventory;
use crate::{
    component::stream_bridge::datagram::packet_path::RolePacketPath,
    protocol_registry::{ColdPacketPathProjection, OutboundAdapterContext, UdpAdapterContext},
    runtime::{
        udp_dispatch::{relay::PreparedUdpRelayChain, FlowFailure},
        udp_flow::{
            packet_path::{
                PacketPathCarrierDescriptor, PacketPathFlowBinding, UdpDatagramSource, UdpPacketRef,
            },
            packet_path_chain::{PacketPathCarrierRequest, PacketPathStartRequest},
        },
    },
};
use std::sync::Arc;
use zero_core::{component::NetworkGeneration, Session};
use zero_engine::{ResolvedLeafOutbound, RouteMode};
use zero_platform_tokio::contract::component::RoleKind;

impl ProtocolInventory {
    pub(super) fn prepare_standard_packet_relay<'a>(
        &self,
        ctx: &UdpAdapterContext<'_>,
        session: &'a Session,
        chain: &[ResolvedLeafOutbound<'_>],
        payload: &'a [u8],
        datagram: &UdpDatagramSource,
        sources: &[Option<ColdPacketPathProjection>],
    ) -> Option<Result<PreparedUdpRelayChain<'a>, FlowFailure>> {
        let (_, prefix) = chain.split_last()?;
        if prefix.is_empty() {
            return None;
        }
        let execution = ctx.packet_path_execution_services();
        let failure = |error| FlowFailure {
            stage: "udp_standard_packet_relay",
            error,
            upstream: None,
        };
        // The caller prepared the exact final codec family once. Selecting
        // a native graph never reconstructs it or creates an executable claim.
        let datagram = datagram.clone();
        let tcp = execution.tcp();
        let plan = match tcp.components.as_ref() {
            Ok(plan) => plan,
            Err(source) => {
                return Some(Err(failure(
                    std::io::Error::other(crate::component::instance::retained(source.clone()))
                        .into(),
                )))
            }
        };
        for leaf in chain {
            let ResolvedLeafOutbound::Proxy { identity } = leaf else {
                // Existing special targets keep their own dispatch, never an
                // invented configured identity or a second direct provider.
                return None;
            };
            let Some(input) = ctx.config().outbounds.get(identity.config_index()) else {
                return Some(Err(failure(
                    std::io::Error::other("packet relay member retired").into(),
                )));
            };
            if !ctx.udp_enabled_for_outbound(Some(input.tag())) {
                return Some(Err(FlowFailure {
                    stage: "udp_policy",
                    error: std::io::Error::other("udp disabled for relay chain outbound").into(),
                    upstream: input
                        .protocol
                        .endpoint()
                        .map(|(server, port)| (server.into(), port)),
                }));
            }
            if let Some((reason, endpoint)) =
                zero_engine::EndpointAdmission::from_config(ctx.config())
                    .outbound_denial(input.tag())
            {
                return Some(Err(failure(zero_engine::EngineError::InvalidPlan {
                    message: format!("{reason}: {endpoint}"),
                })));
            }
        }
        let root = sources
            .iter()
            .rposition(|source| {
                source
                    .as_ref()
                    .is_none_or(|source| source.formatter.is_none())
            })
            .unwrap_or(0);
        let projection = sources[root].as_ref()?;
        let slot = plan.configured(projection.descriptor.tag.as_deref()?)?;
        if !slot.recipe.spec().roles.contains(&RoleKind::Datagram) {
            return None;
        }
        // Only a completely migrated prefix is admitted. An unqualified donor
        // retains its existing whole compatibility path, without partial loans.
        let donors = if root == 0 {
            None
        } else {
            if !projection.associated {
                return None;
            }
            match Self::prepare_native_tcp_relay_chain(prefix[..root].iter().map(|leaf| {
                self.native_tcp_candidate(
                    OutboundAdapterContext::new(ctx.config()),
                    leaf,
                    Some(plan),
                    RouteMode::Flow,
                )
            })) {
                None => return None,
                Some(Err(error)) => return Some(Err(failure(error.error))),
                Some(Ok(donors)) => Some(donors),
            }
        };
        let mut recipe = match RolePacketPath::prepare(
            &slot,
            projection,
            donors,
            plan,
            NetworkGeneration(tcp.upstream().network_generation()),
        ) {
            Ok(recipe) => recipe,
            Err(error) => return Some(Err(failure(error))),
        };
        for source in &sources[root + 1..] {
            recipe.append_formatter(
                source
                    .as_ref()
                    .and_then(|source| source.formatter.clone())?,
            );
        }
        let flow_binding = PacketPathFlowBinding::new(datagram.clone(), &recipe.descriptor());
        Some(Ok(PreparedUdpRelayChain {
            flow_binding,
            request: Box::new(PacketPathStartRequest {
                session_id: session.id,
                carrier: PacketPathCarrierRequest(Arc::new(recipe)),
                datagram,
                packet: UdpPacketRef {
                    target: &session.target,
                    port: session.port,
                    payload,
                },
            }),
        }))
    }
}

impl ProtocolInventory {
    /// One cold family per actual member, shared by standard selection and the
    /// existing compatibility root. No executable codec/TCP claim is prepared.
    pub(super) fn prepare_packet_relay_sources(
        &self,
        ctx: &UdpAdapterContext<'_>,
        prefix: &[ResolvedLeafOutbound<'_>],
    ) -> Result<Vec<Option<ColdPacketPathProjection>>, FlowFailure> {
        let execution = ctx.packet_path_execution_services().tcp();
        prefix
            .iter()
            .map(|leaf| {
                let mut projection = self
                    .registry
                    .prepare_cold_packet_path_projection(ctx.config(), leaf.clone())
                    .map_err(|error| FlowFailure {
                        stage: "relay_prepare",
                        error,
                        upstream: None,
                    })?;
                // Actual factory facts cover standard protocols with no old packet
                // projection. Pure codec Arcs still come from the adapter above.
                if projection
                    .as_ref()
                    .is_none_or(|value| value.formatter.is_none())
                {
                    if let ResolvedLeafOutbound::Proxy { identity } = leaf {
                        if let Some((input, slot)) = ctx
                            .config()
                            .outbounds
                            .get(identity.config_index())
                            .and_then(|input| {
                                Some((
                                    input,
                                    execution
                                        .components
                                        .as_ref()
                                        .ok()?
                                        .configured(input.tag())?,
                                ))
                            })
                        {
                            if let Some(facts) = slot.recipe.packet_path_facts() {
                                projection = Some(ColdPacketPathProjection {
                                    descriptor: PacketPathCarrierDescriptor {
                                        tag: Some(input.tag().into()),
                                        server: match &facts.endpoint.address {
                                            zero_core::Address::Domain(name) => name.clone(),
                                            zero_core::Address::Ipv4(bytes) => {
                                                std::net::Ipv4Addr::from(*bytes).to_string()
                                            }
                                            zero_core::Address::Ipv6(bytes) => {
                                                std::net::Ipv6Addr::from(*bytes).to_string()
                                            }
                                        },
                                        port: facts.endpoint.port,
                                        cache_key: format!("standard-packet:{:?}", slot.role_id()),
                                    },
                                    associated: facts.associated,
                                    formatter: None,
                                });
                            }
                        }
                    }
                }
                Ok(projection)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;

pub(super) fn no_carrier() -> crate::runtime::udp_dispatch::FlowFailure {
    crate::runtime::udp_dispatch::FlowFailure {
        stage: "no_udp_relay_final_hop",
        error: std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this adapter does not support UDP relay final hop",
        )
        .into(),
        upstream: None,
    }
}
