use zero_engine::{ResolvedOutbound, RouteMode};

use super::super::ProtocolInventory;
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

pub(crate) enum PreparedTcpOutbound {
    NativeRelay(super::PreparedNativeTcpRelayChain),
    Relay {
        prefix: super::PreparedTcpRelayPrefix,
        final_hop: super::PreparedTcpRelayHop,
    },
    Single(super::PreparedTcpCandidate),
    Fallback(Vec<super::PreparedTcpCandidate>),
}

impl ProtocolInventory {
    pub(crate) fn prepare_tcp_outbound<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        resolved: &'a ResolvedOutbound<'a>,
        mode: RouteMode,
        components: Option<&crate::component::catalog::ComponentPlan>,
    ) -> Result<PreparedTcpOutbound, TcpOutboundFailure> {
        // Strict plane admission uses the same installed catalog. An absent
        // Packet role is reported without reconstructing an old protocol leaf.
        match resolved {
            ResolvedOutbound::Relay { chain } => {
                if mode == RouteMode::Packet {
                    return Err(crate::transport::TcpOutboundFailure {
                        stage: "data_plane_route",
                        error: zero_engine::EngineError::Io(std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "packet route mode has no executable TCP relay-chain path",
                        )),
                        upstream_endpoint: None,
                        network: None,
                    });
                }
                if let Some(prepared) = Self::prepare_native_tcp_relay_chain(
                    chain.iter().enumerate().map(|(index, leaf)| {
                        if index != 0
                            && matches!(leaf, zero_engine::ResolvedLeafOutbound::Direct { .. })
                        {
                            return None;
                        }
                        self.native_tcp_candidate(ctx, leaf, components, mode)
                    }),
                ) {
                    return prepared.map(PreparedTcpOutbound::NativeRelay);
                }
                let (prefix, final_hop) = self.prepare_tcp_relay(ctx, chain, components)?;
                Ok(PreparedTcpOutbound::Relay { prefix, final_hop })
            }
            ResolvedOutbound::Single(candidate) => {
                if let Some(prepared) = self.native_tcp_candidate(ctx, candidate, components, mode)
                {
                    return prepared.map(PreparedTcpOutbound::Single);
                }
                let claimed = self
                    .claim_outbound_leaf(ctx.config(), candidate.clone())
                    .map_err(|error| TcpOutboundFailure {
                        stage: "outbound_leaf_runtime",
                        error,
                        upstream_endpoint: None,
                        network: None,
                    })?;
                Ok(PreparedTcpOutbound::Single(
                    self.prepare_claimed_tcp_candidate(ctx, &claimed, mode)?,
                ))
            }
            ResolvedOutbound::Fallback { candidates } => {
                let mut prepared = Vec::with_capacity(candidates.len());
                let mut last_failure = None;

                for candidate in candidates.iter().cloned() {
                    let prepared_candidate = self
                        .native_tcp_candidate(ctx, &candidate, components, mode)
                        .unwrap_or_else(|| {
                            self.claim_outbound_leaf(ctx.config(), candidate)
                                .map_err(|error| TcpOutboundFailure {
                                    stage: "outbound_leaf_runtime",
                                    error,
                                    upstream_endpoint: None,
                                    network: None,
                                })
                                .and_then(|claimed| {
                                    self.prepare_claimed_tcp_candidate(ctx, &claimed, mode)
                                })
                        });
                    match prepared_candidate {
                        Ok(candidate) => prepared.push(candidate),
                        Err(failure) => last_failure = Some(failure),
                    }
                }

                if prepared.is_empty() {
                    Err(last_failure
                        .expect("validated fallback groups always have at least one candidate"))
                } else {
                    Ok(PreparedTcpOutbound::Fallback(prepared))
                }
            }
        }
    }
}
