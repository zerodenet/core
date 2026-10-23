use zero_engine::{ResolvedOutbound, RouteMode};

use super::super::ProtocolInventory;
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

pub(crate) enum PreparedTcpOutbound {
    NativeRelay(super::PreparedNativeTcpRelayChain),
    Relay(super::PreparedTcpRelayChain),
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
        // This migration registers Stream-only factories. Strict Packet mode
        // continues through the existing executable-plane capability check.
        let components = if mode == RouteMode::Packet {
            None
        } else {
            components
        };
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
                    chain
                        .iter()
                        .map(|leaf| self.native_tcp_candidate(ctx, leaf, components)),
                ) {
                    return prepared.map(PreparedTcpOutbound::NativeRelay);
                }
                let claimed = self.claim_relay_chain(
                    ctx.config(),
                    chain.iter().cloned(),
                    |error| TcpOutboundFailure {
                        stage: "outbound_leaf_runtime",
                        error,
                        upstream_endpoint: None,
                        network: None,
                    },
                    |error| TcpOutboundFailure {
                        stage: "relay_prepare",
                        error,
                        upstream_endpoint: None,
                        network: None,
                    },
                )?;
                Ok(PreparedTcpOutbound::Relay(
                    self.prepare_claimed_tcp_relay_chain(ctx, &claimed)?,
                ))
            }
            ResolvedOutbound::Single(candidate) => {
                if let Some(prepared) = self.native_tcp_candidate(ctx, candidate, components) {
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
                        .native_tcp_candidate(ctx, &candidate, components)
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
