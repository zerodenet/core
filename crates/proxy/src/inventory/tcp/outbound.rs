use zero_engine::ResolvedOutbound;

use super::super::ProtocolInventory;
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

pub(crate) enum PreparedTcpOutbound<'a> {
    Relay(super::PreparedTcpRelayChain<'a>),
    Single(super::PreparedTcpCandidate<'a>),
    Fallback(Vec<super::PreparedTcpCandidate<'a>>),
}

impl ProtocolInventory {
    pub(crate) fn prepare_tcp_outbound<'a>(
        &self,
        ctx: OutboundAdapterContext<'a>,
        resolved: &'a ResolvedOutbound<'a>,
    ) -> Result<PreparedTcpOutbound<'a>, TcpOutboundFailure> {
        match resolved {
            ResolvedOutbound::Relay { chain } => {
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
                let claimed = self
                    .claim_outbound_leaf(ctx.config(), candidate.clone())
                    .map_err(|error| TcpOutboundFailure {
                        stage: "outbound_leaf_runtime",
                        error,
                        upstream_endpoint: None,
                        network: None,
                    })?;
                Ok(PreparedTcpOutbound::Single(
                    self.prepare_claimed_tcp_candidate(ctx, &claimed)?,
                ))
            }
            ResolvedOutbound::Fallback { candidates } => {
                let mut prepared = Vec::with_capacity(candidates.len());
                let mut last_failure = None;

                for candidate in candidates.iter().cloned() {
                    let prepared_candidate = self
                        .claim_outbound_leaf(ctx.config(), candidate)
                        .map_err(|error| TcpOutboundFailure {
                            stage: "outbound_leaf_runtime",
                            error,
                            upstream_endpoint: None,
                            network: None,
                        })
                        .and_then(|claimed| self.prepare_claimed_tcp_candidate(ctx, &claimed));
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
