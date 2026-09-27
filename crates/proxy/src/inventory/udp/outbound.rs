use zero_core::Session;
use zero_engine::{EngineError, ResolvedOutbound, RouteMode};

use super::super::ProtocolInventory;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::FlowFailure;

pub(crate) enum PreparedUdpOutbound<'a> {
    Relay(Box<crate::runtime::udp_dispatch::relay::PreparedUdpRelayChain<'a>>),
    Single(super::leaf::PreparedUdpLeafCandidate<'a>),
    Fallback(Vec<super::leaf::PreparedUdpLeafCandidate<'a>>),
}

impl ProtocolInventory {
    pub(crate) fn prepare_udp_outbound<'a>(
        &self,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        resolved: &'a ResolvedOutbound<'a>,
        payload: &'a [u8],
        mode: RouteMode,
    ) -> Result<PreparedUdpOutbound<'a>, FlowFailure> {
        match resolved {
            ResolvedOutbound::Single(candidate) => {
                let claimed = self
                    .claim_outbound_leaf(ctx.config(), candidate.clone())
                    .map_err(|error| FlowFailure {
                        stage: "outbound_leaf_runtime",
                        error,
                        upstream: None,
                    })?;
                Ok(PreparedUdpOutbound::Single(
                    self.prepare_claimed_udp_leaf_candidate(ctx, &claimed, mode)?,
                ))
            }
            ResolvedOutbound::Fallback { candidates } => {
                let mut prepared = Vec::with_capacity(candidates.len());
                let mut last_failure = None;

                for candidate in candidates.iter().cloned() {
                    let prepared_candidate = self
                        .claim_outbound_leaf(ctx.config(), candidate)
                        .map_err(|error| FlowFailure {
                            stage: "outbound_leaf_runtime",
                            error,
                            upstream: None,
                        })
                        .and_then(|claimed| {
                            self.prepare_claimed_udp_leaf_candidate(ctx.clone(), &claimed, mode)
                        });
                    match prepared_candidate {
                        Ok(candidate) => prepared.push(candidate),
                        Err(failure) => last_failure = Some(failure),
                    }
                }

                if prepared.is_empty() {
                    Err(last_failure.unwrap_or_else(|| FlowFailure {
                        stage: "fallback_exhausted",
                        error: EngineError::Io(std::io::Error::other(
                            "all fallback outbounds failed",
                        )),
                        upstream: None,
                    }))
                } else {
                    Ok(PreparedUdpOutbound::Fallback(prepared))
                }
            }
            ResolvedOutbound::Relay { chain } => {
                if mode == RouteMode::Packet {
                    return Err(FlowFailure {
                        stage: "data_plane_route",
                        error: EngineError::Io(std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "packet route mode has no executable UDP relay-chain path",
                        )),
                        upstream: None,
                    });
                }
                let claimed = self.claim_relay_chain(
                    ctx.config(),
                    chain.iter().cloned(),
                    |error| FlowFailure {
                        stage: "outbound_leaf_runtime",
                        error,
                        upstream: None,
                    },
                    |error| FlowFailure {
                        stage: "outbound_leaf_runtime",
                        error,
                        upstream: None,
                    },
                )?;
                Ok(PreparedUdpOutbound::Relay(Box::new(
                    self.prepare_claimed_udp_relay_chain(ctx, session, &claimed, payload)?,
                )))
            }
        }
    }
}
