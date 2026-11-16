use zero_core::Session;
use zero_engine::{EngineError, ResolvedOutbound, RouteMode};

use super::super::ProtocolInventory;
use crate::protocol_registry::UdpAdapterContext;
use crate::runtime::udp_dispatch::FlowFailure;

pub(crate) enum PreparedUdpOutbound<'a> {
    #[cfg(feature = "managed-stream-runtime")]
    DatagramRelay(Box<super::PreparedDatagramRelay>),
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
            ResolvedOutbound::Single(candidate) => Ok(PreparedUdpOutbound::Single(
                self.prepare_udp_candidate(ctx, candidate, mode)?,
            )),
            ResolvedOutbound::Fallback { candidates } => {
                let mut prepared = Vec::with_capacity(candidates.len());
                let mut last_failure = None;

                for candidate in candidates {
                    let prepared_candidate =
                        self.prepare_udp_candidate(ctx.clone(), candidate, mode);
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
                let datagram = match chain.last() {
                    Some(last) => self
                        .registry
                        .prepare_cold_packet_path_projection(ctx.config(), last.clone())
                        .map_err(|error| FlowFailure {
                            stage: "relay_prepare",
                            error,
                            upstream: None,
                        })?
                        .and_then(|projection| projection.formatter),
                    None => None,
                };
                if let Some(datagram) = datagram {
                    let sources =
                        self.prepare_packet_relay_sources(&ctx, &chain[..chain.len() - 1])?;
                    if let Some(selected) = self.prepare_standard_packet_relay(
                        &ctx, session, chain, payload, &datagram, &sources,
                    ) {
                        return selected
                            .map(|selected| PreparedUdpOutbound::Relay(Box::new(selected)));
                    }
                    return Err(super::standard_packet::no_carrier());
                }
                #[cfg(feature = "managed-stream-runtime")]
                if let Some(selected) = self.prepare_standard_datagram_relay(&ctx, chain) {
                    return selected
                        .map(|selected| PreparedUdpOutbound::DatagramRelay(Box::new(selected)));
                }
                Err(super::standard_packet::no_carrier())
            }
        }
    }
}
