use zero_engine::{ResolvedOutbound, RouteMode};

use super::super::ProtocolInventory;
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

pub(crate) enum PreparedTcpOutbound {
    NativeRelay(super::PreparedNativeTcpRelayChain),
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
                Err(crate::runtime::path::missing_tcp_route_capability(mode))
            }
            ResolvedOutbound::Single(candidate) => self
                .prepare_tcp_candidate(ctx, candidate, components, mode)
                .map(PreparedTcpOutbound::Single),
            ResolvedOutbound::Fallback { candidates } => {
                let mut prepared = Vec::with_capacity(candidates.len());
                let mut last_failure = None;

                for candidate in candidates {
                    let prepared_candidate =
                        self.prepare_tcp_candidate(ctx, candidate, components, mode);
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
