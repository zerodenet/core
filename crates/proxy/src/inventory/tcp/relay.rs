use super::super::{ClaimedRelayChain, ProtocolInventory};
use super::{PreparedTcpCandidate, PreparedTcpRelayHop};
use crate::protocol_registry::OutboundAdapterContext;
use crate::transport::TcpOutboundFailure;

pub(crate) struct PreparedTcpRelayChain<'a> {
    pub(crate) first: PreparedTcpCandidate<'a>,
    pub(crate) relay_hops: Vec<PreparedTcpRelayHop<'a>>,
}

impl ProtocolInventory {
    pub(crate) fn prepare_claimed_tcp_relay_chain<'a>(
        &self,
        ctx: OutboundAdapterContext,
        claimed_chain: &ClaimedRelayChain<'a>,
    ) -> Result<PreparedTcpRelayChain<'a>, TcpOutboundFailure> {
        let first_prepared = self.prepare_claimed_tcp_candidate(ctx, claimed_chain.first())?;
        let mut prepared_hops = Vec::with_capacity(claimed_chain.relay_hops().len());
        for next_hop in claimed_chain.relay_hops() {
            let prepared = self
                .prepare_claimed_tcp_relay_hop(ctx, next_hop)
                .map_err(|error| TcpOutboundFailure {
                    stage: "relay_prepare",
                    error,
                    upstream_endpoint: None,
                    network: None,
                })?;
            prepared_hops.push(prepared);
        }

        Ok(PreparedTcpRelayChain {
            first: first_prepared,
            relay_hops: prepared_hops,
        })
    }
}
