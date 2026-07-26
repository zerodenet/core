#[cfg(feature = "udp-runtime")]
use std::iter;
use std::path::Path;

use zero_config::RuntimeConfig;
use zero_engine::{EngineError, ResolvedLeafOutbound};

use super::ProtocolInventory;
use crate::protocol_registry::{ClaimedOutboundLeaf, OutboundLeafRuntime};
use crate::runtime::tcp_dispatch::operation::{
    PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
#[cfg(feature = "udp-runtime")]
use crate::runtime::udp_dispatch::operation::PreparedUdpFlowOperation;

#[derive(Clone)]
pub(crate) struct ClaimedInventoryLeaf<'a> {
    claimed: ClaimedOutboundLeaf<'a>,
}

impl<'a> ClaimedInventoryLeaf<'a> {
    fn new(claimed: ClaimedOutboundLeaf<'a>) -> Self {
        Self { claimed }
    }

    pub(crate) fn runtime(&self) -> OutboundLeafRuntime {
        self.claimed.runtime.clone()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_claimed(self) -> ClaimedOutboundLeaf<'a> {
        self.claimed
    }

    pub(crate) fn prepare_tcp_connect(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedTcpConnectOperation + 'a>, crate::transport::TcpOutboundFailure>
    {
        self.claimed.prepare_tcp_connect(source_dir)
    }

    pub(crate) fn prepare_tcp_relay_hop(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<(String, u16, Box<dyn PreparedTcpRelayOperation + 'a>), EngineError> {
        self.claimed.prepare_tcp_relay_hop(source_dir)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_flow(
        &self,
        source_dir: Option<&Path>,
    ) -> Result<Box<dyn PreparedUdpFlowOperation + 'a>, crate::runtime::udp_dispatch::FlowFailure>
    {
        self.claimed.prepare_udp_flow(source_dir)
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn prepare_udp_packet_path(
        &self,
    ) -> Option<
        Box<
            dyn crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation
                + 'a,
        >,
    > {
        self.claimed.prepare_udp_packet_path()
    }
}

#[derive(Clone)]
pub(crate) struct ClaimedRelayChain<'a> {
    first: ClaimedInventoryLeaf<'a>,
    relay_hops: Vec<ClaimedInventoryLeaf<'a>>,
}

impl<'a> ClaimedRelayChain<'a> {
    pub(crate) fn new(
        first: ClaimedInventoryLeaf<'a>,
        relay_hops: Vec<ClaimedInventoryLeaf<'a>>,
    ) -> Self {
        Self { first, relay_hops }
    }

    pub(crate) fn first(&self) -> &ClaimedInventoryLeaf<'a> {
        &self.first
    }

    pub(crate) fn relay_hops(&self) -> &[ClaimedInventoryLeaf<'a>] {
        &self.relay_hops
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn len(&self) -> usize {
        1 + self.relay_hops.len()
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn final_hop(&self) -> &ClaimedInventoryLeaf<'a> {
        self.relay_hops
            .last()
            .expect("relay chain must have at least 2 hops")
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn leaves(&self) -> impl Iterator<Item = &ClaimedInventoryLeaf<'a>> {
        iter::once(&self.first).chain(self.relay_hops.iter())
    }
}

impl ProtocolInventory {
    pub(crate) fn on_config_reloaded(&self, config: &RuntimeConfig) {
        self.registry.on_config_reloaded(config);
    }

    pub(crate) fn claim_outbound_leaf<'a>(
        &self,
        config: &'a RuntimeConfig,
        leaf: ResolvedLeafOutbound<'a>,
    ) -> Result<ClaimedInventoryLeaf<'a>, EngineError> {
        let claimed = self.registry.claim_outbound_leaf(config, leaf)?;
        Ok(ClaimedInventoryLeaf::new(claimed))
    }

    pub(in crate::inventory) fn claim_relay_chain<'a, E, F, G>(
        &self,
        config: &'a RuntimeConfig,
        chain: impl IntoIterator<Item = ResolvedLeafOutbound<'a>>,
        map_first_error: F,
        map_relay_error: G,
    ) -> Result<ClaimedRelayChain<'a>, E>
    where
        F: FnOnce(EngineError) -> E,
        G: Fn(EngineError) -> E,
    {
        let mut chain = chain.into_iter();
        let first = chain.next().expect("relay chain must have at least 2 hops");
        let second = chain.next().expect("relay chain must have at least 2 hops");

        let first = self
            .claim_outbound_leaf(config, first)
            .map_err(map_first_error)?;
        let map_relay_error = &map_relay_error;
        let relay_hops = std::iter::once(second)
            .chain(chain)
            .map(|leaf| {
                self.claim_outbound_leaf(config, leaf)
                    .map_err(map_relay_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ClaimedRelayChain::new(first, relay_hops))
    }
}
