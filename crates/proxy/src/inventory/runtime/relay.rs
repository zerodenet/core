use super::ClaimedInventoryLeaf;
#[cfg(feature = "udp-runtime")]
use std::iter;

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
    pub(crate) fn final_hop(&self) -> &ClaimedInventoryLeaf<'a> {
        self.relay_hops
            .last()
            .expect("relay chain must have at least 2 hops")
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn into_final_hop(mut self) -> ClaimedInventoryLeaf<'a> {
        self.relay_hops
            .pop()
            .expect("relay chain must have at least 2 hops")
    }

    #[cfg(feature = "udp-runtime")]
    pub(crate) fn leaves(&self) -> impl Iterator<Item = &ClaimedInventoryLeaf<'a>> {
        iter::once(&self.first).chain(self.relay_hops.iter())
    }
}
