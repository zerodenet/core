//! Actual Direct caller/provider selection, preserved over snapshot and source changes.
use super::*;
impl UdpIngressRuntime {
    /// Explicit actual caller grant. No default budget is inferred from peer count.
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn with_bound_direct_budget(
        mut self,
        budget: crate::runtime::udp_dispatch::BoundDirectBudget,
    ) -> Self {
        self.bound_direct_budget = Some(budget);
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn bound_direct_budget(
        &self,
    ) -> Option<crate::runtime::udp_dispatch::BoundDirectBudget> {
        self.bound_direct_budget
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn with_direct_peer_driver(
        mut self,
        slot: crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot,
    ) -> Self {
        self.direct_peer_driver = Some(slot);
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn direct_peer_driver(
        &self,
    ) -> Option<&crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot> {
        self.direct_peer_driver.as_ref()
    }
    /// The dispatcher original is already retained by this accepted/peer owner.
    /// The same actual caller polls native I/O; no task or fallback owner is created.
    #[cfg(feature = "managed-datagram-runtime")]
    pub(in crate::runtime) fn with_inline_direct_driver(
        mut self,
        slot: crate::runtime::udp_socket::endpoint::peer::DirectPeerDriverSlot,
        cancel: zero_platform_tokio::contract::CancellationToken,
    ) -> Self {
        self.inline_direct = true;
        self.direct_preferred_port = Some(self.source_addr.map(|address| address.port()));
        self.inline_direct_cancel = Some(cancel);
        self.bound_direct_budget = Some(
            crate::runtime::udp_dispatch::BoundDirectBudget::for_accepted(
                self.tcp_services.config(),
            ),
        );
        self.direct_peer_driver = Some(slot);
        self
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn direct_preferred_port(&self) -> Option<u16> {
        self.direct_preferred_port
            .unwrap_or_else(|| self.source_addr.map(|address| address.port()))
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn inline_direct(&self) -> bool {
        self.inline_direct
    }
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn inline_direct_cancellation(
        &self,
    ) -> Option<zero_platform_tokio::contract::CancellationToken> {
        self.inline_direct_cancel.clone()
    }
}
