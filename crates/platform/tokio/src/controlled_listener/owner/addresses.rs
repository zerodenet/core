//! Freeze the host's actual resolution before the first native acquisition.
use super::*;

impl ControlledTcpListenerOwner {
    pub(in crate::controlled_listener) fn resolving(
        declaration: &ResourceDeclaration,
        attempts: NonZeroUsize,
        parent: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::TcpListener { destination, .. } = declaration else {
            return Err(denied(
                Reason::InvalidConfig,
                "not a TCP listener declaration",
            ));
        };
        if !matches!(&destination.address, Address::Domain(host) if !host.is_empty()) {
            return Err(denied(
                Reason::InvalidConfig,
                "resolving listener requires a hostname",
            ));
        }
        Self::construct(
            declaration,
            attempts,
            parent,
            Arc::from([]),
            Some(destination.port),
        )
    }

    /// Host-selected addresses only; this grants no DNS execution or arbitrary
    /// endpoint changes. One frozen result belongs to this one original owner.
    pub fn install_resolved_addresses(
        &mut self,
        addresses: Arc<[SocketAddr]>,
    ) -> Result<(), ChannelError> {
        let Some(port) = self.resolution_port else {
            return Err(denied(
                Reason::InvalidConfig,
                "literal listeners cannot be retargeted",
            ));
        };
        if self.acquired
            || !self.addresses.is_empty()
            || addresses.is_empty()
            || addresses.len() > self.remaining
            || addresses.iter().any(|address| address.port() != port)
        {
            return Err(denied(
                Reason::InvalidConfig,
                "invalid or repeated listener resolution",
            ));
        }
        self.addresses = addresses;
        Ok(())
    }
}
