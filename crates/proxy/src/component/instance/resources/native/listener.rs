//! Thin host lease over the very same component UDP native owner. The caller
//! installs this sole owner before binding and fences its actual host phase.
use super::*;
use std::num::NonZeroUsize;
use zero_platform_tokio::contract::lifecycle::CloseReport;

/// This is not ResourceAccess and grants no task, child graph or raw control.
/// A host must select one provider for a canonical declaration, never construct
/// both this owner and a second HostResources owner for that same declaration.
pub(crate) struct HostDatagramListenerOwner {
    component: ComponentId,
    address: SocketAddr,
    native: sockets::DatagramGrant,
    revoked: bool,
    first_fault: Option<Arc<ChannelError>>,
}
impl HostDatagramListenerOwner {
    pub(crate) fn new(
        component: ComponentId,
        declaration: &ResourceDeclaration,
        actual_attempts: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::DatagramListener {
            destination,
            max_bind_attempts,
            max_pending_sends,
        } = declaration
        else {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "not a datagram listener declaration",
            ));
        };
        if actual_attempts > *max_bind_attempts {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "datagram listener attempt budget exceeds declaration",
            ));
        }
        let address = sockets::literal(component, destination)?;
        Ok(Self {
            component,
            address,
            native: sockets::DatagramGrant::new(component, actual_attempts, *max_pending_sends)?,
            revoked: false,
            first_fault: None,
        })
    }
    /// No await, clone, rebind or alternate control. Wrong-address/retired
    /// requests are rejected before consuming any original native attempt.
    pub(crate) fn bind_listener(
        &mut self,
        address: SocketAddr,
    ) -> Result<GrantedDatagramListener, ChannelError> {
        if self.revoked {
            return Err(error(
                self.component,
                Reason::Revoked,
                "datagram listener owner was revoked",
            ));
        }
        if address != self.address {
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "datagram listener address differs from declaration",
            ));
        }
        match self.native.bind(self.component, address) {
            Ok(view) => Ok(view),
            Err(failure) => Err(self.remember(failure)),
        }
    }
    pub(crate) fn revoke(&mut self) {
        self.revoked = true;
        self.native.revoke();
    }
    pub(crate) fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        match self.native.shutdown(self.component, self.address, deadline) {
            Ok(report) => Ok(report),
            Err(failure) => Err(self.remember(failure)),
        }
    }
    /// Historical native fault and actual physical completion are independent.
    pub(crate) fn fault(&self) -> Option<Arc<ChannelError>> {
        self.first_fault.clone()
    }
    fn remember(&mut self, failure: ChannelError) -> ChannelError {
        if failure.reason != Reason::Io && failure.reason != Reason::Internal {
            return failure;
        }
        let source = Arc::new(failure);
        self.first_fault.get_or_insert_with(|| source.clone());
        crate::component::instance::retained(source)
    }
}

#[cfg(test)]
#[path = "../../../tests/datagram_listener_owner.rs"]
mod tests;
