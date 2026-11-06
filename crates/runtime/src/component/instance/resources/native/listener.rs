//! Thin host lease over the very same component UDP native owner. The caller
//! installs this sole owner before binding and fences its actual host phase.
use super::*;
use std::num::NonZeroUsize;
use zero_platform_tokio::contract::lifecycle::CloseReport;

/// This is not ResourceAccess and grants no task, child graph or raw control.
/// A host must select one provider for a canonical declaration, never construct
/// both this owner and a second HostResources owner for that same declaration.
pub struct HostDatagramListenerOwner {
    component: ComponentId,
    originals: Option<(
        Arc<crate::HostOriginalBudgets>,
        zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    )>,
    address: SocketAddr,
    addresses: Arc<[SocketAddr]>,
    native: sockets::DatagramGrant,
    revoked: bool,
    first_fault: Option<Arc<ChannelError>>,
}
impl HostDatagramListenerOwner {
    pub fn new(
        component: ComponentId,
        declaration: &ResourceDeclaration,
        actual_attempts: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::DatagramListener {
            destination,
            max_bind_attempts,
            max_pending_sends,
            ..
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
            originals: None,
            address,
            addresses: Arc::from([address]),
            native: sockets::DatagramGrant::new(component, actual_attempts, *max_pending_sends)?,
            revoked: false,
            first_fault: None,
        })
    }
    /// Bind only the same actual host roots and registrar, before the first native attempt.
    pub fn bind_original_budgets(
        &mut self,
        originals: Arc<crate::HostOriginalBudgets>,
        tasks: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    ) -> Result<(), ChannelError> {
        if let Some((actual, registrar)) = &self.originals {
            if Arc::ptr_eq(actual, &originals)
                && registrar
                    .scope_identity()
                    .same_scope(&tasks.scope_identity())
            {
                return Ok(());
            }
            return Err(error(
                self.component,
                Reason::StalePlan,
                "actual listener cannot cross host original roots",
            ));
        }
        if self.revoked || !self.native.known_no_acquisition() {
            return Err(error(
                self.component,
                Reason::Revoked,
                "listener original roots must precede acquisition",
            ));
        }
        self.originals = Some((originals, tasks));
        Ok(())
    }
    /// Read the actual original acquisition state; no native I/O or close grant.
    pub fn known_no_acquisition(&self) -> bool {
        self.native.known_no_acquisition()
    }
    pub fn original_budgets(&self) -> Option<&Arc<crate::HostOriginalBudgets>> {
        self.originals.as_ref().map(|(roots, _)| roots)
    }
    pub fn original_tasks(
        &self,
    ) -> Option<&zero_platform_tokio::contract::scope::WeakTaskRegistrar> {
        self.originals.as_ref().map(|(_, tasks)| tasks)
    }
    /// The configuration host supplies its bounded, already joined system
    /// lookup. This does not resolve names or acquire a second native owner.
    pub fn new_resolved(
        component: ComponentId,
        declaration: &ResourceDeclaration,
        attempts: NonZeroUsize,
        addresses: Arc<[SocketAddr]>,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::DatagramListener {
            destination,
            max_bind_attempts,
            ..
        } = declaration
        else {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "not a datagram declaration",
            ));
        };
        if !matches!(destination.address, zero_core::Address::Domain(_))
            || addresses.is_empty()
            || addresses.len() > max_bind_attempts.get()
            || addresses.iter().any(|a| a.port() != destination.port)
        {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "invalid resolved listener addresses",
            ));
        }
        let mut literal = declaration.clone();
        if let ResourceDeclaration::DatagramListener { destination, .. } = &mut literal {
            destination.address = match addresses[0].ip() {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            };
        }
        let mut owner = Self::new(component, &literal, attempts)?;
        owner.addresses = addresses;
        Ok(owner)
    }
    /// No await, clone, rebind or alternate control. Wrong-address/retired
    /// requests are rejected before consuming any original native attempt.
    pub fn bind_listener(
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
        if !self.addresses.contains(&address) {
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "datagram listener address differs from declaration",
            ));
        }
        match self.native.bind(self.component, address) {
            Ok(view) => {
                self.address = address;
                Ok(view)
            }
            Err(failure) => Err(self.remember(failure)),
        }
    }
    /// Bind the same retained controller before its unique listener view escapes.
    pub fn register_provider(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    ) -> Result<
        zero_platform_tokio::controlled_datagram::provider::DatagramProviderRegistration,
        ChannelError,
    > {
        if self.component != instance.component {
            return Err(error(
                self.component,
                Reason::StalePlan,
                "listener host instance changed",
            ));
        }
        self.native
            .register_provider(instance, tasks)
            .map_err(|source| {
                error(
                    self.component,
                    Reason::StalePlan,
                    "actual listener provider registration failed",
                )
                .with_source(source)
            })
    }
    pub fn register_driver(
        &self,
        original: &zero_platform_tokio::controlled_datagram::listener::DatagramListenerIdentity,
        witness: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<
        zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin,
        ChannelError,
    > {
        self.native
            .register_driver(original, witness)
            .map_err(|source| {
                error(
                    self.component,
                    Reason::StalePlan,
                    "actual listener driver registration failed",
                )
                .with_source(source)
            })
    }
    pub fn revoke(&mut self) {
        self.revoked = true;
        self.native.revoke();
    }
    pub fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        match self.native.shutdown(self.component, self.address, deadline) {
            Ok(report) => Ok(report),
            Err(failure) => Err(self.remember(failure)),
        }
    }
    /// Historical native fault and actual physical completion are independent.
    #[cfg(test)]
    pub fn fault(&self) -> Option<Arc<ChannelError>> {
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
