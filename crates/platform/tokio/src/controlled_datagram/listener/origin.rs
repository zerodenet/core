//! Sealed observations of this listener's actual host registration and wire.
//! These values retain no socket, Scope, task supervisor or close capability.
use super::*;
use crate::contract::scope::{TaskJoinWitness, TaskRegistrar, TaskScopeIdentity};
use std::sync::OnceLock;
use zero_core::component::ComponentInstanceId;

struct Provider {
    identity: DatagramListenerIdentity,
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    driver: OnceLock<TaskJoinWitness>,
    retired: tokio_util::sync::CancellationToken,
    closed: Arc<OnceLock<DatagramCloseReceipt>>,
}
/// Constructed only by the unique retained original listener controller.
#[derive(Clone)]
pub struct ListenerProviderRegistration(Arc<Provider>);
/// This provider's actual, already registered wire driver. No constructor can
/// turn a component number, foreign witness or Packet device name into one.
#[derive(Clone)]
pub struct ListenerCarrierOrigin {
    owner: ListenerProviderRegistration,
    driver: TaskJoinWitness,
}
/// A matching original OS-close receipt. Task joins and logical users are
/// deliberately separate observations; neither is implied by this value.
#[derive(Clone)]
pub struct ListenerProviderClosed(Arc<OnceLock<DatagramCloseReceipt>>);

impl ControlledDatagramListenerControl {
    /// Called by the native host before either acquired view escapes. A source
    /// factory receives RX/TX only and cannot register itself as a provider.
    pub fn register_provider(
        &self,
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
    ) -> std_io::Result<ListenerProviderRegistration> {
        self.shared.check(*self.shared.lock())?;
        let scope = tasks.scope_identity();
        let provider = self.shared.provider.get_or_init(|| {
            ListenerProviderRegistration(Arc::new(Provider {
                identity: DatagramListenerIdentity(Arc::downgrade(&self.shared)),
                instance,
                scope: scope.clone(),
                driver: OnceLock::new(),
                retired: self.shared.socket.entry.retired.clone(),
                closed: self.shared.socket.entry.closed_receipt.clone(),
            }))
        });
        if provider.instance() != instance || !provider.scope().same_scope(&scope) {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "listener was registered by another actual host instance or Scope",
            ));
        }
        Ok(provider.clone())
    }
    /// Only the retained native controller may attest that this exact listener
    /// is driven by this exact registration. There is one wire per original.
    pub fn register_driver(
        &self,
        listener: &DatagramListenerIdentity,
        witness: &TaskJoinWitness,
    ) -> std_io::Result<ListenerCarrierOrigin> {
        let owner = self
            .shared
            .provider
            .get()
            .ok_or_else(|| std_io::Error::other("listener has no actual host registration"))?;
        if !owner.matches_listener(listener)
            || !owner.scope().same_scope(&witness.scope_identity())
            || !owner.scope().resource_open()
            || witness.is_joined()
        {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "wire witness does not belong to this original listener Scope",
            ));
        }
        self.shared.check(*self.shared.lock())?;
        let actual = owner.0.driver.get_or_init(|| witness.clone());
        if !actual.same_registration(witness) {
            return Err(std_io::Error::new(
                std_io::ErrorKind::AlreadyExists,
                "original listener already has another actual wire driver",
            ));
        }
        Ok(ListenerCarrierOrigin {
            owner: owner.clone(),
            driver: actual.clone(),
        })
    }
}
impl DatagramListenerIdentity {
    /// Read the host's actual registration; metadata alone cannot create one.
    pub fn provider(&self) -> Option<ListenerProviderRegistration> {
        self.0.upgrade()?.provider.get().cloned()
    }
}
impl ListenerProviderRegistration {
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.instance
    }
    pub fn scope(&self) -> &TaskScopeIdentity {
        &self.0.scope
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn matches_listener(&self, listener: &DatagramListenerIdentity) -> bool {
        self.0.identity.same_listener(listener)
    }
    pub fn limit(&self) -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }
    pub fn closed(&self) -> Option<ListenerProviderClosed> {
        self.0.closed.get()?;
        Some(ListenerProviderClosed(self.0.closed.clone()))
    }
    pub fn origin(&self) -> Option<ListenerCarrierOrigin> {
        if self.0.retired.is_cancelled() || !self.0.scope.resource_open() {
            return None;
        }
        let driver = self.0.driver.get()?;
        if driver.is_joined() {
            return None;
        }
        Some(ListenerCarrierOrigin {
            owner: self.clone(),
            driver: driver.clone(),
        })
    }
}
impl ListenerCarrierOrigin {
    pub fn owner(&self) -> &ListenerProviderRegistration {
        &self.owner
    }
    pub fn driver(&self) -> &TaskJoinWitness {
        &self.driver
    }
    pub fn same_original(&self, other: &Self) -> bool {
        self.owner.same_owner(&other.owner) && self.driver.same_registration(&other.driver)
    }
    pub fn closed(&self) -> Option<ListenerProviderClosed> {
        self.owner.closed()
    }
    pub fn is_retired(&self) -> bool {
        self.owner.0.retired.is_cancelled()
            || !self.owner.scope().resource_open()
            || self.driver.is_joined()
    }
    pub async fn retired(&self) {
        if self.is_retired() {
            return;
        }
        tokio::select! {
            _ = self.owner.0.retired.cancelled() => {},
            _ = self.driver.wait() => {},
        }
    }
}
impl ListenerProviderClosed {
    pub fn matches(&self, owner: &ListenerProviderRegistration) -> bool {
        Arc::ptr_eq(&self.0, &owner.0.closed) && self.0.get().is_some()
    }
    pub fn receipt(&self) -> DatagramCloseReceipt {
        *self
            .0
            .get()
            .expect("only a real native close constructs this observation")
    }
}

#[cfg(test)]
#[path = "../../../tests/controlled_datagram/listener_origin.rs"]
mod tests;
