//! One sealed host registration on the existing actual native UDP Entry.
//! Listener and bound views share it; no carrier pool or second close owner.
use super::*;
use crate::contract::scope::{TaskJoinWitness, TaskScopeIdentity};
use std::sync::OnceLock;
use zero_core::component::ComponentInstanceId;
struct Provider {
    identity: ControlledDatagramIdentity,
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    driver: OnceLock<TaskJoinWitness>,
    retired: tokio_util::sync::CancellationToken,
    closed: Arc<OnceLock<DatagramCloseReceipt>>,
}
#[derive(Clone)]
pub struct DatagramProviderRegistration(Arc<Provider>);
#[derive(Clone)]
pub struct NativeDatagramOrigin {
    owner: DatagramProviderRegistration,
    driver: TaskJoinWitness,
}
#[derive(Clone)]
pub struct DatagramProviderClosed(Arc<OnceLock<DatagramCloseReceipt>>);
impl Entry {
    pub(super) fn register_provider(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
    ) -> std_io::Result<DatagramProviderRegistration> {
        if self.retired.is_cancelled() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::NotConnected,
                "original UDP entry is retired",
            ));
        }
        let provider = self.provider.get_or_init(|| {
            DatagramProviderRegistration(Arc::new(Provider {
                identity: ControlledDatagramIdentity::from_entry(self),
                instance,
                scope: scope.clone(),
                driver: OnceLock::new(),
                retired: self.retired.clone(),
                closed: self.closed_receipt.clone(),
            }))
        });
        if provider.instance() != instance || !provider.scope().same_scope(&scope) {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "original UDP entry belongs to another actual host instance or Scope",
            ));
        }
        Ok(provider.clone())
    }
    pub(super) fn register_driver(
        self: &Arc<Self>,
        witness: &TaskJoinWitness,
    ) -> std_io::Result<NativeDatagramOrigin> {
        let owner = self
            .provider
            .get()
            .ok_or_else(|| std_io::Error::other("original UDP entry has no host registration"))?;
        if !owner.scope().same_scope(&witness.scope_identity())
            || !owner.scope().resource_open()
            || witness.is_joined()
        {
            return Err(std_io::Error::new(
                std_io::ErrorKind::InvalidInput,
                "wire witness differs from original UDP Scope",
            ));
        }
        if self.retired.is_cancelled() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::NotConnected,
                "original UDP entry is retired",
            ));
        }
        let actual = owner.0.driver.get_or_init(|| witness.clone());
        if !actual.same_registration(witness) {
            return Err(std_io::Error::new(
                std_io::ErrorKind::AlreadyExists,
                "original UDP entry already has another actual driver",
            ));
        }
        Ok(NativeDatagramOrigin {
            owner: owner.clone(),
            driver: actual.clone(),
        })
    }
}
impl DatagramProviderRegistration {
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.instance
    }
    pub fn scope(&self) -> &TaskScopeIdentity {
        &self.0.scope
    }
    pub fn identity(&self) -> &ControlledDatagramIdentity {
        &self.0.identity
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(super) fn matches_entry(&self, entry: &Arc<Entry>) -> bool {
        self.0
            .identity
            .same_original(&ControlledDatagramIdentity::from_entry(entry))
    }
    pub fn limit(&self) -> NonZeroUsize {
        NonZeroUsize::new(1).unwrap()
    }
    pub fn closed(&self) -> Option<DatagramProviderClosed> {
        self.0.closed.get()?;
        Some(DatagramProviderClosed(self.0.closed.clone()))
    }
    pub fn origin(&self) -> Option<NativeDatagramOrigin> {
        if self.0.retired.is_cancelled() || !self.0.scope.resource_open() {
            return None;
        }
        let driver = self.0.driver.get()?;
        if driver.is_joined() {
            return None;
        }
        Some(NativeDatagramOrigin {
            owner: self.clone(),
            driver: driver.clone(),
        })
    }
}
impl NativeDatagramOrigin {
    pub fn owner(&self) -> &DatagramProviderRegistration {
        &self.owner
    }
    pub fn driver(&self) -> &TaskJoinWitness {
        &self.driver
    }
    pub fn same_original(&self, other: &Self) -> bool {
        self.owner.same_owner(&other.owner) && self.driver.same_registration(&other.driver)
    }
    pub fn closed(&self) -> Option<DatagramProviderClosed> {
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
        tokio::select! { _ = self.owner.0.retired.cancelled() => {}, _ = self.driver.wait() => {} }
    }
}
impl DatagramProviderClosed {
    pub fn matches(&self, owner: &DatagramProviderRegistration) -> bool {
        Arc::ptr_eq(&self.0, &owner.0.closed) && self.0.get().is_some()
    }
    pub fn receipt(&self) -> DatagramCloseReceipt {
        *self
            .0
            .get()
            .expect("only actual native close issues this observation")
    }
}

impl ControlledDatagramSocket {
    pub fn original_provider(&self) -> Option<DatagramProviderRegistration> {
        self.entry.provider.get().cloned()
    }
}
