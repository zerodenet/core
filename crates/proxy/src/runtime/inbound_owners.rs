//! Persistent actual listener drivers and carrier resources, outside run futures.
mod bind;
mod entry;
pub(crate) use bind::{LeasedInbound, NativeListenerView};
#[cfg(feature = "managed-datagram-runtime")]
mod peer_binding;
#[cfg(feature = "managed-datagram-runtime")]
mod peers;
mod preparing;
mod recovery;
mod state_recovery;
pub(crate) use preparing::{preparation_stopped, PreparingInbounds};
mod released_fault;
use crate::inventory::ProtocolInventory;
pub(crate) use entry::{
    consume_reported_error, ClosedInboundReceipt, InboundOwner, InboundReady, InboundTaskAccess,
    InboundTerminal,
};
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use peer_binding::{HostPeerBinding, JoinedPeer};
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use peers::{
    children::{PeerBoundDatagramAuthority, PeerBoundDatagramRegistration, PeerResourceContext},
    InboundPeerTasks, MAX_DATAGRAM_PEERS,
};
pub(crate) use released_fault::released_fault_identity;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tokio::{sync::watch, time::Instant};
use zero_core::component::ComponentInstanceId;
use zero_engine::EngineError;
use zero_platform_tokio::contract::{lifecycle::DynResourceLifecycle, CancellationToken};

#[derive(Default)]
pub(crate) struct InboundOwnerRegistry {
    entries: Mutex<HashMap<String, Arc<InboundOwner>>>,
    terminals: Mutex<HashMap<String, Arc<InboundTerminal>>>,
    reservations: Mutex<bind::BindReservations>,
    retained_states: Mutex<std::collections::BTreeMap<String, state_recovery::RetainedState>>,
}
impl std::fmt::Debug for InboundOwnerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundOwnerRegistry")
            .field(
                "owners",
                &self
                    .entries
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .finish()
    }
}
#[derive(Clone)]
pub(crate) struct InboundInstallContext {
    entry: Arc<InboundOwner>,
    protocols: ProtocolInventory,
}
impl InboundInstallContext {
    #[cfg(feature = "managed-datagram-runtime")]
    pub(crate) fn peer_tasks(&self) -> Arc<InboundPeerTasks> {
        self.entry.install_peer_tasks(self.protocols.clone())
    }
    #[cfg(feature = "raw-ip-runtime")]
    pub(crate) fn child_tasks(
        &self,
        limit: NonZeroUsize,
    ) -> Result<Arc<InboundPeerTasks>, EngineError> {
        self.entry
            .install_child_group(self.protocols.clone(), limit)
    }
    pub(crate) fn register(
        &self,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<ComponentInstanceId, EngineError> {
        let identity = self.protocols.allocate_resource_identity()?;
        self.entry.install_resource(identity, owner)?;
        Ok(identity)
    }
    pub(crate) fn tasks(&self) -> InboundTaskAccess {
        self.entry.task_access()
    }
}
impl InboundOwnerRegistry {
    pub(crate) fn identities(&self) -> std::collections::BTreeSet<ComponentInstanceId> {
        let active = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        active
            .values()
            .chain(reserved.entries.values())
            .map(|owner| owner.identity())
            .collect()
    }
    pub(crate) fn install(
        &self,
        tag: &str,
        protocols: &ProtocolInventory,
        nested_tasks: usize,
        shutdown: watch::Sender<bool>,
    ) -> Result<InboundInstallContext, EngineError> {
        self.install_inner(tag, protocols, nested_tasks, shutdown, false)
    }
    pub(crate) fn install_listener(
        &self,
        tag: &str,
        protocols: &ProtocolInventory,
        nested_tasks: usize,
        shutdown: watch::Sender<bool>,
    ) -> Result<InboundInstallContext, EngineError> {
        self.install_inner(tag, protocols, nested_tasks, shutdown, true)
    }
    fn install_inner(
        &self,
        tag: &str,
        protocols: &ProtocolInventory,
        nested_tasks: usize,
        shutdown: watch::Sender<bool>,
        listener: bool,
    ) -> Result<InboundInstallContext, EngineError> {
        let limit = nested_tasks
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| std::io::Error::other("inbound declared task limit exhausted"))?;
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains_key(tag) {
            return Err(std::io::Error::other(format!(
                "inbound `{tag}` still has an unconfirmed owner"
            ))
            .into());
        }
        let entry = Arc::new(InboundOwner::new(
            protocols.allocate_resource_identity()?,
            limit,
            shutdown,
        ));
        if listener {
            entry.begin_listener_installation();
        }
        entries.insert(tag.to_owned(), entry.clone());
        Ok(InboundInstallContext {
            entry,
            protocols: protocols.clone(),
        })
    }
    pub(crate) fn owner(&self, tag: &str) -> Option<Arc<InboundOwner>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(tag)
            .cloned()
    }
    #[cfg(test)]
    pub(crate) async fn shutdown(&self, tag: &str, deadline: Instant) -> Result<(), EngineError> {
        let Some(receipt) = self.confirm_shutdown(tag, deadline).await? else {
            return Ok(());
        };
        if let Some(failure) = receipt.terminal().failure() {
            return Err(
                std::io::Error::other(crate::component::instance::retained(failure)).into(),
            );
        }
        self.release_closed(receipt, |_| Ok(()))
    }
}
impl InboundInstallContext {
    pub(crate) fn owner(&self) -> Arc<InboundOwner> {
        self.entry.clone()
    }
}
