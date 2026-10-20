//! Persistent actual listener drivers and carrier resources, outside run futures.
mod entry;
#[cfg(feature = "managed-datagram-runtime")]
mod peer_binding;
#[cfg(feature = "managed-datagram-runtime")]
mod peers;
use crate::inventory::ProtocolInventory;
pub(crate) use entry::{InboundOwner, InboundTaskAccess};
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use peer_binding::{HostPeerBinding, JoinedPeer};
#[cfg(feature = "managed-datagram-runtime")]
pub(crate) use peers::{InboundPeerTasks, MAX_DATAGRAM_PEERS};
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
}
impl std::fmt::Debug for InboundOwnerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InboundOwnerRegistry")
            .field(
                "owners",
                &self.entries.lock().expect("inbound owners poisoned").len(),
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
        self.entry.install_peer_tasks()
    }
    pub(crate) fn register(
        &self,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<ComponentInstanceId, EngineError> {
        let identity = self.protocols.allocate_resource_identity()?;
        self.entry.install_resource(identity, owner);
        Ok(identity)
    }
    pub(crate) fn tasks(&self) -> InboundTaskAccess {
        self.entry.task_access()
    }
}
impl InboundOwnerRegistry {
    pub(crate) fn install(
        &self,
        tag: &str,
        protocols: &ProtocolInventory,
        nested_tasks: usize,
        shutdown: watch::Sender<bool>,
    ) -> Result<InboundInstallContext, EngineError> {
        let limit = nested_tasks
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| std::io::Error::other("inbound declared task limit exhausted"))?;
        let mut entries = self.entries.lock().expect("inbound owners poisoned");
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
        entries.insert(tag.to_owned(), entry.clone());
        Ok(InboundInstallContext {
            entry,
            protocols: protocols.clone(),
        })
    }
    pub(crate) fn owner(&self, tag: &str) -> Option<Arc<InboundOwner>> {
        self.entries
            .lock()
            .expect("inbound owners poisoned")
            .get(tag)
            .cloned()
    }
    pub(crate) async fn shutdown(&self, tag: &str, deadline: Instant) -> Result<(), EngineError> {
        let Some(entry) = self.owner(tag) else {
            return Ok(());
        };
        entry.shutdown(deadline).await?;
        let mut entries = self.entries.lock().expect("inbound owners poisoned");
        if entries
            .get(tag)
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            entries.remove(tag);
        }
        Ok(())
    }
    pub(crate) async fn shutdown_all(&self, deadline: Instant) -> Result<(), EngineError> {
        let tags = self
            .entries
            .lock()
            .expect("inbound owners poisoned")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut error = None;
        for tag in tags {
            if let Err(failure) = self.shutdown(&tag, deadline).await {
                error.get_or_insert(failure);
            }
        }
        error.map_or(Ok(()), Err)
    }
}
impl InboundInstallContext {
    pub(crate) fn owner(&self) -> Arc<InboundOwner> {
        self.entry.clone()
    }
}
