//! One actual configuration's pre-bind owners, retained before native creation.
mod grant;
mod lease;
mod retirement;
use super::*;
use crate::protocol_registry::PreparedInboundBind;
pub(crate) use lease::{InboundBindLease, LeasedInbound};
use std::{collections::BTreeMap, sync::Weak};
use zero_config::RuntimeConfig;

#[derive(Default)]
pub(super) struct BindReservations {
    pub(super) closing: bool,
    config: Weak<RuntimeConfig>,
    pub(super) entries: BTreeMap<String, Arc<InboundOwner>>,
}
impl InboundOwnerRegistry {
    pub(crate) fn reserve_listener(
        self: &Arc<Self>,
        tag: &str,
        config: &Arc<RuntimeConfig>,
        protocols: &ProtocolInventory,
        prepared: PreparedInboundBind,
        shutdown: watch::Sender<bool>,
        max_bind_attempts: NonZeroUsize,
    ) -> Result<InboundBindLease, EngineError> {
        if !config.inbounds.iter().any(|inbound| inbound.tag == tag) {
            return Err(io(
                "listener reservation is outside its actual configuration",
            ));
        }
        let limit = prepared
            .internal_tasks()
            .checked_add(1)
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| io("inbound declared task limit exhausted"))?;
        let mut reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if reserved.closing {
            return Err(super::preparing::stopped_preparation());
        }
        if !reserved.entries.is_empty() && !reserved.config.ptr_eq(&Arc::downgrade(config)) {
            return Err(io(
                "previous listener reservation batch still requires confirmed cleanup",
            ));
        }
        if reserved.entries.contains_key(tag) {
            return Err(io("this listener already has a retained bind reservation"));
        }
        let identity = protocols.allocate_resource_identity()?;
        let owner = Arc::new(InboundOwner::new(identity, limit, shutdown.clone()));
        let grant = Arc::new(
            grant::NativeTcpGrant::new(
                identity,
                &prepared.resources()[0],
                prepared.address(),
                max_bind_attempts,
                owner.task_access().cancellation(),
            )
            .map_err(std::io::Error::other)?,
        );
        // Construction above is cold. The unique actual native holder is a
        // registered resource before this owner or any bind capability escapes.
        owner.install_resource(
            identity,
            zero_platform_tokio::contract::lifecycle::erase_lifecycle(grant::TcpResource(
                grant.clone(),
            )),
        )?;
        reserved.config = Arc::downgrade(config);
        reserved.entries.insert(tag.to_owned(), owner.clone());
        Ok(InboundBindLease::new(
            Arc::downgrade(self),
            tag.to_owned(),
            protocols.clone(),
            prepared,
            owner,
            grant,
            shutdown,
        ))
    }
    pub(crate) fn open_bind_admission(&self) -> Result<(), EngineError> {
        let active = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !active.is_empty() || !reserved.entries.is_empty() {
            return Err(io("old listener owners still require confirmed cleanup"));
        }
        reserved.closing = false;
        Ok(())
    }
    pub(super) fn close_bind_admission(&self) {
        self.reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closing = true;
    }
}
fn io(message: &'static str) -> EngineError {
    std::io::Error::other(message).into()
}
#[cfg(test)]
mod tests;
