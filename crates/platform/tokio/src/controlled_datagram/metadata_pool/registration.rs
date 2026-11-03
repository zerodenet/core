use super::*;
#[derive(Clone)]
pub struct MetadataPoolRegistration {
    pub(super) owner: Weak<Shared>,
    instance: ComponentInstanceId,
    limit: NonZeroUsize,
    closed: Arc<OnceLock<bool>>,
}
#[derive(Clone)]
pub struct MetadataPoolOwnerClosed {
    instance: ComponentInstanceId,
    closed: Arc<OnceLock<bool>>,
}
impl MetadataPoolRegistration {
    pub fn binding_retired(
        &self,
        expected: &(
            zero_core::request::Destination,
            crate::contract::network::UpstreamBinding,
        ),
    ) -> bool {
        self.owner.upgrade().is_none_or(|owner| {
            owner.cancel.is_cancelled()
                || owner
                    .state
                    .lock()
                    .expect("metadata pool poisoned")
                    .last_binding
                    .as_ref()
                    .is_some_and(|actual| actual != expected)
        })
    }
    pub async fn binding_retired_wait(
        &self,
        expected: &(
            zero_core::request::Destination,
            crate::contract::network::UpstreamBinding,
        ),
    ) {
        let (changed, cancel) = match self.owner.upgrade() {
            Some(owner) => (owner.changed.clone(), owner.cancel.clone()),
            None => return,
        };
        loop {
            let notified = changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.binding_retired(expected) {
                return;
            }
            tokio::select! { biased; _ = cancel.cancelled() => return, _ = notified => {} }
        }
    }
    pub fn current_original_for(
        &self,
        expected: &(
            zero_core::request::Destination,
            crate::contract::network::UpstreamBinding,
        ),
    ) -> Option<MetadataPoolOrigin> {
        let owner = self.owner.upgrade()?;
        let slots = owner
            .state
            .lock()
            .ok()?
            .slots
            .values()
            .cloned()
            .collect::<Vec<_>>();
        slots.into_iter().rev().find_map(|slot| {
            let origin = MetadataPoolPermit(slot).origin();
            (!origin.is_retired() && origin.selected_binding().as_ref() == Some(expected))
                .then_some(origin)
        })
    }
    pub fn is_retired(&self) -> bool {
        self.owner
            .upgrade()
            .is_none_or(|owner| owner.cancel.is_cancelled())
    }
    pub async fn retired(&self) {
        let cancel = match self.owner.upgrade() {
            Some(owner) => owner.cancel.clone(),
            None => return,
        };
        cancel.cancelled().await;
    }
    /// Actual current original facts, never a synthesized endpoint or native
    /// receipt. Missing/busy originals remain unavailable to observers.
    pub fn current_original(&self) -> Option<MetadataPoolOrigin> {
        let owner = self.owner.upgrade()?;
        let slots = owner
            .state
            .lock()
            .ok()?
            .slots
            .values()
            .cloned()
            .collect::<Vec<_>>();
        slots.into_iter().rev().find_map(|slot| {
            let origin = MetadataPoolPermit(slot).origin();
            (!origin.is_retired() && origin.endpoints().is_some()).then_some(origin)
        })
    }
    pub(super) fn new(owner: &Arc<Shared>) -> Self {
        Self {
            owner: Arc::downgrade(owner),
            instance: owner.instance,
            limit: owner.limit,
            closed: owner.closed.clone(),
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn limit(&self) -> NonZeroUsize {
        self.limit
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.closed, &other.closed)
    }
    pub fn closed(&self) -> Option<MetadataPoolOwnerClosed> {
        self.closed.get().map(|_| MetadataPoolOwnerClosed {
            instance: self.instance,
            closed: self.closed.clone(),
        })
    }
    pub fn revoke(&self) {
        if let Some(owner) = self.owner.upgrade() {
            owner.cancel.cancel();
            owner.state.lock().expect("metadata pool poisoned").stopped = true;
        }
    }
}
impl MetadataPoolOwnerClosed {
    pub fn had_native_acquisition(&self) -> bool {
        self.closed.get() == Some(&true)
    }
    pub fn matches(&self, actual: &MetadataPoolRegistration) -> bool {
        self.instance == actual.instance
            && Arc::ptr_eq(&self.closed, &actual.closed)
            && self.closed.get().is_some()
    }
}
