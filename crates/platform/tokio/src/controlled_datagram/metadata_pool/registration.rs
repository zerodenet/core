use super::*;
#[derive(Clone)]
pub struct MetadataPoolRegistration {
    pub(super) owner: Weak<Shared>,
    instance: ComponentInstanceId,
    limit: NonZeroUsize,
    closed: Arc<OnceLock<()>>,
}
#[derive(Clone)]
pub struct MetadataPoolOwnerClosed {
    instance: ComponentInstanceId,
    closed: Arc<OnceLock<()>>,
}
impl MetadataPoolRegistration {
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
        self.closed.get().map(|()| MetadataPoolOwnerClosed {
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
    pub fn matches(&self, actual: &MetadataPoolRegistration) -> bool {
        self.instance == actual.instance
            && Arc::ptr_eq(&self.closed, &actual.closed)
            && self.closed.get().is_some()
    }
}
