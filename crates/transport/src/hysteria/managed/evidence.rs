//! A closed original leaves only a weak header and its sealed completion fence.
//! No cached client, native provider, configuration or task owner is retained.
use super::*;
pub(super) struct Evidence {
    pub(super) closed: AtomicBool,
    _memory: PacketMetadataCredit,
}
#[derive(Clone)]
pub(in crate::hysteria) struct OwnerRecord {
    pub(in crate::hysteria) instance: ComponentInstanceId,
    pub(in crate::hysteria) scope: TaskScopeIdentity,
    owner: std::sync::Weak<ManagedPool>,
    evidence: Arc<Evidence>,
}
impl OwnerRecord {
    pub(in crate::hysteria) fn new(owner: &Arc<ManagedPool>) -> Self {
        Self {
            instance: owner.instance,
            scope: owner.scope.clone(),
            owner: Arc::downgrade(owner),
            evidence: owner.evidence.clone(),
        }
    }
    pub(in crate::hysteria) fn is_closed(&self) -> bool {
        self.evidence.closed.load(Ordering::Acquire)
    }
    pub(in crate::hysteria) fn actual(&self) -> Option<Arc<ManagedPool>> {
        self.owner.upgrade()
    }
}
impl Evidence {
    pub(super) fn prepare(memory: &PacketDriverMetadataView) -> Result<Arc<Self>, ChannelError> {
        // Keep the weak ManagedPool allocation header prepaid until every
        // corresponding ownership-census record is reclaimed as well.
        let layout = PacketMetadataLayout::arc::<Self>()
            .and_then(|l| l.checked_add(PacketMetadataLayout::arc::<ManagedPool>()?))
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        let credit = memory
            .reserve(layout)
            .map_err(|source| error(Reason::Capacity).with_source(source))?;
        Ok(Arc::new(Self {
            closed: false.into(),
            _memory: credit,
        }))
    }
}
