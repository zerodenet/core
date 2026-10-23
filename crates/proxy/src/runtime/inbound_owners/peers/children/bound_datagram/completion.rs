//! Only the sole installed provider and the original Scope can complete this.
use super::*;

struct Completion {
    identity: PeerChildIdentity,
    binding: Arc<HostPeerBinding>,
    closed: Mutex<Option<Arc<PeerBoundDatagramClosed>>>,
}
/// Retaining an observation does not retain the provider or authorize native I/O.
/// Its small completion cell is one per currently held peer resource handle.
#[derive(Clone)]
pub(crate) struct PeerBoundDatagramRegistration(Arc<Completion>);

/// Physical completion of this exact installed provider and protocol owner,
/// after the original containing Scope consumed its actual peer TaskExit.
pub(crate) struct PeerBoundDatagramClosed {
    origin: std::sync::Weak<Completion>,
    identity: PeerChildIdentity,
    joined: Arc<JoinedPeer>,
}
impl PeerBoundDatagramRegistration {
    pub(super) fn new(identity: PeerChildIdentity, binding: Arc<HostPeerBinding>) -> Self {
        Self(Arc::new(Completion {
            identity,
            binding,
            closed: Mutex::new(None),
        }))
    }
    pub(crate) fn identity(&self) -> PeerChildIdentity {
        self.0.identity
    }
    pub(crate) fn belongs_to(&self, binding: &Arc<HostPeerBinding>) -> bool {
        Arc::ptr_eq(&self.0.binding, binding)
    }
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn closed(&self) -> Option<Arc<PeerBoundDatagramClosed>> {
        self.0
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub(super) fn joined(&self, expected: TaskId) -> Result<Arc<JoinedPeer>, ChannelError> {
        let joined = self
            .0
            .binding
            .observed_join()
            .ok_or_else(|| error(Reason::Revoked, "actual provider peer has not joined"))?;
        if expected != self.0.identity.task || joined.task() != expected {
            return Err(error(
                Reason::InvalidConfig,
                "provider peer identity differs",
            ));
        }
        Ok(joined)
    }
    /// Private to this provider wrapper. The close path calls this only after
    /// both protocol and original native closure have actually completed.
    pub(super) fn confirm(&self, joined: Arc<JoinedPeer>) {
        assert_eq!(joined.task(), self.0.identity.task);
        let mut cell = self
            .0
            .closed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cell.get_or_insert_with(|| {
            Arc::new(PeerBoundDatagramClosed {
                origin: Arc::downgrade(&self.0),
                identity: self.0.identity,
                joined,
            })
        });
    }
}
impl PeerBoundDatagramClosed {
    pub(crate) fn matches(&self, registration: &PeerBoundDatagramRegistration) -> bool {
        self.origin.ptr_eq(&Arc::downgrade(&registration.0))
    }
    pub(crate) fn identity(&self) -> PeerChildIdentity {
        self.identity
    }
    pub(crate) fn joined(&self) -> Arc<JoinedPeer> {
        self.joined.clone()
    }
}
