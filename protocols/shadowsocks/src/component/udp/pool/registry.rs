//! Actual canonical cache ownership; historical selections never retarget.
use super::*;
#[derive(Debug, Default)]
pub(crate) struct PoolRegistry {
    roots: Mutex<HashMap<String, Arc<PoolRoot>>>,
    budget: Arc<crate::transport::udp_socket::responses::budget::PendingBudget>,
}
impl PoolRegistry {
    pub(crate) fn compact(&self) {
        let roots: Vec<_> = self.roots.lock().unwrap().values().cloned().collect();
        for root in roots {
            root.compact();
        }
    }
    pub(crate) fn selection(&self, key: &str) -> PoolSelection {
        let root = self
            .roots
            .lock()
            .unwrap()
            .entry(key.into())
            .or_insert_with(|| {
                Arc::new(PoolRoot {
                    state: Mutex::default(),
                    budget: self.budget.clone(),
                })
            })
            .clone();
        let epoch = root.state.lock().unwrap().epoch.clone();
        PoolSelection { root, epoch }
    }
}
#[derive(Clone, Debug)]
pub(crate) struct PoolSelection {
    pub(super) root: Arc<PoolRoot>,
    epoch: Arc<()>,
}
impl PoolSelection {
    pub(crate) fn current(&self) -> io::Result<Option<Arc<Managed>>> {
        self.root.current(&self.epoch)
    }
}
#[derive(Default)]
pub(super) struct RootState {
    pub(super) epoch: Arc<()>,
    pub(super) ownership: Ownership,
}
#[derive(Default)]
pub(crate) struct PoolRoot {
    pub(super) state: Mutex<RootState>,
    pub(super) budget: Arc<crate::transport::udp_socket::responses::budget::PendingBudget>,
}
#[derive(Default, Clone)]
pub(super) enum Ownership {
    #[default]
    Cold,
    Library,
    Native(Arc<Managed>),
    Retired(Arc<Retired>),
}
impl std::fmt::Debug for PoolRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SsUdpCanonicalRoot").finish_non_exhaustive()
    }
}
impl PoolRoot {
    fn current(&self, epoch: &Arc<()>) -> io::Result<Option<Arc<Managed>>> {
        let mut state = self.state.lock().unwrap();
        if !Arc::ptr_eq(epoch, &state.epoch) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "old SS UDP cache selection cannot move to a new original",
            ));
        }
        match &state.ownership {
            Ownership::Cold => {
                state.ownership = Ownership::Library;
                Ok(None)
            }
            Ownership::Library => Ok(None),
            Ownership::Native(actual) => Ok(Some(actual.clone())),
            Ownership::Retired(_) => Err(io::ErrorKind::Interrupted.into()),
        }
    }
    pub(super) fn claim(&self, managed: Arc<Managed>) -> Result<(), ChannelError> {
        loop {
            let previous = self.state.lock().unwrap().ownership.clone();
            match &previous {
                Ownership::Library => {
                    return Err(error(managed.instance, Stage::Prepare, Reason::Capacity)
                        .with_source(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            "existing library UDP original closure remains unconfirmed",
                        )))
                }
                Ownership::Native(old) if !old.confirmed() => {
                    return Err(error(managed.instance, Stage::Prepare, Reason::Capacity))
                }
                Ownership::Retired(closed) if !closed.valid() => {
                    return Err(error(managed.instance, Stage::Prepare, Reason::StalePlan))
                }
                _ => {}
            }
            let mut slot = self.state.lock().unwrap();
            let same = match (&slot.ownership, &previous) {
                (Ownership::Cold, Ownership::Cold) => true,
                (Ownership::Native(a), Ownership::Native(b)) => Arc::ptr_eq(a, b),
                (Ownership::Retired(a), Ownership::Retired(b)) => Arc::ptr_eq(a, b),
                _ => false,
            };
            if !same {
                drop(slot);
                continue;
            }
            if !matches!(previous, Ownership::Cold) {
                slot.epoch = Arc::new(());
            }
            let old = std::mem::replace(&mut slot.ownership, Ownership::Native(managed));
            drop(slot);
            drop(old);
            return Ok(());
        }
    }
    fn compact(&self) {
        let old = match &self.state.lock().unwrap().ownership {
            Ownership::Native(actual) => actual.clone(),
            _ => return,
        };
        if !old.confirmed() {
            return;
        }
        let owner = old.grants.registration();
        let (Some(native), Some(users)) = (owner.closed(), owner.users_closed()) else {
            return;
        };
        let proof = Arc::new(Retired {
            owner,
            native,
            users,
        });
        if !proof.valid() {
            return;
        }
        let prior = {
            let mut slot = self.state.lock().unwrap();
            if !matches!(&slot.ownership,Ownership::Native(actual) if Arc::ptr_eq(actual,&old)) {
                return;
            }
            std::mem::replace(&mut slot.ownership, Ownership::Retired(proof))
        };
        // Drop the credential/codec/driver record census outside every metadata
        // lock. Unknown originals and live views never reach this branch.
        drop(prior);
    }
}
pub(super) struct Retired {
    owner: zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration,
    native: zero_platform_tokio::controlled_carrier::CarrierOwnerClosed,
    users: zero_platform_tokio::controlled_carrier::CarrierOwnerUsersClosed,
}
impl Retired {
    fn valid(&self) -> bool {
        self.native.matches(&self.owner) && self.users.matches(&self.owner)
    }
}
