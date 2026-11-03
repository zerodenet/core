use super::*;

impl ClientPool {
    pub(in crate::client::pool) fn enter_legacy(&self) -> io::Result<()> {
        let mut owner = self.ownership.lock().unwrap();
        match &*owner {
            Ownership::Pristine | Ownership::LegacyUsed => {
                *owner = Ownership::LegacyUsed;
                Ok(())
            }
            Ownership::Managed(_) => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "mieru pool belongs to a native generation; legacy dial is forbidden",
            )),
        }
    }
    pub(crate) fn claim_managed(
        &self,
        instance: ComponentInstanceId,
        limit: NonZeroUsize,
        tasks: TaskRegistrar,
        cancel: CancellationToken,
        bindings: ManagedBindings,
    ) -> io::Result<Arc<ManagedState>> {
        let ManagedBindings {
            carriers,
            plans,
            profiles,
            identity,
            dynamic,
        } = bindings;
        if carriers.is_empty()
            || carriers.iter().any(|carrier| {
                carrier
                    .grants
                    .as_ref()
                    .is_some_and(|grants| grants.instance() != instance)
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Mieru controlled grants belong to another component generation",
            ));
        }
        let mut ownership = self.ownership.lock().unwrap();
        let available = match &*ownership {
            Ownership::Pristine => true,
            Ownership::Managed(old) => {
                old.instance != instance && old.state.lock().unwrap().retired
            }
            Ownership::LegacyUsed => false,
        };
        if !available {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "mieru pool was already used or claimed; no implicit legacy driver transfer",
            ));
        }
        let scope_cancel = tasks.cancellation();
        if cancel.is_cancelled() || scope_cancel.is_cancelled() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let owner = Arc::new(ManagedState {
            instance,
            limit,
            tasks,
            cancel,
            scope_cancel,
            carriers,
            plans,
            profiles,
            identity,
            dynamic,
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        });
        let previous = std::mem::replace(&mut *ownership, Ownership::Managed(owner.clone()));
        drop(ownership);
        // Upstream/task destructors may wake arbitrary host code. Never drop
        // the confirmed previous generation under the pool ownership lock.
        drop(previous);
        Ok(owner)
    }
    pub(crate) fn confirm_retired(&self, actual: &Arc<ManagedState>) -> io::Result<()> {
        let ownership = self.ownership.lock().unwrap();
        if !matches!(&*ownership, Ownership::Managed(owner) if Arc::ptr_eq(owner, actual)) {
            return Err(io::Error::other(
                "Mieru retirement belongs to another actual owner",
            ));
        }
        let state = actual.state.lock().unwrap();
        if !state.stopped || state.users != 0 || !state.records.is_empty() {
            return Err(io::Error::other(
                "Mieru retirement still owns users or original carriers",
            ));
        }
        // Clear only this pool's confirmed generation. Never restore Pristine:
        // detached legacy drivers cannot acquire it between native generations.
        let entries = std::mem::take(&mut *self.entries.lock().unwrap());
        drop(state);
        drop(ownership);
        drop(entries);
        // Admission remains Managed, stopped and non-retired until the old
        // cache values have actually returned. Neither legacy nor a new native
        // owner can race their destruction.
        let ownership = self.ownership.lock().unwrap();
        if !matches!(&*ownership, Ownership::Managed(owner) if Arc::ptr_eq(owner, actual)) {
            return Err(io::Error::other("Mieru retirement lost its actual owner"));
        }
        actual.state.lock().unwrap().retired = true;
        Ok(())
    }
    pub(crate) fn managed(&self, instance: ComponentInstanceId) -> io::Result<Arc<ManagedState>> {
        let ownership = self.ownership.lock().unwrap();
        match &*ownership {
            Ownership::Managed(owner) if owner.instance == instance && owner.ready() => {
                Ok(owner.clone())
            }
            _ => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "mieru native pool dependency is not ready for this generation",
            )),
        }
    }
}
