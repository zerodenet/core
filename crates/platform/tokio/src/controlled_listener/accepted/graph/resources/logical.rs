//! Reusable logical custody in the original accepted entry, not another task owner.
use super::*;

/// Retains the exact original owner. A child body's return never releases it.
pub struct AcceptedResource {
    users: Weak<Users>,
    original: Arc<Retained>,
}
impl AcceptedCaller {
    /// Ordinal zero is reserved for an association's original UDP listener.
    /// Remaining declared slots may be reused only after real child cleanup.
    pub fn retain_logical_resource(
        &self,
        owner: Box<dyn DynResourceLifecycle>,
    ) -> Result<AcceptedResource, ChannelError> {
        self.retain_logical_resource_bound(owner, None)
    }
    /// Same existing graph and already registered task, installed atomically
    /// before opening that task's start gate. No task or native I/O is created.
    pub fn retain_logical_resource_with_driver(
        &self,
        owner: Box<dyn DynResourceLifecycle>,
        driver: &AcceptedTasks,
    ) -> Result<AcceptedResource, ChannelError> {
        self.retain_logical_resource_bound(owner, Some(driver))
    }
    fn retain_logical_resource_bound(
        &self,
        owner: Box<dyn DynResourceLifecycle>,
        driver: Option<&AcceptedTasks>,
    ) -> Result<AcceptedResource, ChannelError> {
        self.check_open()?;
        let users = self.users.upgrade().ok_or_else(gone)?;
        let driver = match driver {
            Some(driver) => {
                if !self.users.ptr_eq(&driver.users)
                    || driver
                        .task
                        .get()
                        .is_none_or(|(_, witness)| witness.is_joined())
                {
                    return Err(gone());
                }
                let actual = OnceLock::new();
                assert!(actual.set(driver.task.clone()).is_ok());
                actual
            }
            None => OnceLock::new(),
        };
        if users.peer.is_none() && users.pool.upgrade().is_none() {
            return Err(gone());
        }
        let mut state = users.state();
        if state.sealed
            || self
                .task
                .get()
                .is_none_or(|(_, witness)| witness.is_joined())
        {
            return Err(gone());
        }
        let ordinal = (1..users.authority.max_resources)
            .find(|ordinal| {
                !state
                    .resources
                    .iter()
                    .any(|resource| resource.ordinal == *ordinal)
            })
            .ok_or_else(|| {
                failure(
                    Stage::Prepare,
                    Reason::Capacity,
                    "accepted logical custody exhausted",
                )
            })?;
        let original = Arc::new(Retained {
            ordinal,
            owner: AsyncMutex::new(Some(owner)),
            complete: AtomicBool::new(false),
            logical_owner: Some(self.task.clone()),
            driver,
            cleanup_delivery: OnceLock::new(),
        });
        state.resources.push(original.clone());
        users.logical_charge(true);
        Ok(AcceptedResource {
            users: self.users.clone(),
            original,
        })
    }
}
impl AcceptedResource {
    /// Called inside the actual spawn constructor, before admission or I/O.
    /// The same scope publishes its original task/witness into this exact cell.
    pub fn bind_driver(&self, tasks: &AcceptedTasks) -> Result<(), ChannelError> {
        if !self.users.ptr_eq(&tasks.users) {
            return Err(gone());
        }
        let users = self.users.upgrade().ok_or_else(gone)?;
        let state = users.state();
        if self.original.complete()
            || self
                .original
                .logical_owner
                .as_ref()
                .and_then(|owner| owner.get())
                .is_some_and(|(_, witness)| witness.is_joined())
            || tasks
                .task
                .get()
                .is_some_and(|(_, witness)| witness.is_joined())
            || !state
                .resources
                .iter()
                .any(|resource| Arc::ptr_eq(resource, &self.original))
        {
            return Err(gone());
        }
        self.original
            .driver
            .set(tasks.task.clone())
            .map_err(|_| gone())
    }
    pub async fn close(&self, deadline: Instant) -> Result<bool, ChannelError> {
        if self.original.complete() {
            return Ok(true);
        }
        let (_, witness) = self
            .original
            .driver
            .get()
            .and_then(|driver| driver.get())
            .ok_or_else(gone)?;
        tokio::time::timeout_at(deadline, witness.wait())
            .await
            .map_err(|source| {
                failure(
                    Stage::Close,
                    Reason::Timeout,
                    "logical driver join remains pending",
                )
                .with_source(source)
            })?;
        let Some(users) = self.users.upgrade() else {
            return if self.original.complete() {
                Ok(true)
            } else {
                Err(gone())
            };
        };
        if !users.close_resource(&self.original, deadline).await {
            let source = users.first_failure();
            return source.map_or(Ok(false), |source| Err(retained(source)));
        }
        let removed = {
            let mut state = users.state();
            let index = state
                .resources
                .iter()
                .position(|resource| Arc::ptr_eq(resource, &self.original));
            index.map(|index| state.resources.remove(index))
        };
        drop(removed);
        Ok(true)
    }
}

impl Users {
    /// The same host observer calls this after consuming original Scope exits.
    /// No helper owns a task or invents a task-completion/native-close receipt.
    pub(in crate::controlled_listener::accepted::graph) async fn retire_joined_logical_resources(
        &self,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        let resources = self.state().resources.clone();
        for resource in resources {
            let Some(creator) = &resource.logical_owner else {
                continue;
            };
            // An unpublished bound registration remains charged. Never turn
            // its creator's join into that driver's own join receipt.
            let origin = match resource.driver.get() {
                Some(driver) => driver.get(),
                None => creator.get(),
            };
            let Some((_, witness)) = origin else {
                continue;
            };
            if !witness.is_joined() {
                continue;
            }
            if !self.close_resource(&resource, deadline).await {
                let source = self.first_failure();
                return Err(source.map_or_else(
                    || {
                        failure(
                            Stage::Close,
                            Reason::Timeout,
                            "logical original cleanup remains pending",
                        )
                    },
                    retained,
                ));
            }
            let removed = {
                let mut state = self.state();
                state
                    .resources
                    .iter()
                    .position(|active| Arc::ptr_eq(active, &resource))
                    .map(|index| state.resources.remove(index))
            };
            drop(removed);
        }
        Ok(())
    }
}
