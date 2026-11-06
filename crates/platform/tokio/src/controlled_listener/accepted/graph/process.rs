//! Metadata workers use the same finite actual accepted task nodes and Scope.
use super::*;
use crate::{LocalProcessInfo, LocalProcessLookup, LocalProcessTransport};
impl AcceptedCaller {
    pub async fn lookup_local_process(
        &self,
        source: std::net::SocketAddr,
        transport: LocalProcessTransport,
        read_limit: std::num::NonZeroUsize,
    ) -> Result<Option<LocalProcessInfo>, ChannelError> {
        self.check_open()?;
        let users = self.users.upgrade().ok_or_else(gone)?;
        let caller = self.task_join_witness().ok_or_else(gone)?;
        let original = LocalProcessLookup::prepare(
            caller,
            source,
            transport,
            users.cancel.clone(),
            read_limit,
        )?;
        users.bind_lookup_total(&original)?;
        users.install_process_lookup(self, original.clone())?;
        original.result(&users.cancel).await
    }
}
struct Installing {
    users: Arc<Users>,
    original: Arc<LocalProcessLookup>,
    caller: TaskId,
    accepted: bool,
    registration_attempted: bool,
}
impl Drop for Installing {
    fn drop(&mut self) {
        if self.accepted {
            return;
        }
        let removed = {
            let mut state = self.users.state();
            state.spawning -= 1;
            state.lookup_spawning -= 1;
            if self.registration_attempted {
                state.sealed = true;
                state.unaccounted = true;
                None // A registrar unwind cannot prove no original task exists.
            } else {
                state.tasks.get_mut(&self.caller).and_then(|node| {
                    if node
                        .process
                        .as_ref()
                        .is_some_and(|actual| Arc::ptr_eq(actual, &self.original))
                    {
                        node.process.take()
                    } else {
                        None
                    }
                })
            }
        };
        self.original.cancel();
        drop(removed);
        self.users.changed.notify_waiters();
    }
}
impl AcceptedPeerScope {
    /// Same actual peer installation inherits the original Host's weak root;
    /// this binds accounting only and cannot create a task or accepted caller.
    pub fn bind_process_lookup_totals(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        registrar: &crate::contract::scope::WeakTaskRegistrar,
        root: &Arc<crate::CarrierOriginalTotals>,
    ) -> Result<(), ChannelError> {
        if !self
            .users
            .authority
            .matches_original_scope(&registrar.scope_identity())
        {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "metadata root belongs to another actual peer Scope",
            ));
        }
        let weak = Arc::downgrade(root);
        if let Some((actual_instance, actual_root)) = self.users.lookup_totals.get() {
            return if *actual_instance == instance && actual_root.ptr_eq(&weak) {
                Ok(())
            } else {
                Err(failure(
                    Stage::Prepare,
                    Reason::StalePlan,
                    "metadata peer cannot change actual Host root",
                ))
            };
        }
        self.users.lookup_totals.set((instance, weak)).map_err(|_| {
            failure(
                Stage::Prepare,
                Reason::StalePlan,
                "metadata peer root already installed",
            )
        })
    }
}
impl Users {
    fn bind_lookup_total(&self, original: &LocalProcessLookup) -> Result<(), ChannelError> {
        let pool = self.pool.upgrade();
        let root;
        let instance;
        if let Some(binding) = pool.as_ref().and_then(|pool| pool.originals.get()) {
            if !self
                .authority
                .matches_original_scope(&binding.scope.scope_identity())
            {
                return Err(failure(
                    Stage::Prepare,
                    Reason::StalePlan,
                    "metadata caller differs from original accepted Scope",
                ));
            }
            instance = binding.instance;
            root = binding.root.upgrade();
        } else if let Some((actual, binding)) = self.lookup_totals.get() {
            instance = *actual;
            root = binding.upgrade();
        } else {
            return Err(failure(
                Stage::Prepare,
                Reason::Internal,
                "actual accepted metadata Host root is missing",
            ));
        }
        let root = root.ok_or_else(|| {
            failure(
                Stage::Prepare,
                Reason::Revoked,
                "actual metadata Host root retired",
            )
        })?;
        original.bind_original_totals(instance, &self.authority.registrar, &root)
    }
    fn process_lookups(&self) -> Vec<Arc<LocalProcessLookup>> {
        self.state()
            .tasks
            .values()
            .filter_map(|node| node.process.clone())
            .collect()
    }
    fn install_process_lookup(
        self: &Arc<Self>,
        caller: &AcceptedCaller,
        original: Arc<LocalProcessLookup>,
    ) -> Result<(), ChannelError> {
        self.reap_process_lookups();
        let (caller_id, witness) = caller.task.get().ok_or_else(gone)?;
        {
            let mut state = self.state();
            if state.sealed
                || state.unaccounted
                || self.cancel.is_cancelled()
                || !state
                    .tasks
                    .get(caller_id)
                    .is_some_and(|node| node.witness.same_registration(witness))
            {
                return Err(gone());
            }
            if state
                .tasks
                .values()
                .filter_map(|node| node.process.as_ref())
                .any(|lookup| lookup.caller().same_registration(witness))
                || state
                    .tasks
                    .values()
                    .filter(|node| node.process.is_some())
                    .count()
                    >= self.authority.max_tasks.get()
            {
                let mut cause = failure(
                    Stage::Prepare,
                    Reason::Capacity,
                    "actual accepted metadata worker budget is full",
                );
                cause.delivery = crate::contract::DeliveryState::NotSubmitted;
                return Err(cause);
            }
            // Original custody precedes the blocking registrar and all OS IO.
            state.tasks.get_mut(caller_id).unwrap().process = Some(original.clone());
            state.spawning += 1;
            state.lookup_spawning += 1;
        }
        let mut installing = Installing {
            users: self.clone(),
            original: original.clone(),
            caller: *caller_id,
            accepted: false,
            registration_attempted: true,
        };
        let start = match original.register(&self.authority.registrar) {
            Ok(start) => start,
            Err(source) => {
                installing.registration_attempted = false;
                return Err(source);
            }
        };
        let actual = original
            .witness()
            .expect("successful original blocking registration");
        let task = actual.task_id().expect("actual blocking TaskId");
        {
            let mut state = self.state();
            if let Some(node) = state.tasks.get_mut(caller_id) {
                if node
                    .process
                    .as_ref()
                    .is_some_and(|actual| Arc::ptr_eq(actual, &original))
                {
                    node.process = None;
                }
            }
            state.tasks.insert(
                task,
                Node {
                    witness: actual,
                    primary: false,
                    process: Some(original),
                    process_helper: true,
                },
            );
            state.spawning -= 1;
            state.lookup_spawning -= 1;
            installing.accepted = true;
        }
        // A cancelled original caller cannot release metadata work into IO.
        caller.check_open()?;
        start.start();
        self.changed.notify_waiters();
        Ok(())
    }
    pub(super) fn reap_process_lookups(&self) {
        let retired = self
            .process_lookups()
            .into_iter()
            .filter(|original| original.retired())
            .collect::<Vec<_>>();
        if retired.is_empty() {
            return;
        }
        let removed = {
            let mut state = self.state();
            let ids = state
                .tasks
                .iter()
                .filter(|(_, node)| {
                    node.process_helper
                        && node.process.as_ref().is_some_and(|original| {
                            original
                                .witness()
                                .is_some_and(|actual| actual.same_registration(&node.witness))
                                && retired.iter().any(|actual| Arc::ptr_eq(actual, original))
                        })
                })
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| state.tasks.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(removed);
        self.changed.notify_waiters();
    }
    pub(super) fn cancel_process_lookups(&self) {
        for original in self.process_lookups() {
            original.cancel();
        }
    }
    pub(super) async fn retire_process_lookups(&self, deadline: Instant) -> bool {
        for original in self.process_lookups() {
            if let Err(source) = original.wait_retired(deadline).await {
                self.remember(Arc::new(source));
                return false;
            }
        }
        self.reap_process_lookups();
        true
    }
}
fn gone() -> ChannelError {
    failure(
        Stage::Prepare,
        Reason::Revoked,
        "actual metadata caller retired",
    )
}

#[cfg(test)]
mod tests;
