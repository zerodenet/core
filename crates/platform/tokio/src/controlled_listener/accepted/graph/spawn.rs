use super::*;
use crate::contract::scope::TaskClass;
struct Starting {
    users: Arc<Users>,
    accepted: bool,
    registration_attempted: bool,
}
impl Drop for Starting {
    fn drop(&mut self) {
        if self.accepted {
            return;
        }
        {
            let mut state = self.users.state();
            state.spawning -= 1;
            state.sealed = true;
            state.unaccounted |= self.registration_attempted;
        }
        // No task was accepted by this registration. This is not a TaskExit or
        // an FD/user close receipt; escaped byte views remain independently live.
        self.users.cancel.cancel();
        self.users.changed.notify_waiters();
    }
}
impl Users {
    pub(super) fn spawn<F, Fut>(
        self: &Arc<Self>,
        class: TaskClass,
        primary: bool,
        make: F,
    ) -> Result<(TaskId, TaskJoinWitness), ChannelError>
    where
        F: FnOnce(AcceptedTcpTasks) -> Fut,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        // Scope admission and any last-owner drops happen outside graph locks.
        if !self.authority.scope.registration_open() || self.cancel.is_cancelled() {
            return Err(failure(
                Stage::Prepare,
                Reason::Revoked,
                "accepted scope admission is closed",
            ));
        }
        {
            let mut state = self.state();
            if state.sealed || state.unaccounted || (primary && state.primary) {
                return Err(failure(
                    Stage::Prepare,
                    Reason::Revoked,
                    "accepted users were sealed or handler already assigned",
                ));
            }
            if state
                .tasks
                .len()
                .checked_add(state.spawning)
                .is_none_or(|used| used >= self.authority.max_tasks.get())
            {
                return Err(failure(
                    Stage::Prepare,
                    Reason::Capacity,
                    "declared accepted task graph is full",
                ));
            }
            state.spawning += 1;
            state.primary |= primary;
        }
        let mut starting = Starting {
            users: self.clone(),
            accepted: false,
            registration_attempted: false,
        };
        let task = Arc::new(OnceLock::new());
        let work = make(AcceptedTcpTasks {
            users: Arc::downgrade(self),
            task: task.clone(),
        });
        let (admit, admitted) = tokio::sync::oneshot::channel();
        let cancel = self.cancel.clone();
        // A registrar unwind may have installed a real task before returning
        // its opaque identity. Preserve that gap instead of claiming no task.
        starting.registration_attempted = true;
        let registration = self.authority.registrar.spawn_tracked(class, async move {
            if admitted.await.is_err() {
                return Ok(());
            }
            tokio::select! { biased;
                result = work => result,
                _ = cancel.cancelled() => Ok(()),
            }
        });
        let (id, witness) = match registration {
            Ok(registration) => registration,
            Err(source) => {
                starting.registration_attempted = false;
                let reason = match source {
                    crate::contract::scope::SpawnError::Capacity => Reason::Capacity,
                    crate::contract::scope::SpawnError::IdentityExhausted => Reason::Internal,
                    crate::contract::scope::SpawnError::NotStarted => Reason::InvalidConfig,
                    crate::contract::scope::SpawnError::Closing => Reason::Revoked,
                };
                let source = Arc::new(
                    failure(
                        Stage::Prepare,
                        reason,
                        "actual accepted task registration failed",
                    )
                    .with_source(source),
                );
                self.remember(source.clone());
                return Err(retained(source));
            }
        };
        assert!(
            task.set((id, witness.clone())).is_ok(),
            "one original accepted task registration"
        );
        {
            let mut state = self.state();
            state.tasks.insert(
                id,
                Node {
                    witness: witness.clone(),
                    primary,
                },
            );
            state.spawning -= 1;
            starting.accepted = true;
        }
        // Persistent same-entry TaskId/witness precedes the first protocol poll.
        // Sending can synchronously wake external code: every graph lock is gone.
        let _ = admit.send(());
        self.changed.notify_waiters();
        Ok((id, witness))
    }
}
