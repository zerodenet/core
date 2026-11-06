//! Cold same-host process reservation before its actor receives authority.
use super::*;
impl HostResources {
    pub(in crate::component) fn prepare_process(
        &self,
        request: ProcessRequest,
        tasks: &TaskRegistrar,
    ) -> Result<Arc<OwnedProcess>, ChannelError> {
        if self
            .processes
            .lock()
            .get(&request.program)
            .is_some_and(|entry| entry.endpoint.is_some())
        {
            return Err(self.denied());
        }
        self.prepare_process_original(request, tasks)
    }
    pub(super) fn prepare_process_original(
        &self,
        request: ProcessRequest,
        tasks: &TaskRegistrar,
    ) -> Result<Arc<OwnedProcess>, ChannelError> {
        // Original Scope observations occur outside host locks: upgrading a
        // weak scope can deliver callbacks when its last owner is released.
        if !self.processes.validate(tasks) {
            return Err(self.denied());
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancel.is_cancelled() || state.closed {
            return Err(error(
                self.component,
                Reason::Revoked,
                "native process owner revoked",
            ));
        }
        let deadline = state.start_deadline.ok_or_else(|| {
            error(
                self.component,
                Reason::InvalidConfig,
                "owned process requires actual start phase",
            )
        })?;
        if Instant::now() >= deadline {
            return Err(error(
                self.component,
                Reason::Timeout,
                "owned process start deadline elapsed",
            ));
        }
        let remaining = state
            .remaining
            .get_mut(&request.program)
            .ok_or_else(|| self.denied())?;
        if *remaining == 0 {
            return Err(error(
                self.component,
                Reason::Capacity,
                "declared process original budget exhausted",
            ));
        }
        let program = request.program.clone();
        let mut entries = self.processes.lock();
        let entry = entries.get_mut(&program).ok_or_else(|| self.denied())?;
        if entry.legacy {
            return Err(error(
                self.component,
                Reason::InvalidConfig,
                "legacy process has no tree original",
            ));
        }
        let original = OwnedProcess::prepare(request, tasks).map_err(|source| {
            error(
                self.component,
                Reason::Io,
                "cold process original preparation failed",
            )
            .with_source(source)
        })?;
        let ordinal = entry.ordinal;
        entry.jobs.push(Job {
            original: original.clone(),
            reported: false,
        });
        *remaining -= 1; // pending/failed/retiring reservations are not refunded
        drop(entries);
        drop(state);
        let instance = self
            .processes
            .identity
            .as_ref()
            .expect("validated actual process instance")
            .0;
        let reservation = match self
            .processes
            .prepaid
            .get()
            .filter(|paid| paid.ordinal() == ordinal)
        {
            Some(paid) => self
                .processes
                .budget
                .reserve_prepaid(instance, ordinal, &original, tasks, paid),
            None => self.processes.budget.reserve(instance, &original),
        };
        if let Err(source) = reservation {
            // This reservation has never escaped to its actor and acquired
            // no native resource. Preserve/finish that exact cold original
            // before refunding only its unused declaration start permission.
            original.request_stop();
            let closed = original.try_finish().map_err(|source| {
                error(
                    self.component,
                    Reason::Io,
                    "unused process original cleanup failed",
                )
                .with_source(source)
            })?;
            if !closed
                .is_some_and(|closed| closed.matches(&original) && !closed.had_native_acquisition())
            {
                return Err(error(
                    self.component,
                    Reason::Io,
                    "unused process original remains unconfirmed",
                ));
            }
            let removed = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut entries = self.processes.lock();
                let entry = entries
                    .get_mut(&program)
                    .expect("same declared process owner");
                let index = entry
                    .jobs
                    .iter()
                    .position(|job| Arc::ptr_eq(&job.original, &original))
                    .expect("same cold original");
                let removed = entry.jobs.remove(index);
                *state
                    .remaining
                    .get_mut(&program)
                    .expect("same declaration budget") += 1;
                removed
            };
            drop(removed);
            let reason = match source.kind() {
                std::io::ErrorKind::WouldBlock => Reason::Capacity,
                std::io::ErrorKind::PermissionDenied => Reason::Revoked,
                std::io::ErrorKind::AlreadyExists => Reason::StalePlan,
                _ => Reason::Io,
            };
            let mut failure = error(
                self.component,
                reason,
                "actual kernel process job reservation refused",
            )
            .with_source(source);
            failure.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
            return Err(failure);
        }
        if self.cancel.is_cancelled()
            || self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed
        {
            original.request_stop();
            let _ = original.try_finish();
            return Err(error(
                self.component,
                Reason::Revoked,
                "process owner revoked during cold reservation",
            ));
        }
        Ok(original)
    }
}
