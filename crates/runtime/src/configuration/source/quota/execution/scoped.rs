//! The actual finite blocking writer, held by this Source's original Scope.
use super::*;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use zero_platform_tokio::contract::{
    lifecycle::TaskClass,
    scope::{HostedScope, TaskJoinWitness, TaskJoined, TaskScope},
    ChannelError, Impact, Reason, Stage,
};

pub(super) struct Actor {
    scope: HostedScope,
    job: Option<Job>,
    failed: bool,
    sealed: bool,
    // Metadata promises follow every actual Scope/witness holder.
    slot: task::Slot,
}
struct Job {
    witness: Option<TaskJoinWitness>,
    joined: Option<TaskJoined>,
}
impl Actor {
    pub(super) fn new(tasks: Arc<TaskRegistrationBudget>, slot: task::Slot) -> io::Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(io::Error::other)?;
        let mut scope = TaskScope::suspended_hosted(runtime, NonZeroUsize::new(1));
        scope.bind_task_budget(tasks).map_err(io::Error::other)?;
        scope.enable_registration().map_err(io::Error::other)?;
        Ok(Self {
            scope,
            job: None,
            failed: false,
            sealed: false,
            slot,
        })
    }
    pub(super) fn idle(&self) -> bool {
        self.job.is_none() && !self.failed && !self.sealed
    }
    pub(super) fn drained(&self) -> bool {
        self.sealed && self.job.is_none() && self.scope.is_drained()
    }
    pub(super) fn seal(&mut self) {
        if !self.sealed {
            self.scope.begin_shutdown();
            self.sealed = true;
        }
    }
    pub(super) fn launch(
        &mut self,
        storage: &OwnedQuotaStorage,
        producer: Producer,
        gate: Arc<Gate>,
        cell: business::Cell,
        closing: bool,
    ) -> Result<(), (io::Error, business::Cell)> {
        if !self.idle() {
            return Err((io::ErrorKind::WouldBlock.into(), cell));
        }
        let launch = match self.scope.registrar().reserve_launch(TaskClass::Critical) {
            Ok(launch) => launch,
            Err(error) => return Err((io::Error::new(io::ErrorKind::WouldBlock, error), cell)),
        };
        gate.reset();
        if let Err(cell) = storage.execution.install_working_cell(cell) {
            return Err((io::Error::other("quota working frame already held"), cell));
        }
        let original = storage;
        let storage = storage.downgrade();
        // The returned real witness is installed below before opening this
        // same gate. Body/IO ownership is never reconstructed from a task ID.
        let opened = gate.clone();
        let work = move || {
            opened.wait();
            let Some(storage) = storage.upgrade() else {
                return Err(ChannelError::new(
                    Stage::Prepare,
                    Reason::Revoked,
                    Impact::Component,
                ));
            };
            let cell = storage
                .execution
                .take_working_cell()
                .expect("same gated quota input");
            let result = catch_unwind(AssertUnwindSafe(|| {
                let result = storage.ensure_writer_lease().and_then(|_| producer.write());
                storage.execution.receive_write(result, cell, closing)
            }));
            let panic = result.is_err();
            let native = storage.original_io_status();
            let cleanup = if closing || panic || native.is_err() {
                storage.close_lease()
            } else {
                Ok(())
            };
            match result {
                Err(original) => {
                    // The exact native controller already retained an OS close
                    // failure. Preserve this original panic Box as the actual
                    // Tokio blocking JoinError; no synthesized task outcome.
                    drop(cleanup);
                    resume_unwind(original)
                }
                Ok(_) => {
                    let native = match (native, cleanup) {
                        (Ok(()), Ok(())) => Ok(()),
                        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
                        (Err(acquisition), Err(cleanup)) => {
                            Err(io::Error::other(AcquisitionCleanupFailure {
                                acquisition,
                                cleanup,
                            }))
                        }
                    };
                    native.map_err(|error| {
                        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                            .with_source(error)
                    })?;
                    Ok(())
                }
            }
        };
        match launch.spawn_blocking_tracked(work) {
            Ok((_, witness)) => {
                self.job = Some(Job {
                    witness: Some(witness),
                    joined: None,
                });
                gate.release();
                Ok(())
            }
            Err(error) => {
                // The failed synchronous registration never opened the gate;
                // the original input remains in the same finite owner slot.
                let cell = original
                    .execution
                    .take_working_cell()
                    .expect("unpublished quota work retains its actual input");
                Err((io::Error::other(error), cell))
            }
        }
    }
    pub(super) fn progress(
        &mut self,
        storage: &OwnedQuotaStorage,
        cx: &mut Context<'_>,
    ) -> io::Result<()> {
        while let Poll::Ready(Some(receipt)) = self.scope.poll_exit(cx) {
            let job = self
                .job
                .as_mut()
                .ok_or_else(|| io::Error::other("unowned quota Scope exit"))?;
            let witness = job.witness.as_ref().unwrap();
            if !witness.matches_joined(receipt.joined()) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "foreign quota Scope receipt",
                ));
            }
            if receipt.result().is_err() {
                let failure = self
                    .slot
                    .receive(witness)?
                    .ok_or_else(|| io::Error::other("actual quota failure missing"))?;
                // The same Source strongly receives the paid immutable result
                // before local failure acceptance or exact Hosted ACK.
                let original = storage.execution.receive_task_failure(failure.clone());
                storage.receive_worker_failure(original, false)?;
                witness
                    .acknowledge_hosted_failure(failure.joined())
                    .map_err(io::Error::other)?;
                self.failed = true;
            }
            job.joined = Some(receipt.joined().clone());
            self.scope.acknowledge(&receipt).map_err(io::Error::other)?;
            drop(receipt);
        }
        let Some(job) = self.job.as_mut() else {
            return Ok(());
        };
        let Some(joined) = job.joined.as_ref() else {
            return Ok(());
        };
        if !joined.registration_released() {
            return Ok(());
        }
        let witness = job.witness.take().unwrap();
        let joined = job.joined.take().unwrap();
        if self.failed {
            // The pure failure delivery owns the same joined/error headers.
            // Its guard follows all returned errors; no failed result is reset.
            drop(witness);
            drop(joined);
        } else if let Err((witness, joined)) =
            TaskJoinWitness::try_retire_success_headers(witness, joined)
        {
            // No loop, allocation or new worker. Original Scope progress/tick
            // retries after real holders return. Preserve both exact handles.
            job.witness = Some(witness);
            job.joined = Some(joined);
            return Ok(());
        }
        self.job = None;
        if self.failed {
            self.seal();
        }
        Ok(())
    }
}
