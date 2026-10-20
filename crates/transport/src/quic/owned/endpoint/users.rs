use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{CancellationToken, ChannelError, Impact, Reason, Stage};

pub(super) struct Users {
    limit: NonZeroUsize,
    active: AtomicUsize,
    changed: Notify,
    pub(super) cancel: CancellationToken,
    admission: super::super::drivers::Admission,
}
pub(super) struct Permit(pub(super) Arc<Users>);
impl Users {
    pub(super) fn new(
        limit: NonZeroUsize,
        cancel: CancellationToken,
        admission: super::super::drivers::Admission,
    ) -> Arc<Self> {
        Arc::new(Self {
            limit,
            active: AtomicUsize::new(0),
            changed: Notify::new(),
            cancel,
            admission,
        })
    }
    pub(super) fn acquire(self: &Arc<Self>) -> Result<Permit, ChannelError> {
        self.admission.check()?;
        if self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            if active >= self.limit.get() {
                return Err(error(Stage::Prepare, Reason::Capacity));
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }
        let permit = Permit(self.clone());
        if self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        Ok(permit)
    }
    pub(super) fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    pub(super) async fn wait_empty(&self, deadline: Instant) -> Result<(), ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active() == 0 {
                return Ok(());
            }
            timeout_at(deadline, changed)
                .await
                .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let previous = self.0.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        self.0.changed.notify_waiters();
    }
}
pub(super) fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
pub(super) fn protocol(
    stage: Stage,
    source: impl std::error::Error + Send + Sync + 'static,
) -> ChannelError {
    error(stage, Reason::Protocol).with_source(source)
}
