//! The actual logical user counter and leases, moved from the Quinn owner.
//! No receipt can be obtained from a mutable public summary.
use crate::contract::{CancellationToken, ChannelError, Impact, Reason, Stage};
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

pub struct LogicalUsers<A: UserAdmission> {
    limit: NonZeroUsize,
    active: AtomicUsize,
    changed: Notify,
    cancel: CancellationToken,
    admission: A,
}
pub struct LogicalUserPermit<A: UserAdmission>(Arc<LogicalUsers<A>>);

pub trait UserAdmission: Send + Sync {
    fn check(&self) -> Result<(), ChannelError>;
}

impl<A: UserAdmission> LogicalUserPermit<A> {
    pub fn users(&self) -> &Arc<LogicalUsers<A>> {
        &self.0
    }
}
impl<A: UserAdmission> LogicalUsers<A> {
    pub fn new(limit: NonZeroUsize, cancel: CancellationToken, admission: A) -> Arc<Self> {
        Arc::new(Self {
            limit,
            active: AtomicUsize::new(0),
            changed: Notify::new(),
            cancel,
            admission,
        })
    }
    pub fn acquire(self: &Arc<Self>) -> Result<LogicalUserPermit<A>, ChannelError> {
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
        let permit = LogicalUserPermit(self.clone());
        if self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        Ok(permit)
    }
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    pub async fn wait_empty(&self, deadline: Instant) -> Result<(), ChannelError> {
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
impl<A: UserAdmission> Drop for LogicalUserPermit<A> {
    fn drop(&mut self) {
        let previous = self.0.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
        self.0.changed.notify_waiters();
    }
}

fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Session)
}
