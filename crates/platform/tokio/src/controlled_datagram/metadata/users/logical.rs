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
    /// Reserve each real view in one atomic operation; no partial group waits.
    fn acquire_group<const N: usize>(
        self: &Arc<Self>,
    ) -> Result<[LogicalUserPermit<A>; N], ChannelError> {
        assert!((1..=3).contains(&N));
        self.admission.check()?;
        if self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            if N > self.limit.get().saturating_sub(active) {
                return Err(error(Stage::Prepare, Reason::Capacity));
            }
            match self.active.compare_exchange_weak(
                active,
                active + N,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }
        let permits = std::array::from_fn(|_| LogicalUserPermit(self.clone()));
        if self.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        Ok(permits)
    }
    async fn wait_group<const N: usize>(
        self: &Arc<Self>,
    ) -> Result<[LogicalUserPermit<A>; N], ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.acquire_group() {
                Ok(permits) => return Ok(permits),
                Err(source) if source.reason == Reason::Capacity && N <= self.limit.get() => {}
                Err(source) => return Err(source),
            }
            tokio::select! { biased;
                _ = self.cancel.cancelled() => return Err(error(Stage::Prepare, Reason::Revoked)),
                _ = changed => {},
            }
        }
    }
    pub fn acquire(self: &Arc<Self>) -> Result<LogicalUserPermit<A>, ChannelError> {
        let [permit] = self.acquire_group()?;
        Ok(permit)
    }
    pub fn acquire_pair(
        self: &Arc<Self>,
    ) -> Result<(LogicalUserPermit<A>, LogicalUserPermit<A>), ChannelError> {
        let [first, second] = self.acquire_group()?;
        Ok((first, second))
    }
    pub async fn acquire_wait(self: &Arc<Self>) -> Result<LogicalUserPermit<A>, ChannelError> {
        let [permit] = self.wait_group().await?;
        Ok(permit)
    }
    pub async fn acquire_pair_wait(
        self: &Arc<Self>,
    ) -> Result<(LogicalUserPermit<A>, LogicalUserPermit<A>), ChannelError> {
        let [first, second] = self.wait_group().await?;
        Ok((first, second))
    }
    pub async fn acquire_triplet_wait(
        self: &Arc<Self>,
    ) -> Result<
        (
            LogicalUserPermit<A>,
            LogicalUserPermit<A>,
            LogicalUserPermit<A>,
        ),
        ChannelError,
    > {
        let [first, second, third] = self.wait_group().await?;
        Ok((first, second, third))
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
