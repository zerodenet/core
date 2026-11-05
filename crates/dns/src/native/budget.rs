//! Actual capacity shared by the host's Node and borrowing query purposes.
use std::{io, num::NonZeroUsize, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeDnsBudgetLimits {
    pub originals: NonZeroUsize,
    pub queries: NonZeroUsize,
    pub blocking_jobs: NonZeroUsize,
    pub h2_drivers: NonZeroUsize,
}
pub struct NativeDnsBudget {
    limits: NativeDnsBudgetLimits,
    tasks: crate::OwnedDnsQueryBudget,
    sockets: Arc<tokio::sync::Semaphore>,
    admission: tokio::sync::Mutex<()>,
}
impl NativeDnsBudget {
    pub fn new(limits: NativeDnsBudgetLimits) -> Self {
        Self {
            limits,
            tasks: crate::OwnedDnsQueryBudget::with_limits(
                limits.queries,
                limits.blocking_jobs,
                limits.h2_drivers,
            ),
            sockets: Arc::new(tokio::sync::Semaphore::new(limits.originals.get())),
            admission: Default::default(),
        }
    }
    pub fn limits(&self) -> NativeDnsBudgetLimits {
        self.limits
    }
    pub fn tasks(&self) -> crate::OwnedDnsQueryBudget {
        self.tasks.clone()
    }
    pub fn admission(&self) -> &tokio::sync::Mutex<()> {
        &self.admission
    }
    pub fn reserve(&self, cost: u32) -> io::Result<tokio::sync::OwnedSemaphorePermit> {
        self.sockets
            .clone()
            .try_acquire_many_owned(cost)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "shared native DNS original capacity exhausted",
                )
            })
    }
}
