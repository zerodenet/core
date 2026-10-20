//! Actual query I/O leases end before endpoint close confirmation. Acquisition
//! is fenced by EndpointOwner's setup gate; a late query cannot enter shutdown.
use std::{
    io,
    sync::atomic::{AtomicUsize, Ordering},
};
#[derive(Default)]
pub(super) struct Users {
    active: AtomicUsize,
    changed: tokio::sync::Notify,
}
pub(in super::super) struct Lease<'a>(&'a Users);
impl Users {
    pub(super) fn is_empty(&self) -> bool {
        self.active.load(Ordering::Acquire) == 0
    }
    pub(super) fn acquire(&self) -> Lease<'_> {
        self.active.fetch_add(1, Ordering::AcqRel);
        Lease(self)
    }
    pub(super) async fn drain(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_empty() {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        }
    }
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
