use super::*;
/// A real ProbeOwner cleanup source, independent of the task's work result.
/// No TaskFailure/TaskId, IO authority or NativeClosed proof is fabricated.
#[derive(Clone, Debug)]
pub struct ProbeOwnershipFailure {
    original: Arc<ChannelError>,
    entry: Weak<Entry>,
    settled: Arc<AtomicBool>,
}
impl ProbeOwnershipFailure {
    /// Wait only for this same ProbeOwner entry's positive physical retirement.
    /// Caller/worker/Scope faults stay in their original receivers independently.
    pub async fn wait_settled(&self) -> Result<(), ChannelError> {
        if self.settled.load(Ordering::Acquire) {
            return Ok(());
        }
        let entry = self
            .entry
            .upgrade()
            .ok_or_else(|| failure(Reason::Revoked))?;
        loop {
            let changed = entry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if entry.complete.load(Ordering::Acquire) {
                return Ok(());
            }
            changed.await;
        }
    }
    pub fn original(&self) -> &Arc<ChannelError> {
        &self.original
    }
    /// A real request receiver must already hold this same typed source.
    pub fn acknowledge(&self, receipt: &crate::CommandCleanupAccepted) -> bool {
        let Some(source) = receipt.retained_source() else {
            return false;
        };
        if !self.contained_in(source.as_ref()) {
            return false;
        }
        self.accept()
    }
    fn contained_in(&self, error: &(dyn std::error::Error + 'static)) -> bool {
        if error.downcast_ref::<Self>().is_some_and(|actual| {
            actual.entry.ptr_eq(&self.entry) && Arc::ptr_eq(&actual.original, &self.original)
        }) {
            return true;
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            if error
                .get_ref()
                .is_some_and(|source| self.contained_in(source))
            {
                return true;
            }
        }
        error
            .source()
            .is_some_and(|source| self.contained_in(source))
    }
    fn accept(&self) -> bool {
        let Some(entry) = self.entry.upgrade() else {
            return false;
        };
        if !entry
            .fault
            .get()
            .is_some_and(|actual| Arc::ptr_eq(actual, &self.original))
        {
            return false;
        }
        entry.fault_received.store(true, Ordering::Release);
        entry.changed.notify_waiters();
        if let Some(owner) = entry.owner.upgrade() {
            owner.wake.signal();
        }
        true
    }
}
impl std::fmt::Display for ProbeOwnershipFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.original, f)
    }
}
impl std::error::Error for ProbeOwnershipFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.original.as_ref())
    }
}
pub(super) fn retained_probe(error: &Arc<ChannelError>, entry: &Arc<Entry>) -> ChannelError {
    retained(error).with_source(ProbeOwnershipFailure {
        original: error.clone(),
        entry: Arc::downgrade(entry),
        settled: entry.complete.clone(),
    })
}

impl ProbeOwner {
    /// Called only by the existing catalog's actual failure observer. Construct
    /// the strongly typed returned source before recording that real delivery.
    pub(crate) fn original_failure(&self) -> Option<ChannelError> {
        let entries = lock(&self.entries).clone();
        for entry in entries {
            if entry.fault_received.load(Ordering::Acquire) {
                continue;
            }
            if let Some(source) = entry.fault.get() {
                let returned = retained_probe(source, &entry);
                entry.fault_received.store(true, Ordering::Release);
                entry.changed.notify_waiters();
                self.wake.signal();
                return Some(returned);
            }
        }
        None
    }
}
