//! Composable `ResourceLifecycle` chain that preserves start ordering, reverse
//! revocation/shutdown ordering, partial-close receipts, and first failure across
//! retries.

use std::sync::Arc;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{
        erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
    },
    ChannelError, CloseReason,
};

struct Entry {
    lifecycle: Box<dyn DynResourceLifecycle>,
    completed: bool,
    pending: Option<CloseReport>,
}

/// Composes multiple `ResourceLifecycle` owners into one persistent lifecycle.
///
/// Entries are pushed in dependency/start order (outer/underlying resource first,
/// inner/admission owner last). `start` runs forward; `revoke` and `shutdown`
/// run in reverse order. Completed entries are never shut down a second time,
/// and collected `CloseReport`s plus the first error remain retained across retries.
#[derive(Default)]
pub struct LifecycleChain {
    entries: Vec<Entry>,
    failure: Option<Arc<ChannelError>>,
}

impl LifecycleChain {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(mut self, lifecycle: Box<dyn DynResourceLifecycle>) -> Self {
        self.entries.push(Entry {
            lifecycle,
            completed: false,
            pending: None,
        });
        self
    }

    pub fn push_typed<L: ResourceLifecycle + 'static>(self, lifecycle: L) -> Self {
        self.push(erase_lifecycle(lifecycle))
    }

    pub fn push_opt(self, lifecycle: Option<Box<dyn DynResourceLifecycle>>) -> Self {
        match lifecycle {
            Some(lifecycle) => self.push(lifecycle),
            None => self,
        }
    }

    pub fn push_typed_opt<L: ResourceLifecycle + 'static>(self, lifecycle: Option<L>) -> Self {
        match lifecycle {
            Some(lifecycle) => self.push_typed(lifecycle),
            None => self,
        }
    }

    pub fn into_boxed(mut self) -> Box<dyn DynResourceLifecycle> {
        if self.entries.len() == 1 {
            return self.entries.pop().expect("single lifecycle").lifecycle;
        }
        erase_lifecycle(self)
    }
}

impl Drop for LifecycleChain {
    fn drop(&mut self) {
        self.revoke(CloseReason::Shutdown);
    }
}

impl ResourceLifecycle for LifecycleChain {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        for entry in &mut self.entries {
            entry.lifecycle.start(deadline).await?;
        }
        Ok(())
    }

    fn revoke(&self, reason: CloseReason) {
        for entry in self.entries.iter().rev() {
            entry.lifecycle.revoke(reason);
        }
    }

    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut failed = false;
        for entry in self.entries.iter_mut().rev() {
            if entry.completed {
                continue;
            }
            match entry.lifecycle.shutdown(deadline).await {
                Ok(report) => {
                    if report.is_complete() {
                        entry.completed = true;
                    }
                    match &mut entry.pending {
                        Some(existing) => {
                            existing.unfinished.clear();
                            existing.append(report);
                        }
                        None => entry.pending = Some(report),
                    }
                }
                Err(source) => {
                    failed = true;
                    self.failure.get_or_insert_with(|| {
                        source
                            .retained_source()
                            .cloned()
                            .unwrap_or_else(|| Arc::new(source))
                    });
                }
            }
        }
        if failed {
            return Err(ChannelError::retained(
                self.failure.as_ref().expect("recorded failure").clone(),
            ));
        }
        let mut merged = CloseReport::default();
        for entry in self.entries.iter_mut().rev() {
            if let Some(report) = entry.pending.take() {
                merged.append(report);
            }
        }
        Ok(merged)
    }
}
