//! Timer/group continuations polled by the original orchestration caller.
//! Network work remains the actual Host ProbeCall and standard Stream provider.
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{any::Any, future::Future, io, sync::Mutex};
use zero_engine::EngineError;
pub(in crate::runtime) type Outcome = Result<Result<(), EngineError>, io::Error>;
#[derive(Default)]
pub(in crate::runtime) struct Schedules {
    pending: FuturesUnordered<BoxFuture<'static, Outcome>>,
}
impl Schedules {
    pub(in crate::runtime) fn push(
        &mut self,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) {
        self.pending.push(Box::pin(async move {
            std::panic::AssertUnwindSafe(work)
                .catch_unwind()
                .await
                .map_err(|original| io::Error::other(OriginalPanic(Mutex::new(original))))
        }));
    }
    pub(in crate::runtime) async fn join_next(&mut self) -> Option<Outcome> {
        self.pending.next().await
    }
    pub(in crate::runtime) fn len(&self) -> usize {
        self.pending.len()
    }
    pub(in crate::runtime) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub(in crate::runtime) fn clear(&mut self) {
        // Return the original waiters. Their actual ProbeOriginal cancellation
        // requests native retirement; only Host Probe receipts can confirm it.
        // Destroy all old continuations before installing a new generation.
        self.pending = FuturesUnordered::new();
    }
}
struct OriginalPanic(Mutex<Box<dyn Any + Send + 'static>>);
impl std::fmt::Debug for OriginalPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for OriginalPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let original = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(message) = original.downcast_ref::<String>() {
            return f.write_str(message);
        }
        if let Some(message) = original.downcast_ref::<&'static str>() {
            return f.write_str(message);
        }
        f.write_str("URLTest continuation panicked with retained original payload")
    }
}
impl std::error::Error for OriginalPanic {}
#[cfg(test)]
#[path = "tests/urltest_schedule.rs"]
mod tests;
