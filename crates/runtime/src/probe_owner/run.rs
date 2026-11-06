//! Install the original task and its credit before diagnostic work can poll.
use super::*;
use std::{future::Future, num::NonZeroUsize};
use tokio::sync::oneshot;
use zero_platform_tokio::contract::scope::{TaskClass, TaskRegistrar};
struct CancelWaiter(ProbeCall);
impl Drop for CancelWaiter {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
impl ProbeOwner {
    pub(crate) async fn run<T, F, Fut>(
        self: &Arc<Self>,
        registrar: TaskRegistrar,
        credit: ProbeAdmission,
        limit: NonZeroUsize,
        purpose: ServicePurpose,
        deadline: Instant,
        work: F,
    ) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: FnOnce(ProbeCall) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        if !matches!(
            purpose,
            ServicePurpose::DiagnosticProbe | ServicePurpose::HealthProbe
        ) {
            return Err(failure(Reason::InvalidConfig));
        }
        let entry = Arc::new(Entry {
            owner: Arc::downgrade(self),
            stop: CancellationToken::new(),
            facts: Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose,
            }),
            deadline,
            witness: OnceLock::new(),
            pending: Mutex::new(Vec::new()),
            cleanup_started: AtomicBool::new(false),
            cleanup: Mutex::new(None),
            fault: OnceLock::new(),
            complete: AtomicBool::new(false),
            changed: Notify::new(),
            credit: Mutex::new(Some(credit)),
        });
        let call = ProbeCall(entry.clone());
        let (begin, started) = oneshot::channel();
        let (send, receive) = oneshot::channel();
        {
            let deferred = self.wake.enter();
            let callbacks = registrar.defer_callbacks();
            let mut entries = lock(&self.entries);
            if self.stopping.load(Ordering::Acquire) || entries.len() >= limit.get() {
                return Err(failure(Reason::Capacity));
            }
            let running = call.clone();
            let (_,witness)=registrar.spawn_tracked(TaskClass::Auxiliary,async move {
                if started.await.is_err() {return Ok(());}
                let result=tokio::select! {biased;_=running.0.stop.cancelled()=>None,result=work(running.clone())=>Some(result)};
                if let Some(result)=result {let _=send.send(result);}
                Ok(())
            }).map_err(|source|failure(Reason::Capacity).with_source(source))?;
            entry
                .witness
                .set(witness)
                .unwrap_or_else(|_| unreachable!("one actual probe task"));
            entries.push(entry.clone());
            drop(entries);
            drop(callbacks);
            drop(deferred);
        }
        let _waiter = CancelWaiter(call.clone());
        let _ = begin.send(());
        self.wake.signal();
        let result = receive.await;
        loop {
            let changed = entry.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(error) = entry.fault.get() {
                return Err(retained(error));
            }
            if entry.complete.load(Ordering::Acquire) {
                return result.map_err(|source| failure(Reason::Cancelled).with_source(source));
            }
            changed.await;
        }
    }
}
