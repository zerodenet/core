//! Durable observation of this exact existing Process/Endpoint declaration.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use zero_platform_tokio::owned_process::ProcessEndpointRetirement;
pub(super) struct Retirement {
    instance: ComponentInstanceId,
    ordinal: usize,
    scope: TaskScopeIdentity,
    closed: AtomicBool,
    changed: tokio::sync::Notify,
}
impl Retirement {
    pub(super) fn new(
        instance: ComponentInstanceId,
        ordinal: usize,
        scope: TaskScopeIdentity,
    ) -> Arc<Self> {
        Arc::new(Self {
            instance,
            ordinal,
            scope,
            closed: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        })
    }
    fn publish(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}
impl ProcessEndpointRetirement for Retirement {
    fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    fn process_ordinal(&self) -> usize {
        self.ordinal
    }
    fn scope_identity(&self) -> TaskScopeIdentity {
        self.scope.clone()
    }
    fn originals_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    fn wait_originals_closed(
        &self,
        deadline: Instant,
    ) -> zero_platform_tokio::contract::BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(async move {
            tokio::time::timeout_at(deadline, async {
                loop {
                    let changed = self.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if self.originals_closed() {
                        return;
                    }
                    changed.await;
                }
            })
            .await
            .map_err(|_| {
                error(
                    self.instance.component,
                    Reason::Timeout,
                    "same declared Process originals remain unconfirmed",
                )
            })
        })
    }
}
impl ProcessDeclarations {
    pub(in crate::component::instance::resources::native) fn endpoint_retirement(
        &self,
        ordinal: usize,
    ) -> Option<Arc<dyn ProcessEndpointRetirement>> {
        let entries = self.lock();
        let entry = entries.values().find(|entry| {
            entry
                .endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint.ordinal == ordinal)
        })?;
        if entry.legacy {
            return None;
        }
        Some(entry.endpoint.as_ref()?.retirement.clone()?)
    }
    pub(in crate::component::instance::resources::native::process) fn publish_endpoint_retirements(
        &self,
    ) {
        // Called only by the actual Host shutdown after acquisition admission
        // closed. Native proofs stay separate from the actor's publication ACK.
        let ready: Vec<_> = self
            .lock()
            .values()
            .filter_map(|entry| {
                let endpoint = entry.endpoint.as_ref()?;
                let jobs_closed = !entry.legacy
                    && entry.jobs.iter().all(|job| {
                        job.original
                            .closed()
                            .is_some_and(|closed| closed.matches(&job.original))
                    });
                let endpoint_closed = endpoint.original.as_ref().is_none_or(|original| {
                    original
                        .closed()
                        .is_some_and(|closed| closed.matches(original))
                });
                (jobs_closed && endpoint_closed)
                    .then(|| endpoint.retirement.clone())
                    .flatten()
            })
            .collect();
        // Arbitrary waiter callbacks never run under the declarations mutex.
        for original in ready {
            original.publish();
        }
    }
}
