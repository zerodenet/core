//! The contract fixture's provider publishes only its matching original closes.
use super::*;
use zero_platform_tokio::{
    contract::scope::TaskScopeIdentity, owned_process::ProcessEndpointRetirement,
};
pub(super) struct Retirement {
    pub(super) scope: TaskScopeIdentity,
    generation: u64,
    closed: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}
impl Retirement {
    pub(super) fn new(scope: &TaskScope, generation: u64) -> Self {
        Self {
            scope: scope.registrar().scope_identity(),
            generation,
            closed: Default::default(),
            changed: Default::default(),
        }
    }
    pub(super) fn publish(&self) {
        self.closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
    }
}
impl ProcessEndpointRetirement for Retirement {
    fn instance(&self) -> zero_core::component::ComponentInstanceId {
        zero_core::component::ComponentInstanceId {
            component: ComponentId(770),
            generation: ResourceGeneration(self.generation),
        }
    }
    fn process_ordinal(&self) -> usize {
        0
    }
    fn scope_identity(&self) -> TaskScopeIdentity {
        self.scope.clone()
    }
    fn originals_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    fn wait_originals_closed(&self, end: Instant) -> BoxFuture<'_, Result<(), ChannelError>> {
        Box::pin(async move {
            tokio::time::timeout_at(end, async {
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
            .map_err(|source| {
                failure(
                    ComponentId(770),
                    Stage::Close,
                    Reason::Timeout,
                    "same declared original remains open",
                )
                .with_source(source)
            })
        })
    }
}
