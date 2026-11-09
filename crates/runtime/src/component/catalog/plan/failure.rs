use super::*;
use zero_platform_tokio::contract::lifecycle::TaskFailure;
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::component::catalog) enum FailureKey {
    Task(zero_core::component::TaskId),
    Resource(
        zero_core::component::ComponentInstanceId,
        zero_platform_tokio::contract::lifecycle::ResourceFailurePhase,
    ),
}
#[derive(Debug)]
enum FailureKind {
    Task(TaskFailure),
    Resource(Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>),
}
#[derive(Debug)]
pub(in crate::component::catalog) struct PlanFailure {
    kind: FailureKind,
    instance: zero_core::component::ComponentInstanceId,
    reported: AtomicBool,
}
impl std::fmt::Display for PlanFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            FailureKind::Task(task) => write!(
                f,
                "component task {} ({:?}) failed: {}",
                task.id.0, task.class, task.error
            ),
            FailureKind::Resource(resource) => write!(
                f,
                "component {:?} {:?} failed: {}",
                resource.instance, resource.controller, resource.source
            ),
        }
    }
}
impl std::error::Error for PlanFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.original())
    }
}
impl PlanFailure {
    pub(in crate::component::catalog) fn new(
        failure: TaskFailure,
        instance: zero_core::component::ComponentInstanceId,
    ) -> Self {
        Self {
            kind: FailureKind::Task(failure),
            instance,
            reported: false.into(),
        }
    }
    pub(in crate::component::catalog) fn resource(
        failure: Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>,
    ) -> Self {
        Self {
            instance: failure.instance,
            kind: FailureKind::Resource(failure),
            reported: false.into(),
        }
    }
    pub(in crate::component::catalog) fn instance(
        &self,
    ) -> zero_core::component::ComponentInstanceId {
        self.instance
    }
    #[cfg(test)]
    pub(in crate::component::catalog) fn task(&self) -> Option<&TaskFailure> {
        match &self.kind {
            FailureKind::Task(task) => Some(task),
            _ => None,
        }
    }
    pub(in crate::component::catalog) fn key(&self) -> FailureKey {
        match &self.kind {
            FailureKind::Task(failure) => FailureKey::Task(failure.id),
            FailureKind::Resource(failure) => {
                FailureKey::Resource(failure.instance, failure.controller)
            }
        }
    }
    pub(in crate::component::catalog) fn original(&self) -> &ChannelError {
        match &self.kind {
            FailureKind::Task(f) => &f.error,
            FailureKind::Resource(f) => &f.source,
        }
    }
    pub(in crate::component::catalog) fn reported(&self) -> bool {
        match &self.kind {
            FailureKind::Resource(f) => f.reported(),
            _ => self.reported.load(Ordering::Acquire),
        }
    }
    /// The existing plan archive must own this exact RF, not merely another
    /// error with the same controller key or cleanup receipt, before accepting
    /// the original protocol ledger. Publication remains a separate handoff.
    pub(in crate::component::catalog) fn receive_resource_ownership(
        &self,
        received: &Arc<zero_platform_tokio::contract::lifecycle::ResourceFailure>,
    ) -> bool {
        match &self.kind {
            FailureKind::Resource(actual) if Arc::ptr_eq(actual, received) => {
                actual.acknowledge_retained(received)
            }
            _ => false,
        }
    }
    pub(in crate::component::catalog) fn confirm_reported(&self) {
        self.reported.store(true, Ordering::Release);
        if let FailureKind::Resource(f) = &self.kind {
            f.confirm_reported();
        }
    }
    pub(in crate::component::catalog) fn retained(self: &Arc<Self>) -> ChannelError {
        let original = self.original();
        let mut error = ChannelError::new(original.stage, original.reason, original.impact);
        error.component = original.component;
        error.session = original.session;
        error.delivery = original.delivery;
        error.retry = original.retry;
        if let Some(receipt) = original.cleanup_failure_receipt() {
            error = error.with_cleanup_failure_receipt(receipt.clone());
        }
        error.with_source(self.clone())
    }
}
