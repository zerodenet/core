//! Dependency-ordered start and confirmed retirement of the actual owners.
use super::*;
use zero_platform_tokio::contract::lifecycle::TaskFailure;

#[derive(Debug)]
pub(super) struct PlanTaskFailure(
    pub(super) TaskFailure,
    pub(super) zero_core::component::ComponentInstanceId,
    AtomicBool,
);
impl std::fmt::Display for PlanTaskFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "component task {} ({:?}) failed: {}",
            self.0.id.0, self.0.class, self.0.error
        )
    }
}
impl std::error::Error for PlanTaskFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0.error)
    }
}
impl PlanTaskFailure {
    pub(super) fn new(
        failure: TaskFailure,
        instance: zero_core::component::ComponentInstanceId,
    ) -> Self {
        Self(failure, instance, AtomicBool::new(false))
    }
    pub(super) fn reported(&self) -> bool {
        self.2.load(Ordering::Acquire)
    }
    pub(super) fn confirm_reported(&self) {
        let _ = self
            .2
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
    }
    pub(super) fn retained(self: &Arc<Self>) -> ChannelError {
        let original = &self.0.error;
        let mut error = ChannelError::new(original.stage, original.reason, original.impact);
        error.component = original.component;
        error.session = original.session;
        error.delivery = original.delivery;
        error.retry = original.retry;
        error.with_source(self.clone())
    }
}

impl std::fmt::Debug for ComponentPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlan")
            .field("outbound_count", &self.outbounds.len())
            .finish()
    }
}
impl ComponentPlan {
    pub(super) fn can_reuse(&self) -> bool {
        self.order.iter().all(|slot| slot.can_reuse())
    }

    pub(crate) async fn start(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        for slot in &self.order {
            slot.start(deadline).await?;
        }
        self.scan_tasks(deadline).await?;
        self.started.store(true, Ordering::Release);
        Ok(())
    }
    pub(super) async fn scan_tasks(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        for slot in &self.order {
            slot.scan_tasks(deadline).await?;
        }
        Ok(())
    }
    pub(crate) fn configured(&self, tag: &str) -> Option<Arc<ComponentSlot>> {
        self.outbounds.get(tag).cloned()
    }
    pub(crate) fn direct(&self, tag: Option<&str>) -> Result<Arc<ComponentSlot>, ChannelError> {
        match tag {
            None => Ok(self.builtin.clone()),
            Some(tag) => self.outbounds.get(tag).cloned().ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    format!("no prepared component for outbound `{tag}`"),
                )
            }),
        }
    }
    pub(crate) async fn shutdown(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        self.shutdown_except(deadline, None).await
    }
    pub(super) async fn shutdown_except(
        &self,
        deadline: tokio::time::Instant,
        keep: Option<&ComponentPlan>,
    ) -> Result<(), ChannelError> {
        let retiring = self
            .order
            .iter()
            .filter(|slot| {
                !keep.is_some_and(|plan| plan.order.iter().any(|active| Arc::ptr_eq(slot, active)))
            })
            .map(|slot| slot.instance_id())
            .collect();
        let declared = self.confirm_instances(&retiring, deadline).await;
        if let Some(failed) = self
            .failures
            .lock()
            .expect("component failures poisoned")
            .values()
            .next()
        {
            return Err(failed.retained());
        }
        declared
    }
}
