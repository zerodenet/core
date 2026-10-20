//! Dependency-ordered start and confirmed retirement of the actual owners.
use super::*;

impl std::fmt::Debug for ComponentPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlan")
            .field("outbound_count", &self.outbounds.len())
            .finish()
    }
}
impl ComponentPlan {
    pub(crate) async fn start(&self, deadline: tokio::time::Instant) -> Result<(), ChannelError> {
        for slot in &self.order {
            slot.start(deadline).await?;
        }
        self.started.store(true, Ordering::Release);
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
        let retiring: Vec<_> = self
            .order
            .iter()
            .rev()
            .filter(|slot| {
                !keep.is_some_and(|plan| plan.order.iter().any(|active| Arc::ptr_eq(slot, active)))
            })
            .collect();
        for slot in &retiring {
            slot.revoke();
        }
        let mut failure = None;
        for slot in retiring {
            let result = slot.shutdown(deadline).await.and_then(|report| {
                if report.is_complete() {
                    Ok(())
                } else {
                    Err(invalid(
                        Reason::Timeout,
                        "component cleanup remains unfinished",
                    ))
                }
            });
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
