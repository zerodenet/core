//! One staged original IO future, followed by its actual same-Scope driver.
use super::*;
use zero_platform_tokio::{
    contract::scope::TaskJoined,
    controlled_carrier::{CarrierPreparationJob, StagedCarrierPreparationJob},
};
impl Opener {
    fn check_job_call(
        &self,
        binding: &UpstreamBinding,
        call: &CarrierContext,
    ) -> Result<(), ChannelError> {
        if binding != self.upstream.binding()
            || call.session != self.preparation_call.session
            || call.deadline != self.preparation_call.deadline
            || call.cancel.is_cancelled()
            || self.preparation_call.cancel.is_cancelled()
            || self.transfer.stop.is_cancelled()
            || self
                .transfer
                .published
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "actual preparation job call changed",
            ));
        }
        Ok(())
    }
    pub(super) fn reserve_job(
        &self,
        binding: &UpstreamBinding,
        call: &CarrierContext,
    ) -> Result<CarrierPreparationJob, ChannelError> {
        self.check_job_call(binding, call)?;
        let tasks = self.tasks.as_ref().ok_or_else(|| {
            crate::component::instance::invalid(
                Reason::Unsupported,
                "fixed library owns its explicit executor",
            )
        })?;
        let preparation = self.transfer.preparation.as_ref().ok_or_else(|| {
            crate::component::instance::invalid(
                Reason::StalePlan,
                "actual preparation is not installed",
            )
        })?;
        let _ = tasks;
        preparation.reserve_job()
    }
}
impl Outbound {
    pub(super) async fn handoff_rejected_job(
        &self,
        transfer: &Arc<Transfer>,
        context: &OpenContext,
        rejected: Arc<ChannelError>,
        received: &TaskJoined,
    ) -> Result<Option<std::time::Duration>, ChannelError> {
        let tasks = self.tasks.as_ref().ok_or_else(|| {
            crate::component::instance::invalid(
                Reason::StalePlan,
                "ordinary job has no actual Scope",
            )
        })?;
        let pending = transfer.adoption.take_preparation_job(tasks, received)?;
        let Some(pending) = pending else {
            return Ok(None);
        };
        if context.cancel.is_cancelled()
            || transfer.stop.is_cancelled()
            || context.deadline.is_some_and(|end| Instant::now() >= end)
        {
            // The actual future still owns this original IO. Its Drop returns
            // that view; it never marks native or registration closure.
            drop(pending);
            return Ok(None);
        }
        let tasks = self.tasks.as_ref().ok_or_else(|| {
            crate::component::instance::invalid(
                Reason::StalePlan,
                "ordinary job has no actual Scope",
            )
        })?;
        let key = pending.grant.clone();
        let original = rejected.clone();
        let installing = (|| {
            pending.grant.accept_rejection(rejected, received)?;
            let driver = transfer
                .driver_input
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| {
                    crate::component::instance::invalid(
                        Reason::StalePlan,
                        "original preparation driver input already consumed",
                    )
                })?;
            let StagedCarrierPreparationJob {
                grant,
                return_delay,
                work,
            } = pending;
            let retained = grant.clone();
            transfer.adoption.spawn_preparation_job(
                tasks,
                &grant,
                driver,
                move |_| async move {
                    // Actual job owns the grant until its same original IO future
                    // returns. Requests consume the same Root through its grant clone.
                    let _retained = retained;
                    work.await
                },
            )?;
            Ok(Some(return_delay))
        })();
        installing.map_err(|install| key.retain_failed_install(original, install))
    }
}
