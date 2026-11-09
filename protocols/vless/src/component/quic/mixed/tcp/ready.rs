//! Setup receipt/Host ACK precede the actual HTTP driver and cached receiver.
use super::*;
use zero_platform_tokio::contract::context::with_deadline;
impl PendingTcpOriginal {
    pub async fn ready(
        mut self,
    ) -> Result<zero_transport::split_http::OwnedXhttpTcp, ChannelError> {
        let context = self.context.clone();
        let cancelled = self.generation.cancel.clone();
        tokio::select! { biased;
            _=context.cancel.cancelled()=>Err(error(Stage::Prepare, Reason::Cancelled)),
            _=cancelled.cancelled()=>Err(error(Stage::Prepare, Reason::Revoked)),
            result=with_deadline(context.deadline, self.receive())=>result
                .map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))?,
        }
    }
    async fn receive(&mut self) -> Result<zero_transport::split_http::OwnedXhttpTcp, ChannelError> {
        self.entry.owners.receive_actor_original().await?;
        let joined = self.caller.wait_registration_released().await;
        if !self.caller.matches_joined(&joined) {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        let prepared = { self.entry.owners.state.lock().await.tcp_setup.take() };
        let Some(setup) = prepared else {
            return match joined.result() {
                Err(source) => Err(ChannelError::retained(source)),
                Ok(()) => Err(error(Stage::Prepare, Reason::Cancelled)),
            };
        };
        match setup.outcome {
            setup::Outcome::Ready(input) => {
                let prepared = self
                    .entry
                    .owners
                    .state
                    .lock()
                    .await
                    .tcp
                    .clone()
                    .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
                let mut pending = prepared.start(
                    input,
                    &setup.adoption,
                    &self.generation.tasks,
                    self.entry.retirement.clone(),
                )?;
                let driver = pending.witness().clone();
                // Keep the actual driver before any readiness await or refusal.
                {
                    self.entry.owners.state.lock().await.tcp_driver = Some(driver.clone());
                }
                self.entry
                    .origin
                    .set(pending.origin().clone())
                    .map_err(|_| error(Stage::Prepare, Reason::StalePlan))?;
                setup.preparation.publish_driver(&driver)?;
                let client = pending.ready().await?;
                setup
                    .adoption
                    .prepare_stream_protocol_receiver(
                        &self.generation.tasks,
                        &self.entry.retirement,
                    )?
                    .confirm()?;
                self.pending.0.take();
                Ok(client)
            }
            setup::Outcome::Rejected {
                source,
                input,
                staged_job,
            } => {
                if staged_job {
                    self.handoff_job(setup.adoption, input, &joined).await?;
                }
                Err(ChannelError::retained(source))
            }
        }
    }
    async fn handoff_job(
        &mut self,
        adoption: CarrierAdoption,
        input: Option<
            zero_platform_tokio::controlled_carrier::CarrierDriver<
                zero_platform_tokio::controlled_carrier::CarrierUserView,
            >,
        >,
        joined: &zero_platform_tokio::contract::scope::TaskJoined,
    ) -> Result<(), ChannelError> {
        if self.context.cancel.is_cancelled()
            || self.generation.cancel.is_cancelled()
            || self.entry.retirement.retirement_requested()
        {
            return Err(error(Stage::Prepare, Reason::Cancelled));
        }
        if self
            .context
            .deadline
            .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            return Err(error(Stage::Prepare, Reason::Timeout));
        }
        let rejected = joined
            .result()
            .err()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let staged = adoption
            .take_preparation_job(&self.generation.tasks, joined)?
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let key = staged.grant.clone();
        let result = (|| {
            staged.grant.accept_rejection(rejected.clone(), joined)?;
            let input = input.ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
            let retained = staged.grant.clone();
            let work = staged.work;
            let (_, driver) = adoption.spawn_preparation_job(
                &self.generation.tasks,
                &staged.grant,
                input,
                move |view| async move {
                    let _view = view;
                    let _grant = retained;
                    work.await
                },
            )?;
            Ok((driver, staged.return_delay))
        })();
        let (driver, delay) =
            result.map_err(|source| key.retain_failed_install(rejected, source))?;
        self.entry.owners.state.lock().await.tcp_driver = Some(driver);
        // This actual sealed job, not its rejected first Business, now owns IO.
        // Its existing Host/Scope/60s job and same Root drive final retirement.
        self.pending.0.take();
        tokio::time::sleep(delay).await;
        Ok(())
    }
}
