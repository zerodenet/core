//! Explicit cold Service maintenance; no Business publication or implicit build I/O.
use super::*;
use std::future::Future;
use tokio::time::{Duration, Instant};
impl ComponentCatalog {
    pub(crate) async fn maintain_service<T, F, Fut>(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: Instant,
        work: F,
    ) -> Result<T, ChannelError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        self.drive_supervised(self.maintain_service_inner(config, deadline, work))
            .await
    }
    async fn maintain_service_inner<T, F, Fut>(
        &self,
        config: Arc<RuntimeConfig>,
        deadline: Instant,
        work: F,
    ) -> Result<T, ChannelError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        // Startup/replacement cannot reuse this exact temporary generation
        // until its actual cleanup finishes. The accepted host task owns this
        // guard, not a disconnected diagnostic client's request future.
        let transition = tokio::time::timeout_at(deadline, self.transition.lock())
            .await
            .map_err(|_| invalid(Reason::Timeout, "Service maintenance transition is busy"))?;
        self.finish_maintenance(deadline).await?;
        let existing = {
            let state = self.state.lock().expect("component catalog poisoned");
            if state
                .replacement
                .as_ref()
                .is_some_and(|pending| !pending.released())
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "runtime replacement retirement remains owned",
                ));
            }
            state
                .current
                .iter()
                .chain(state.staged.iter())
                .next()
                .map(|entry| {
                    (
                        entry.matches(&config),
                        entry.ready().started.load(Ordering::Acquire),
                    )
                })
        };
        if let Some((matches, started)) = existing {
            if !matches || !started {
                return Err(invalid(
                    Reason::StalePlan,
                    "runtime owns a different or unstarted component generation",
                ));
            }
            // Runtime-owned generations have their own persistent shutdown
            // owner. A diagnostic must never close those shared components.
            drop(transition);
            return tokio::select! { biased;
                failure = self.next_failure() => Err(failure),
                result = work() => Ok(result),
            };
        }
        let plan = self
            .prepare(config.clone())
            .map_err(super::super::instance::retained)?;
        {
            let mut state = self.state.lock().expect("component catalog poisoned");
            state.staged = Some(Current {
                config,
                plan: Ok(plan.clone()),
            });
            state.cold = None;
            state.maintenance = true;
            self.admission.stage(
                plan.startable_before_devices()
                    .map(|slot| slot.admission_grant()),
            );
        }
        let started = plan.start(deadline).await;
        let result = match started {
            Ok(()) => tokio::select! { biased;
                failure = self.next_failure() => Err(failure),
                result = work() => Ok(result),
            },
            Err(source) => Err(source),
        };
        // This confirms declared component resources/tasks only. It does not
        // invent native closure receipts for transferred legacy Stream I/O.
        self.finish_maintenance(Instant::now() + Duration::from_secs(5))
            .await?;
        result
    }
    pub(super) async fn finish_maintenance(&self, deadline: Instant) -> Result<(), ChannelError> {
        let pending = {
            let state = self.state.lock().expect("component catalog poisoned");
            state.maintenance.then(|| {
                state
                    .staged
                    .as_ref()
                    .expect("retained maintenance plan")
                    .ready()
                    .clone()
            })
        };
        let Some(plan) = pending else {
            return Ok(());
        };
        plan.shutdown(deadline).await?;
        let mut state = self.state.lock().expect("component catalog poisoned");
        if !state
            .staged
            .as_ref()
            .is_some_and(|entry| Arc::ptr_eq(entry.ready(), &plan))
        {
            return Err(invalid(
                Reason::StalePlan,
                "Service maintenance ownership changed",
            ));
        }
        self.admission.stage(std::iter::empty());
        state.staged = None;
        state.cold = None;
        state.maintenance = false;
        state.prune_issued();
        Ok(())
    }
}
