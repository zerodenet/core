//! The actual one status sampler is installed cold in ApplicationServices.
//! Its scope owns the sole worker; no detached monitor or cleanup task exists.
use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};
use tokio::sync::Mutex;
use zero_platform_tokio::contract::{
    scope::{ScopeRetirementObservation, TaskClass, TaskJoinWitness, TaskScope},
    CancellationToken, ChannelError,
};

pub(in crate::application::services) struct StatusMonitor {
    scope: Mutex<TaskScope>,
    stop: CancellationToken,
    observation: ScopeRetirementObservation,
    worker: OnceLock<TaskJoinWitness>,
    result: std::sync::Mutex<Option<Result<(), Arc<ChannelError>>>>,
}
impl StatusMonitor {
    pub(in crate::application::services) fn cold() -> Arc<Self> {
        let scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let observation = scope.registrar().retirement_observation();
        Arc::new(Self {
            scope: Mutex::new(scope),
            stop: CancellationToken::new(),
            observation,
            worker: OnceLock::new(),
            result: std::sync::Mutex::new(None),
        })
    }
    pub(in crate::application::services) fn start(
        &self,
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        // The caller already installed this exact owner before starting it.
        let scope = self.scope.try_lock().map_err(std::io::Error::other)?;
        if self.worker.get().is_some() {
            return Err(std::io::Error::other("same sampler already registered"));
        }
        let stop = self.stop.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                tokio::select! { biased; _ = stop.cancelled() => {}, _ = work => {} }
                Ok(())
            })
            .map_err(std::io::Error::other)?;
        assert!(
            self.worker.set(witness).is_ok(),
            "one actual sampler per application owner"
        );
        Ok(())
    }
    #[cfg(test)]
    pub(in crate::application::services) fn id(&self) -> zero_core::component::TaskId {
        self.worker.get().unwrap().task_id().unwrap()
    }
    fn retain_join(&self) -> Result<(), Arc<ChannelError>> {
        let Some(joined) = self.worker.get().and_then(TaskJoinWitness::try_joined) else {
            return Ok(()); // cold/no worker is not retirement; observation still gates it
        };
        let mut result = self.result.lock().unwrap();
        result.get_or_insert_with(|| joined.result()).clone()
    }
    pub(in crate::application::services) async fn wait_terminated(&self) -> std::io::Result<()> {
        let mut scope = self.scope.lock().await;
        if self.result.lock().unwrap().is_none() {
            let _ = scope.next_exit().await;
        }
        self.retain_join().map_err(std::io::Error::other)
    }
    pub(in crate::application::services) async fn shutdown(&self) -> Result<(), Arc<ChannelError>> {
        self.stop.cancel(); // business stops now; same original scope retains joins
        let mut scope = self.scope.lock().await;
        let report = scope.shutdown(crate::ipc::owner::close_deadline()).await;
        let outcome = self.retain_join();
        if !report.is_complete() || !self.observation.is_drained() {
            return Err(Arc::new(
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Close,
                    zero_platform_tokio::contract::Reason::Timeout,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(std::io::Error::other(format!(
                    "same status sampler scope remains retained: {report:?}"
                ))),
            ));
        }
        outcome
    }
    pub(in crate::application::services) fn retirement_complete(&self) -> bool {
        self.observation.is_drained()
            && self.worker.get().is_none_or(|worker| {
                worker
                    .try_joined()
                    .is_some_and(|joined| joined.registration_released())
            })
    }
    pub(super) fn is_stopping(&self) -> bool {
        self.stop.is_cancelled()
    }
    #[cfg(test)]
    pub(in crate::application::services) fn test_work(
        work: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Arc<Self> {
        let owner = Self::cold();
        owner.start(work).unwrap();
        owner
    }
}

#[cfg(test)]
mod tests;
