//! Classify the original observer outcome without another owner or task.
use super::*;

pub(crate) enum ObserverResult {
    Listener(Result<(), EngineError>),
    SystemTun(Result<(), EngineError>),
}
impl ObserverResult {
    fn into_result(self) -> Result<(), EngineError> {
        match self {
            Self::Listener(result) | Self::SystemTun(result) => result,
        }
    }
}
impl ListenerObservers {
    pub(crate) fn spawn<F>(&mut self, work: F) -> tokio::task::AbortHandle
    where
        F: std::future::Future<Output = Result<(), EngineError>> + Send + 'static,
    {
        self.owned
            .as_mut()
            .expect("live observer ownership lease")
            .tasks
            .spawn(async move { ObserverResult::Listener(work.await) })
    }
    /// The steady orchestration waits even without configured listeners, so a
    /// dynamic source joins through this original consumer while it is active.
    pub(crate) async fn join_next_running(
        &mut self,
    ) -> Option<Result<Result<(), EngineError>, tokio::task::JoinError>> {
        loop {
            let parking = self.parking.clone();
            let changed = parking.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.transfer_system_tun();
            if self.owned.as_ref().expect("live lease").tasks.is_empty() {
                changed.await;
                continue;
            }
            tokio::select! {
                result = self.next_original() => match result {
                    // The original observer accepts Ok only after its actual
                    // driver was explicitly stopping and its Scope joined.
                    // Native/device closure remains the TUN owner's own debt.
                    Some(Ok((_, ObserverResult::SystemTun(Ok(()))))) => continue,
                    result => return result.map(|result| result.map(|(_, value)| value.into_result())),
                },
                _ = &mut changed => {},
            }
        }
    }
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), EngineError>, tokio::task::JoinError>> {
        self.join_next_with_id()
            .await
            .map(|result| result.map(|(_, value)| value))
    }
    pub(crate) async fn join_next_with_id(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, Result<(), EngineError>), tokio::task::JoinError>> {
        self.next_original()
            .await
            .map(|result| result.map(|(id, value)| (id, value.into_result())))
    }
    async fn next_original(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, ObserverResult), tokio::task::JoinError>> {
        self.transfer_system_tun();
        let target = Arc::new(WaitTarget::default());
        let batch = self.owned.as_mut().expect("live observer ownership lease");
        let relay = batch.wake.clone();
        std::future::poll_fn(|cx| {
            // Only this borrowed join future owns the actual caller Waker.
            // The persistent original JoinSet sees a weak-target relay only.
            target.register(cx);
            relay.bind(&target);
            let deferred = relay.enter();
            let waker = std::task::Waker::from(relay.clone());
            let mut inner = std::task::Context::from_waker(&waker);
            let result = batch.tasks.poll_join_next_with_id(&mut inner);
            // Tokio can budget-yield or replace/drop its stored Waker while
            // polling. Deliver external callbacks only after that call returns.
            drop(deferred);
            result
        })
        .await
    }
}
