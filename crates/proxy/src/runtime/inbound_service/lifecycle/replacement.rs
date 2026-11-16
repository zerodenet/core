//! Cold Main quota reservations precede consumer retirement, without I/O or
//! polling a candidate factory. The same persistent Service owner retains them.
use super::*;
impl InboundServices {
    pub(crate) async fn prepare_replacement(
        &self,
        mains: usize,
        services: &TcpRuntimeServices,
    ) -> Result<(), EngineError> {
        let mut owner = self.owner.lock().await;
        owner.retained_failure()?;
        let limit = NonZeroUsize::new(
            services
                .config()
                .runtime
                .effective_component_total_max_tasks(),
        )
        .ok_or_else(|| std::io::Error::other("Service task ceiling is zero"))?;
        if owner
            .cold
            .as_ref()
            .is_some_and(|(_, launches, planned)| *planned == limit && launches.len() == mains)
        {
            return Ok(());
        }
        owner.cold.take();
        if mains != 0 {
            let (scope, launches) = State::prepare(limit, services.service_task_budget(), mains)?;
            owner.cold = Some((scope, launches, limit));
        }
        Ok(())
    }
    pub(crate) async fn prepare_restoration(
        &self,
        services: &TcpRuntimeServices,
    ) -> Result<(), EngineError> {
        let count = self.owner.lock().await.previous.len();
        self.prepare_replacement(count, services).await
    }
    pub(crate) async fn discard_prepared(&self) {
        self.owner.lock().await.cold.take();
    }
    /// Must precede any donor/network/DNS StopOld. Incomplete real custody
    /// blocks the transaction; shutdown never authorizes a replacement alone.
    pub(crate) async fn suspend_for_replacement(&mut self) -> Result<(), EngineError> {
        let mut owner = self.owner.lock().await;
        if owner.scope.is_none() && owner.retained.is_empty() {
            return owner.retained_failure();
        }
        owner.suspended = true;
        match owner
            .close(Instant::now() + std::time::Duration::from_secs(5))
            .await
        {
            Ok(()) => Ok(()),
            Err(source) => Err(owner.fail_transition(source)),
        }
    }
    pub(crate) async fn restore(
        &mut self,
        services: TcpRuntimeServices,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), EngineError> {
        let prepared = {
            let owner = self.owner.lock().await;
            if !owner.suspended {
                return Ok(());
            }
            owner.retained_failure()?;
            owner.previous.iter().cloned().collect()
        };
        self.replace(prepared, services, shutdown).await?;
        self.accept().await;
        Ok(())
    }
    pub(crate) async fn accept(&self) {
        let mut owner = self.owner.lock().await;
        if let Some(staged) = owner.staged.take() {
            owner.previous = staged;
        }
        owner.suspended = false;
    }
}
