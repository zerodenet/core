//! All public diagnostic entry points share the actual finite Service owner.
use super::*;
impl Proxy {
    pub(super) async fn with_diagnostic_service<T: Send + 'static>(
        &self,
        work: impl Future<Output = T> + Send + 'static,
    ) -> Result<T, EngineError> {
        let snapshot = self.engine().runtime_snapshot();
        if self
            .protocols
            .current_components(snapshot.config())
            .is_some()
        {
            return Ok(work.await);
        }
        let proxy = self.clone();
        // The result is nested so a native lifecycle error retains its source;
        // it is not flattened into the task owner's acknowledgement string.
        self.apply_owner
            .run(self.reload_apply_lock.clone(), async move {
                let config = proxy.engine().runtime_snapshot().config().clone();
                let result = proxy
                    .protocols
                    .maintain_service_components(
                        config,
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        || work,
                    )
                    .await
                    .map_err(|source| EngineError::Io(std::io::Error::other(source)));
                Ok(result)
            })
            .await
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))?
    }
}
