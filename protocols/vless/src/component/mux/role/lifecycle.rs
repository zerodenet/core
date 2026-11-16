//! One retained owner for the same component's MUX and ordinary UDP roles.
use super::*;
use zero_platform_tokio::contract::lifecycle::CleanupFailurePhase;
impl ResourceLifecycle for Owner {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        if let Some(ordinary) = &mut self.ordinary {
            ordinary.start(deadline).await?;
        }
        let mut bound = self
            .options
            .prepared
            .leaf
            .bind_native_mux(self.pool)
            .map_err(|source| {
                self.admission
                    .error(
                        None,
                        Stage::Prepare,
                        Reason::Revoked,
                        DeliveryState::NotSubmitted,
                    )
                    .with_source(source)
            })?;
        if let Some(masks) = &self.masks {
            bound = bound.with_native_tcp_masks(masks.clone());
        }
        *self.ready.leaf.lock().unwrap() = Some(bound);
        self.inner.start(deadline).await
    }
    fn revoke(&self, reason: CloseReason) {
        self.inner.revoke(reason);
        if let Some(ordinary) = &self.ordinary {
            ordinary.revoke(reason);
        }
        self.ready.leaf.lock().unwrap().take();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
        self.revoke(CloseReason::Shutdown);
        let mut report = CloseReport::default();
        if let Some(ordinary) = &mut self.ordinary {
            match ordinary.shutdown(deadline).await {
                Ok(actual) => report.append(actual),
                Err(source) => {
                    self.failures
                        .receive(Arc::new(source), CleanupFailurePhase::Shutdown);
                }
            }
        }
        match self.inner.shutdown(deadline).await {
            Ok(actual) => report.append(actual),
            Err(source) => {
                self.failures
                    .receive(Arc::new(source), CleanupFailurePhase::Driver);
            }
        }
        if let Some(failure) = self.failures.report() {
            return Err(ChannelError::retained(failure));
        }
        Ok(report)
    }
}
