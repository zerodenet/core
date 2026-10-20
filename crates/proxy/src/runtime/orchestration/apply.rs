//! Keep servicing an accepted transaction's reconcile/compensation acknowledgements.
use super::state::OrchestrationState;
use crate::runtime::Proxy;
use tokio::time::{Duration, Instant};
use zero_engine::EngineError;

impl OrchestrationState {
    pub(super) async fn drain_configuration(&mut self, proxy: &Proxy) -> Result<(), EngineError> {
        proxy.apply_owner.begin_shutdown();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            tokio::select! { biased;
                result = proxy.apply_owner.wait_idle() => {
                    result.map_err(|message| EngineError::Io(std::io::Error::other(message)))?;
                    return proxy.apply_owner.shutdown(deadline).await
                        .map_err(|message| EngineError::Io(std::io::Error::other(message)));
                }
                Some(()) = self.reload_async_rx.recv() => self.reconcile_reload(proxy).await,
                _ = tokio::time::sleep_until(deadline) => return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "configuration transaction shutdown remains unconfirmed",
                ).into()),
            }
        }
    }
}
