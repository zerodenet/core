//! Task joins are separate from the carrier's original I/O close evidence.
use zero_engine::EngineError;

impl super::SharedRawIpDevice {
    /// Consume actual driver and client TCP handles; preserve their original
    /// failure in this device's persistent owner for subsequent cleanup retries.
    /// This does not certify closure of the carrier's native I/O.
    pub(crate) async fn wait_workers_stopped(&self) -> Result<(), EngineError> {
        self.tcp.shutdown().await;
        self.task.join().await.map(|_| ())
    }

    /// Compatibility for the existing infallible lifecycle completion surface.
    /// Strict replacement must use the fallible worker result and independent
    /// native close evidence, rather than treating this waiter as a close ACK.
    pub(crate) async fn wait_stopped(&self) {
        if let Err(error) = self.wait_workers_stopped().await {
            tracing::warn!(%error, "raw-IP original workers joined with a retained failure");
        }
    }
}
