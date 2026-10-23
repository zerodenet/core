//! Task joins are separate from the carrier's original I/O close evidence.
use zero_engine::EngineError;

impl super::SharedRawIpDevice {
    /// Consume actual driver and client TCP handles; preserve their original
    /// failure in this device's persistent owner for subsequent cleanup retries.
    /// This does not certify closure of the carrier's native I/O.
    pub(crate) async fn wait_workers_stopped(&self) -> Result<(), EngineError> {
        self.tcp.shutdown().await;
        let workers = self.task.join().await.map(|_| ());
        let carrier = match &self.carrier {
            None => Ok(()), // Endpoint stacks have no separate wire carrier.
            Some(carrier) => match carrier.stop_background() {
                Some(joined) => joined.await,
                None => Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "raw-IP carrier background task owner is not migrated",
                ))),
            },
        };
        match (workers, carrier) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(driver), Err(carrier)) => {
                Err(EngineError::Io(std::io::Error::other(WorkerFailures {
                    driver,
                    carrier,
                })))
            }
        }
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

#[derive(Debug)]
struct WorkerFailures {
    driver: EngineError,
    carrier: EngineError,
}
impl std::fmt::Display for WorkerFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "raw-IP original workers failed: {}; outer workers: {}",
            self.driver, self.carrier
        )
    }
}
impl std::error::Error for WorkerFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.driver)
    }
}
