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

    /// The original device workers and its independent original I/O provider
    /// must both finish. A retained task failure is never converted to success.
    pub(crate) async fn wait_closed(&self) -> Result<(), EngineError> {
        self.close_now();
        let workers = self.wait_workers_stopped().await;
        let native = if let Some(carrier) = &self.carrier {
            carrier.close_native().await
        } else {
            Ok(())
        };
        workers.and(native)
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
