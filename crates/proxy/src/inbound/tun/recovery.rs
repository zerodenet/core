use std::io;
use std::time::Duration;

use zero_engine::EngineError;

use crate::runtime::Proxy;

impl Proxy {
    /// Reconcile the running TUN against the current network without replacing
    /// its device or the proxy. Completion acknowledges the route audit itself.
    pub async fn recover_tun(&self) -> Result<(), EngineError> {
        let sender = {
            let _operation = self.tun_operation_lock.lock().await;
            self.tun_control
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|control| control.route_recovery.clone())
                .ok_or_else(|| {
                    EngineError::Io(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "TUN automatic routes are not running",
                    ))
                })?
        };
        // Do not hold the lifecycle lock while waiting: an explicit stop must
        // be able to interrupt recovery and clean up immediately.
        let (reply, completion) = tokio::sync::oneshot::channel();
        tokio::time::timeout(Duration::from_secs(15), async {
            sender.send(reply).await.map_err(|_| {
                io::Error::new(io::ErrorKind::BrokenPipe, "TUN route runtime stopped")
            })?;
            completion
                .await
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "TUN route recovery was interrupted",
                    )
                })?
                .map_err(io::Error::other)
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "TUN route recovery timed out; automatic recovery continues",
            )
        })??;
        Ok(())
    }
}
