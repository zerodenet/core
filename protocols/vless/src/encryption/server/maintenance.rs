//! Actual Source-owned cache maintenance. No I/O, pool or second task owner.
use super::*;
use std::sync::atomic::Ordering;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskJoinWitness, TaskRegistrar},
    CancellationToken, ChannelError, Impact, Reason, Stage,
};
mod library;
const COLD: u8 = 0;
const LIBRARY: u8 = 1;
const PREPARED: u8 = 2;
const STARTED: u8 = 3;
const STOPPED: u8 = 4;

pub(crate) struct HostedMaintenance {
    server: EncryptionServer,
    cancel: CancellationToken,
    original: Option<(TaskId, TaskJoinWitness)>,
    #[cfg(test)]
    sleeping: Arc<std::sync::atomic::AtomicBool>,
}
impl EncryptionServer {
    pub(crate) fn requires_maintenance(&self) -> bool {
        self.inner.config.resume
    }
    pub(crate) fn prepare_hosted(
        &self,
        cancel: CancellationToken,
    ) -> Result<HostedMaintenance, ChannelError> {
        self.inner
            .maintenance
            .compare_exchange(COLD, PREPARED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                failure(
                    Reason::StalePlan,
                    "decryption state already has its original execution owner",
                )
            })?;
        Ok(HostedMaintenance {
            server: self.clone(),
            cancel,
            original: None,
            #[cfg(test)]
            sleeping: Arc::default(),
        })
    }
    pub(super) fn check_hosted(&self) -> io::Result<()> {
        if self.inner.maintenance.load(Ordering::Acquire) == STARTED {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "original decryption Source is not running",
            ))
        }
    }
}
impl HostedMaintenance {
    pub(crate) fn start(&mut self, tasks: &TaskRegistrar) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() {
            return Err(failure(
                Reason::Revoked,
                "decryption Source stopped before start",
            ));
        }
        if self.server.inner.maintenance.load(Ordering::Acquire) == STARTED {
            if self.server.requires_maintenance() && self.original.is_none() {
                return Err(failure(
                    Reason::Internal,
                    "running cache lost its original task witness",
                ));
            }
            return Ok(());
        }
        self.server
            .inner
            .maintenance
            .compare_exchange(PREPARED, STARTED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                failure(
                    Reason::Revoked,
                    "original decryption maintenance cannot restart",
                )
            })?;
        if !self.server.requires_maintenance() {
            return Ok(());
        }
        // Capture the actual lease before registration/first polling. Even an
        // unpolled cancelled worker fences handshakes; Drop never claims join.
        let lease = Lease(Arc::downgrade(&self.server.inner));
        let cancel = self.cancel.clone();
        #[cfg(test)]
        let sleeping = self.sleeping.clone();
        let original = tasks
            .spawn_tracked(TaskClass::Critical, async move {
                let _lease = lease;
                loop {
                    let tick = tokio::time::sleep(Duration::from_secs(60));
                    tokio::pin!(tick);
                    let waiting = std::future::poll_fn(|cx| {
                        let polled = std::future::Future::poll(tick.as_mut(), cx);
                        #[cfg(test)]
                        sleeping.store(polled.is_pending(), Ordering::Release);
                        polled
                    });
                    tokio::select! { biased;
                        _ = cancel.cancelled() => return Ok(()),
                        _ = waiting => {
                            let Some(server) = _lease.0.upgrade() else { return Ok(()) };
                            server.cache.lock().unwrap_or_else(|e| e.into_inner()).prune();
                        }
                    }
                }
            })
            .map_err(|source| {
                failure(
                    Reason::Capacity,
                    "decryption maintenance was not registered",
                )
                .with_source(source)
            })?;
        self.original = Some(original);
        Ok(())
    }
    pub(crate) fn revoke(&self) {
        self.server
            .inner
            .maintenance
            .store(STOPPED, Ordering::Release);
        self.cancel.cancel();
    }
}
impl Drop for HostedMaintenance {
    fn drop(&mut self) {
        self.revoke();
    }
}
struct Lease(std::sync::Weak<Server>);
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(server) = self.0.upgrade() {
            server.maintenance.store(STOPPED, Ordering::Release);
        }
    }
}
fn failure(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(io::Error::other(message))
}
#[cfg(test)]
#[path = "../tests/maintenance.rs"]
mod tests;
