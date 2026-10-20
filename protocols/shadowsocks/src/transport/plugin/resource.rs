//! One actual SIP003 child shared by prepared leaves and its explicit owner.
use super::outbound::PluginPlan;
use std::{io, net::SocketAddr, process::ExitStatus, time::Duration};
use tokio::time::{sleep_until, timeout_at, Instant};

/// A confirmed child outcome. Requesting termination does not produce this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginTermination {
    NotStarted,
    Exited(ExitStatus),
}

/// Protocol-level ownership of the same child used by existing prepared leaves.
/// Shutdown is terminal; a host must confirm it before replacing this resource.
#[derive(Clone)]
pub struct ShadowsocksPluginResource {
    plan: PluginPlan,
}
impl std::fmt::Debug for ShadowsocksPluginResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShadowsocksPluginResource")
            .finish_non_exhaustive()
    }
}
impl ShadowsocksPluginResource {
    pub(crate) fn new(plan: PluginPlan) -> Self {
        Self { plan }
    }
    pub(crate) fn program(&self) -> std::path::PathBuf {
        self.plan.program()
    }
    pub(crate) fn identity(&self) -> String {
        self.plan.cache_identity()
    }
    pub(crate) fn revoke(&self) {
        self.plan.cancel.cancel();
    }
    pub(crate) async fn start_authorized(
        &self,
        resources: std::sync::Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
        claimed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> io::Result<SocketAddr> {
        self.plan
            .acquire_authorized(self.plan.supports_tcp(), resources, claimed)
            .await?
            .map(|lease| lease.endpoint())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "plugin has no enabled transport",
                )
            })
    }
    pub(crate) async fn wait_exit(&self) -> io::Result<ExitStatus> {
        let lease = self
            .plan
            .running
            .lock()
            .await
            .clone()
            .ok_or_else(|| io::Error::other("plugin child was not started"))?;
        loop {
            if let Some(status) = lease.child.lock().unwrap().try_wait()? {
                return Ok(status);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    pub async fn start(&self) -> io::Result<SocketAddr> {
        self.start_with_probe(tokio::net::TcpStream::connect).await
    }
    async fn start_with_probe<Probe, Pending>(&self, probe: Probe) -> io::Result<SocketAddr>
    where
        Probe: FnMut(SocketAddr) -> Pending + Send,
        Pending: std::future::Future<Output = io::Result<tokio::net::TcpStream>> + Send,
    {
        self.plan
            .acquire_with_probe(self.plan.supports_tcp(), probe)
            .await?
            .map(|lease| lease.endpoint())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    "plugin has no enabled transport",
                )
            })
    }

    /// Revoke admission, request termination and reap the actual child.
    /// Timeout or caller cancellation retains its handle for a later retry.
    pub async fn shutdown(&self, deadline: Instant) -> io::Result<PluginTermination> {
        self.plan.cancel.cancel();
        let slot = timeout_at(deadline, self.plan.running.lock())
            .await
            .map_err(|_| timed_out())?;
        let Some(lease) = slot.as_ref() else {
            return Ok(PluginTermination::NotStarted);
        };
        {
            let mut child = lease.child.lock().unwrap();
            if let Some(status) = child.try_wait()? {
                return Ok(PluginTermination::Exited(status));
            }
            child.start_kill()?;
        }
        loop {
            if let Some(status) = lease.child.lock().unwrap().try_wait()? {
                return Ok(PluginTermination::Exited(status));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(timed_out());
            }
            sleep_until(deadline.min(now + Duration::from_millis(10))).await;
        }
    }
}
fn timed_out() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "shadowsocks plugin exit is not confirmed",
    )
}

#[cfg(all(test, unix))]
#[path = "../../../tests/plugin/resource.rs"]
mod tests;
