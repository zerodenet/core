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
    pub(crate) fn fresh_owner(&self) -> Self {
        Self {
            plan: self.plan.fresh_owner(),
        }
    }
    pub(crate) fn endpoint_spec(
        &self,
        generation: zero_core::component::NetworkGeneration,
    ) -> zero_platform_tokio::owned_process::ProcessEndpointSpec {
        self.plan.endpoint_spec(generation)
    }
    pub(crate) fn program(&self) -> std::path::PathBuf {
        self.plan.program()
    }
    pub(crate) fn identity(&self) -> String {
        self.plan.cache_identity()
    }
    pub(crate) fn revoke(&self) {
        self.plan.cancel.cancel();
        #[cfg(any(target_os = "linux", windows))]
        {
            let pending = self
                .plan
                .pending_endpoint
                .lock()
                .unwrap()
                .as_ref()
                .map(|opening| (opening.endpoint.clone(), opening.process.clone()));
            if let Some((endpoint, process)) = pending {
                endpoint.request_stop();
                if let Some(process) = process {
                    process.request_stop();
                }
            }
        }
        let running = self
            .plan
            .running
            .try_lock()
            .ok()
            .and_then(|running| running.clone());
        if let Some(running) = running {
            let _ = running.child.lock().unwrap().start_kill();
            #[cfg(any(target_os = "linux", windows))]
            if let Some(endpoint) = &running.process_endpoint {
                endpoint.request_stop();
            }
        }
    }
    pub(crate) async fn start_authorized(
        &self,
        resources: std::sync::Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
        claimed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        tasks: zero_platform_tokio::contract::scope::TaskRegistrar,
        actor: zero_platform_tokio::contract::scope::TaskJoinWitness,
        endpoint_ordinal: Option<usize>,
        deadline: Instant,
    ) -> io::Result<SocketAddr> {
        self.plan
            .acquire_authorized(
                self.plan.supports_tcp(),
                resources,
                claimed,
                tasks,
                actor,
                endpoint_ordinal,
                deadline,
            )
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
        #[cfg(any(target_os = "linux", windows))]
        {
            let original = lease.child.lock().unwrap().owned();
            if let Some(original) = original {
                return original.wait_root_exit().await;
            }
        }
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
        self.revoke();
        #[cfg(any(target_os = "linux", windows))]
        {
            let pending = self
                .plan
                .pending_endpoint
                .lock()
                .unwrap()
                .as_ref()
                .map(|pending| (pending.endpoint.clone(), pending.process.clone()));
            if let Some((endpoint, process)) = pending {
                let closed = endpoint
                    .shutdown(deadline)
                    .await
                    .map_err(io::Error::other)?
                    .filter(|receipt| receipt.matches(&endpoint))
                    .ok_or_else(timed_out)?;
                let _ = closed;
                if let Some(process) = process {
                    let closed = process
                        .wait_closed(deadline)
                        .await?
                        .filter(|receipt| receipt.matches(&process))
                        .ok_or_else(timed_out)?;
                    if closed.had_native_acquisition() {
                        return process
                            .root_status()
                            .map(PluginTermination::Exited)
                            .ok_or_else(|| {
                                io::Error::other("closed process lacks actual root outcome")
                            });
                    }
                }
            }
        }
        let slot = timeout_at(deadline, self.plan.running.lock())
            .await
            .map_err(|_| timed_out())?;
        let Some(lease) = slot.as_ref() else {
            return Ok(PluginTermination::NotStarted);
        };
        #[cfg(any(target_os = "linux", windows))]
        {
            let original = lease.child.lock().unwrap().owned();
            if let Some(original) = original {
                let receipt = original
                    .wait_closed(deadline)
                    .await?
                    .filter(|receipt| receipt.matches(&original))
                    .ok_or_else(timed_out)?;
                if !receipt.had_native_acquisition() {
                    return Ok(PluginTermination::NotStarted);
                }
                return original
                    .root_status()
                    .map(PluginTermination::Exited)
                    .ok_or_else(|| {
                        io::Error::other("closed original process lacks its root outcome")
                    });
            }
        }
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
