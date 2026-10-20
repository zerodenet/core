use super::process;
use crate::validation::PluginConfig;
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::process::Child;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    host: String,
    port: u16,
    config: PluginConfig,
}
#[derive(Default, Debug)]
pub(crate) struct PluginPool {
    plans: Mutex<HashMap<Key, PluginPlan>>,
}
impl PluginPool {
    pub(crate) fn plan(&self, host: &str, port: u16, config: PluginConfig) -> PluginPlan {
        let key = Key {
            host: host.into(),
            port,
            config,
        };
        self.plans
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| PluginPlan {
                key,
                running: Default::default(),
                cancel: Default::default(),
            })
            .clone()
    }
    pub(crate) fn clear(&self) {
        self.plans.lock().unwrap().clear();
    }
}
#[derive(Clone, Debug)]
pub(crate) struct PluginPlan {
    key: Key,
    pub(super) running: Arc<tokio::sync::Mutex<Option<Arc<PluginLease>>>>,
    pub(super) cancel: zero_platform_tokio::contract::CancellationToken,
}
pub(crate) struct PluginLease {
    pub(super) child: Mutex<Child>,
    endpoint: SocketAddr,
    readiness: OnceLock<Result<(), Arc<io::Error>>>,
}
#[derive(Debug)]
struct StartupFailure(Arc<io::Error>);
impl std::fmt::Display for StartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for StartupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
struct StartupAttempt {
    lease: Arc<PluginLease>,
    armed: bool,
}
impl Drop for StartupAttempt {
    fn drop(&mut self) {
        if self.armed {
            // Cancellation requests termination, while the persistent slot keeps
            // the Child handle for actual wait/reaping. This is no close report.
            let _ = self.lease.child.lock().unwrap().start_kill();
        }
    }
}
impl core::fmt::Debug for PluginLease {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PluginLease")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}
impl PluginLease {
    fn ready(&self) -> io::Result<()> {
        match self.readiness.get() {
            Some(Ok(())) => Ok(()),
            Some(Err(source)) => Err(io::Error::new(
                source.kind(),
                StartupFailure(source.clone()),
            )),
            None => Err(io::Error::other(
                "shadowsocks outbound plugin startup was interrupted",
            )),
        }
    }
    pub(crate) async fn exited(&self) {
        loop {
            if self.check().is_err() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    pub(crate) fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }
    pub(crate) fn check(&self) -> io::Result<()> {
        if self.child.lock().unwrap().try_wait()?.is_some() {
            return Err(io::Error::other("shadowsocks outbound plugin exited"));
        }
        Ok(())
    }
}
impl PluginPlan {
    pub(crate) fn supports_tcp(&self) -> bool {
        self.key.config.mode.tcp()
    }
    pub(crate) fn supports_udp(&self) -> bool {
        self.key.config.mode.udp()
    }
    pub(crate) async fn acquire(&self, tcp: bool) -> io::Result<Option<Arc<PluginLease>>> {
        self.acquire_with_probe(tcp, tokio::net::TcpStream::connect)
            .await
    }
    pub(super) async fn acquire_with_probe<Probe, Pending>(
        &self,
        tcp: bool,
        mut probe: Probe,
    ) -> io::Result<Option<Arc<PluginLease>>>
    where
        Probe: FnMut(SocketAddr) -> Pending + Send,
        Pending: std::future::Future<Output = io::Result<tokio::net::TcpStream>> + Send,
    {
        if (tcp && !self.key.config.mode.tcp()) || (!tcp && !self.key.config.mode.udp()) {
            return Ok(None);
        }
        let mut slot = tokio::select! { biased;
            _ = self.cancel.cancelled() => return Err(revoked()),
            slot = self.running.lock() => slot,
        };
        if self.cancel.is_cancelled() {
            return Err(revoked());
        }
        if let Some(running) = slot.as_ref() {
            running.ready()?;
            running.check()?;
            return Ok(Some(running.clone()));
        }
        let local = SocketAddr::new(process::loopback(&self.key.host), 0);
        let reservation = tokio::net::TcpListener::bind(local).await?;
        let local = reservation.local_addr()?;
        let udp_reservation = if self.key.config.mode.udp() {
            Some(tokio::net::UdpSocket::bind(local).await?)
        } else {
            None
        };
        drop(reservation);
        drop(udp_reservation);
        if self.cancel.is_cancelled() {
            return Err(revoked());
        }
        let child = process::spawn(
            &self.key.config,
            (&self.key.host, self.key.port),
            local,
            false,
        )?;
        let running = Arc::new(PluginLease {
            child: Mutex::new(child),
            endpoint: local,
            readiness: OnceLock::new(),
        });
        // Install the child before readiness can suspend. Aborting this caller
        // leaves the unique owner in the plan; an incomplete attempt is terminal.
        *slot = Some(running.clone());
        let mut attempt = StartupAttempt {
            lease: running.clone(),
            armed: true,
        };
        let readiness = process::ready_with_probe(
            || running.child.lock().unwrap().try_wait(),
            local,
            self.key.config.mode.tcp(),
            || probe(local),
        );
        let result = tokio::select! { biased;
            _ = self.cancel.cancelled() => Err(revoked()),
            result = readiness => result,
        };
        let succeeded = result.is_ok();
        let _ = running.readiness.set(result.map_err(Arc::new));
        attempt.armed = !succeeded;
        running.ready()?;
        Ok(Some(running))
    }
    pub(crate) fn cache_identity(&self) -> String {
        let mode = format!("{:?}", self.key.config.mode);
        let port = self.key.port.to_be_bytes();
        let options_present = [u8::from(self.key.config.options.is_some())];
        let mut parts = vec![
            self.key.host.as_bytes(),
            port.as_slice(),
            self.key.config.command.as_bytes(),
            mode.as_bytes(),
            options_present.as_slice(),
            self.key.config.options.as_deref().unwrap_or("").as_bytes(),
        ];
        parts.extend(self.key.config.args.iter().map(|arg| arg.as_bytes()));
        format!("plugin:{}", crate::shared::cache_identity(parts))
    }
}

fn revoked() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "shadowsocks outbound plugin was revoked",
    )
}

#[cfg(all(test, unix))]
#[path = "../../../tests/plugin/lifecycle.rs"]
mod tests;
