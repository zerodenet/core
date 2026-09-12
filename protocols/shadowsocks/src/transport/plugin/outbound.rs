use super::process;
use crate::validation::PluginConfig;
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
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
    running: Arc<tokio::sync::Mutex<Option<Arc<PluginLease>>>>,
}
pub(crate) struct PluginLease {
    child: Mutex<Child>,
    endpoint: SocketAddr,
}
impl core::fmt::Debug for PluginLease {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PluginLease")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}
impl PluginLease {
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
        if (tcp && !self.key.config.mode.tcp()) || (!tcp && !self.key.config.mode.udp()) {
            return Ok(None);
        }
        let mut slot = self.running.lock().await;
        if let Some(running) = slot.as_ref() {
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
        let mut child = process::spawn(
            &self.key.config,
            (&self.key.host, self.key.port),
            local,
            false,
        )?;
        process::ready(&mut child, local, self.key.config.mode.tcp()).await?;
        let running = Arc::new(PluginLease {
            child: Mutex::new(child),
            endpoint: local,
        });
        *slot = Some(running.clone());
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

#[cfg(all(test, unix))]
#[path = "../../../tests/plugin/lifecycle.rs"]
mod tests;
