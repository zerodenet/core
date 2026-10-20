use super::process;
mod owner;
use crate::validation::PluginConfig;
use owner::{Owner, Shared, StartupAttempt};
use std::{net::SocketAddr, sync::Arc};
use zero_platform_tokio::TokioListener;
use zero_transport::{
    inbound_carrier::{CarrierFuture, InboundCarrier, InboundCarrierPlan},
    RuntimeError,
};

pub struct ShadowsocksInboundPluginPlan {
    config: PluginConfig,
    shared: Arc<Shared>,
}

#[cfg(all(test, unix))]
#[path = "../../../tests/plugin/inbound.rs"]
mod tests;
impl ShadowsocksInboundPluginPlan {
    pub fn new(config: PluginConfig) -> Self {
        Self {
            config,
            shared: Arc::default(),
        }
    }
}
impl InboundCarrierPlan for ShadowsocksInboundPluginPlan {
    fn owner(&self) -> Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle> {
        zero_platform_tokio::contract::lifecycle::erase_lifecycle(Owner(self.shared.clone()))
    }
    fn activate(self: Box<Self>, original: TokioListener) -> CarrierFuture<InboundCarrier> {
        self.activate_with_probe(original, tokio::net::TcpStream::connect)
    }
}
impl ShadowsocksInboundPluginPlan {
    fn activate_with_probe<Probe, Pending>(
        self: Box<Self>,
        original: TokioListener,
        mut connect_probe: Probe,
    ) -> CarrierFuture<InboundCarrier>
    where
        Probe: FnMut(SocketAddr) -> Pending + Send + 'static,
        Pending: std::future::Future<Output = std::io::Result<tokio::net::TcpStream>> + Send,
    {
        Box::pin(async move {
            self.shared.begin()?;
            let remote = original.local_addr()?;
            let host = remote.ip().to_string();
            let local_ip = process::loopback(&host);
            let listener = if self.config.mode.tcp() {
                drop(original);
                TokioListener::bind(&SocketAddr::new(local_ip, 0).to_string()).await?
            } else {
                original
            };
            let udp_addr = if self.config.mode.udp() {
                if self.config.mode.tcp() {
                    listener.local_addr()?
                } else {
                    SocketAddr::new(local_ip, 0)
                }
            } else {
                remote
            };
            let datagram = Arc::new(tokio::net::UdpSocket::bind(udp_addr).await?);
            let local = if self.config.mode.tcp() {
                listener.local_addr()?
            } else {
                datagram.local_addr()?
            };
            self.shared
                .spawn(&self.config, (&host, remote.port()), local)?;
            let mut startup = StartupAttempt {
                shared: self.shared.clone(),
                armed: true,
            };
            let probe = SocketAddr::new(
                if remote.ip().is_unspecified() {
                    local_ip
                } else {
                    remote.ip()
                },
                remote.port(),
            );
            tokio::select! { biased;
                _ = self.shared.cancel.cancelled() => return Err(RuntimeError::Io(std::io::Error::new(std::io::ErrorKind::Interrupted,"inbound plugin activation cancelled"))),
                result = process::ready_with_probe(||self.shared.status(), probe, self.config.mode.tcp(), ||connect_probe(probe)) => result?,
            }
            startup.armed = false;
            let shared = self.shared.clone();
            Ok(InboundCarrier {
                listener,
                datagram,
                completion: Box::pin(async move {
                    loop {
                        if let Some(status) = shared.status()? {
                            return Err(RuntimeError::Io(std::io::Error::other(format!(
                                "shadowsocks inbound plugin exited: {status}"
                            ))));
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }),
            })
        })
    }
}
