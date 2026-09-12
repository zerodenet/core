use super::process;
use crate::validation::PluginConfig;
use std::{net::SocketAddr, sync::Arc};
use zero_platform_tokio::TokioListener;
use zero_transport::{
    inbound_carrier::{CarrierFuture, InboundCarrier, InboundCarrierPlan},
    RuntimeError,
};

pub struct ShadowsocksInboundPluginPlan {
    config: PluginConfig,
}
impl ShadowsocksInboundPluginPlan {
    pub fn new(config: PluginConfig) -> Self {
        Self { config }
    }
}
impl InboundCarrierPlan for ShadowsocksInboundPluginPlan {
    fn activate(self: Box<Self>, original: TokioListener) -> CarrierFuture<InboundCarrier> {
        Box::pin(async move {
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
            let mut child = process::spawn(&self.config, (&host, remote.port()), local, true)?;
            let probe = SocketAddr::new(
                if remote.ip().is_unspecified() {
                    local_ip
                } else {
                    remote.ip()
                },
                remote.port(),
            );
            process::ready(&mut child, probe, self.config.mode.tcp()).await?;
            Ok(InboundCarrier {
                listener,
                datagram,
                completion: Box::pin(async move {
                    let status = child.wait().await?;
                    Err(RuntimeError::Io(std::io::Error::other(format!(
                        "shadowsocks inbound plugin exited: {status}"
                    ))))
                }),
            })
        })
    }
}
