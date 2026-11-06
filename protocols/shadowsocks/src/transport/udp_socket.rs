use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{debug, warn};
use zero_core::{Address, UdpFlowPacket};
use zero_traits::DatagramCodec;
use zero_transport::RuntimeError;

use super::{ShadowsocksManagedDatagramFlowResume, ShadowsocksUdpResponse};
mod responses;
use responses::Responses;
pub use responses::ShadowsocksUdpResponseSink;

pub struct ShadowsocksUdpSocketFlow {
    plugin: Option<Arc<super::plugin::outbound::PluginLease>>,
    socket: Arc<zero_platform_tokio::TokioDatagramSocket>,
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    recv_tx: broadcast::WeakSender<ShadowsocksUdpResponse>,
    responses: Arc<Responses>,
    receiver: tokio::task::AbortHandle,
}

pub fn managed_socket_flow_from_resume(
    resume: &ShadowsocksManagedDatagramFlowResume,
) -> crate::udp::ShadowsocksUdpSocketFlowSpec {
    resume.socket_flow_spec()
}

pub async fn establish_shadowsocks_udp_socket_flow(
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<ShadowsocksUdpSocketFlow, RuntimeError> {
    establish_with_plugin(endpoint, codec, sockets, None).await
}

pub(super) async fn establish_with_plugin(
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
    plugin: Option<Arc<super::plugin::outbound::PluginLease>>,
) -> Result<ShadowsocksUdpSocketFlow, RuntimeError> {
    let socket = Arc::new(sockets.bind_tokio(endpoint).await?);
    let (recv_tx, _) = broadcast::channel::<ShadowsocksUdpResponse>(32);
    let responses = Arc::new(Responses::default());
    let receiver = tokio::spawn(recv_loop(
        socket.clone(),
        endpoint,
        codec.clone(),
        recv_tx.clone(),
        plugin.clone(),
        responses.clone(),
    ))
    .abort_handle();
    Ok(ShadowsocksUdpSocketFlow {
        plugin,
        socket,
        endpoint,
        codec,
        recv_tx: recv_tx.downgrade(),
        responses,
        receiver,
    })
}

pub async fn establish_shadowsocks_udp_socket_flow_with_resume(
    endpoint: SocketAddr,
    resume: ShadowsocksManagedDatagramFlowResume,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<ShadowsocksUdpSocketFlow, RuntimeError> {
    let plugin = match &resume.plugin {
        Some(plugin) => plugin.acquire(false).await?,
        None => None,
    };
    let endpoint = plugin.as_ref().map_or(endpoint, |plugin| plugin.endpoint());
    establish_with_plugin(
        endpoint,
        resume.into_shared_managed_socket_flow_codec(),
        sockets,
        plugin,
    )
    .await
}

impl ShadowsocksUdpSocketFlow {
    /// Bind the existing cache's FIFO response destination to this same receive
    /// future. The cache must retain the callback; this stores only a Weak and
    /// creates neither a second socket nor a response-forwarding task.
    pub fn install_response_sink(&self, sink: &Arc<ShadowsocksUdpResponseSink>) -> bool {
        self.responses.install(sink)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<ShadowsocksUdpResponse> {
        if let Some(sender) = self.recv_tx.upgrade() {
            sender.subscribe()
        } else {
            let (_, receiver) = broadcast::channel(1);
            receiver
        }
    }

    pub async fn send_packet(&self, packet: UdpFlowPacket) -> Result<(), RuntimeError> {
        self.send_datagram(&packet.target, packet.port, &packet.payload)
            .await
    }

    pub async fn send_datagram(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), RuntimeError> {
        if let Some(plugin) = &self.plugin {
            plugin.check()?;
        }
        let datagram = self.codec.encode(target, port, payload)?;
        self.socket.send_to_addr(&datagram, self.endpoint).await?;
        Ok(())
    }
}

impl Drop for ShadowsocksUdpSocketFlow {
    fn drop(&mut self) {
        self.receiver.abort();
    }
}

async fn recv_loop(
    socket: Arc<zero_platform_tokio::TokioDatagramSocket>,
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    recv_tx: broadcast::Sender<ShadowsocksUdpResponse>,
    plugin: Option<Arc<super::plugin::outbound::PluginLease>>,
    responses: Arc<Responses>,
) {
    let mut buf = vec![0u8; 65535];
    let exited = async {
        match &plugin {
            Some(plugin) => plugin.exited().await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(exited);
    loop {
        let received = tokio::select! {
            received = socket.recv_from_addr(&mut buf) => received,
            _ = &mut exited => { warn!("shadowsocks UDP plugin exited"); break; }
        };
        let (n, sender) = match received {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "shadowsocks udp recv loop stopped");
                break;
            }
        };
        if sender != endpoint {
            continue;
        }
        let datagram = &buf[..n];
        let Some((target, port, payload)) = codec.decode(datagram) else {
            warn!(upstream = %sender, bytes = n, "failed to decode shadowsocks udp response");
            continue;
        };
        debug!(
            upstream = %sender,
            target = ?target,
            port,
            bytes = payload.len(),
            "decoded shadowsocks udp response"
        );
        if !responses.deliver((target, port, payload), &recv_tx) {
            break;
        }
    }
}

#[cfg(test)]
#[path = "../../tests/udp_session/socket.rs"]
mod tests;
