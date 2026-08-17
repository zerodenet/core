use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{debug, warn};
use zero_core::{Address, UdpFlowPacket};
use zero_traits::DatagramCodec;
use zero_transport::RuntimeError;

use super::{ShadowsocksManagedDatagramFlowResume, ShadowsocksUdpResponse};

pub struct ShadowsocksUdpSocketFlow {
    socket: Arc<zero_platform_tokio::TokioDatagramSocket>,
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    recv_tx: broadcast::Sender<ShadowsocksUdpResponse>,
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
    let socket = Arc::new(sockets.bind_tokio(endpoint).await?);
    let (recv_tx, _) = broadcast::channel::<ShadowsocksUdpResponse>(32);
    spawn_recv_loop(socket.clone(), codec.clone(), recv_tx.clone());
    Ok(ShadowsocksUdpSocketFlow {
        socket,
        endpoint,
        codec,
        recv_tx,
    })
}

pub async fn establish_shadowsocks_udp_socket_flow_with_resume(
    endpoint: SocketAddr,
    resume: ShadowsocksManagedDatagramFlowResume,
    sockets: &zero_transport::OutboundDatagramSocketFactory,
) -> Result<ShadowsocksUdpSocketFlow, RuntimeError> {
    establish_shadowsocks_udp_socket_flow(
        endpoint,
        resume.into_shared_managed_socket_flow_codec(),
        sockets,
    )
    .await
}

impl ShadowsocksUdpSocketFlow {
    pub fn subscribe(&self) -> broadcast::Receiver<ShadowsocksUdpResponse> {
        self.recv_tx.subscribe()
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
        let datagram = self.codec.encode(target, port, payload)?;
        self.socket.send_to_addr(&datagram, self.endpoint).await?;
        Ok(())
    }
}

fn spawn_recv_loop(
    socket: Arc<zero_platform_tokio::TokioDatagramSocket>,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    recv_tx: broadcast::Sender<ShadowsocksUdpResponse>,
) {
    tokio::spawn(recv_loop(socket, codec, recv_tx));
}

async fn recv_loop(
    socket: Arc<zero_platform_tokio::TokioDatagramSocket>,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    recv_tx: broadcast::Sender<ShadowsocksUdpResponse>,
) {
    let mut buf = vec![0u8; 4096];
    loop {
        let (n, sender) = match socket.recv_from_addr(&mut buf).await {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "shadowsocks udp recv loop stopped");
                break;
            }
        };
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
        if recv_tx.send((target, port, payload)).is_err() {
            break;
        }
    }
}
