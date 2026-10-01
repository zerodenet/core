use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
    sync::mpsc,
    task::JoinHandle,
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_stack::{packet, UserNetworkStack};
use zero_traits::TcpStack;
pub fn public(seed: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([seed; 32])).as_bytes())
}
pub async fn echo_peer() -> (SocketAddr, JoinHandle<()>) {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let address = socket.local_addr().unwrap();
    let private = STANDARD.encode([122; 32]);
    let client = public(121);
    let addresses = ["10.0.0.2/32"];
    let allowed = ["0.0.0.0/0"];
    let peers = [PeerInput {
        public_key: &client,
        pre_shared_key: None,
        endpoint: "127.0.0.1:9",
        allowed_ips: &allowed,
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &private,
        addresses: &addresses,
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let mut tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
    let (sender, mut responses) = mpsc::channel(128);
    let (tcp, _) = UserNetworkStack::new(sender, 1380).into_parts();
    let echo_tcp = tcp.clone();
    let echo = tokio::spawn(async move {
        while let Some((mut stream, _, _)) = echo_tcp.accept().await {
            tokio::spawn(async move {
                let mut buffer = [0; 1024];
                while let Ok(size) = stream.read(&mut buffer).await {
                    if size == 0 {
                        break;
                    }
                    stream.write_all(&buffer[..size]).await.unwrap();
                }
            });
        }
    });
    let task = tokio::spawn(async move {
        let _echo = Abort(echo);
        let mut endpoint = None;
        let mut buffer = [0; 65535];
        loop {
            let actions = tokio::select! {
                received=socket.recv_from(&mut buffer) => {
                    let (size,source)=received.unwrap(); endpoint=Some(source);
                    tunnel.receive_datagram(Some(source),&buffer[..size]).unwrap()
                }
                response=responses.recv() => tunnel.send_ip_packet(&response.unwrap()).unwrap(),
            };
            for action in actions {
                match action {
                    TunnelAction::SendNetwork(bytes) => {
                        socket.send_to(&bytes, endpoint.unwrap()).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet: inner, .. } => {
                        if let Some(udp) = packet::parse_udp(&inner) {
                            let reply = packet::build_udp(
                                udp.dst.ip,
                                udp.src.ip,
                                udp.dst.port,
                                udp.src.port,
                                udp.payload,
                            );
                            for action in tunnel.send_ip_packet(&reply).unwrap() {
                                if let TunnelAction::SendNetwork(bytes) = action {
                                    socket.send_to(&bytes, endpoint.unwrap()).await.unwrap();
                                }
                            }
                        } else {
                            tcp.feed(&inner).await;
                        }
                    }
                }
            }
        }
    });
    (address, task)
}
struct Abort(JoinHandle<()>);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}
