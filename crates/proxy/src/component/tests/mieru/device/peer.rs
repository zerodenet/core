//! Real WG peer forwards decrypted TCP/UDP to the ordinary Mieru fixture.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::SocketAddr;
use tokio::{
    net::{TcpStream, UdpSocket},
    sync::mpsc,
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_platform_tokio::contract::CancellationToken;
use zero_stack::{packet, UserNetworkStack};
use zero_traits::TcpStack;

pub(super) fn public(seed: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([seed; 32])).as_bytes())
}
pub(super) struct Peer {
    pub address: SocketAddr,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub async fn new(backend: SocketAddr) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp.connect(backend).await.unwrap();
        let private = STANDARD.encode([122; 32]);
        let client = public(121);
        let peers = [PeerInput {
            public_key: &client,
            pre_shared_key: None,
            endpoint: "127.0.0.1:9",
            allowed_ips: &["0.0.0.0/0"],
            keepalive_secs: 0,
            reserved: &[],
        }];
        let profile = validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &["10.0.0.2/32"],
            mtu: 1420,
            peers: &peers,
        })
        .unwrap();
        let mut tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
        let (sender, mut responses) = mpsc::channel(128);
        let (tcp, _) = UserNetworkStack::new(sender, 1380).into_parts();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let mut endpoint = None;
            let mut flow: Option<(packet::Endpoint, packet::Endpoint)> = None;
            let mut bytes = vec![0; 65536];
            let mut reply = vec![0; 65536];
            loop {
                let actions = tokio::select! {biased;
                    _=cancel.cancelled()=>break,
                    stream=tcp.accept()=>{
                        let Some((mut stream,_,_))=stream else {break;};
                        let cancel=cancel.clone();
                        connections.spawn(async move {
                            tokio::select! {biased;
                                _=cancel.cancelled()=>{},
                                _=async {
                                    let mut remote=TcpStream::connect(backend).await.unwrap();
                                    let _=tokio::io::copy_bidirectional(&mut stream,&mut remote).await;
                                }=>{},
                            }
                        });
                        continue;
                    },
                    result=socket.recv_from(&mut bytes)=>{
                        let (size,peer)=result.unwrap();endpoint=Some(peer);
                        tunnel.receive_datagram(Some(peer),&bytes[..size]).unwrap()
                    },
                    result=udp.recv(&mut reply),if flow.is_some()=>{
                        let size=result.unwrap();
                        let (source,target)=flow.unwrap();
                        let packet=packet::build_udp(target.ip,source.ip,target.port,source.port,&reply[..size]);
                        tunnel.send_ip_packet(&packet).unwrap()
                    },
                    response=responses.recv()=>tunnel.send_ip_packet(&response.unwrap()).unwrap(),
                };
                for action in actions {
                    match action {
                        TunnelAction::SendNetwork(bytes) => {
                            socket.send_to(&bytes, endpoint.unwrap()).await.unwrap();
                        }
                        TunnelAction::ReceiveIp { packet: inner, .. } => {
                            if let Some(packet) = packet::parse_udp(&inner) {
                                flow = Some((packet.src, packet.dst));
                                udp.send(packet.payload).await.unwrap();
                            } else {
                                tcp.feed(&inner).await;
                            }
                        }
                    }
                }
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            stop,
            task,
        }
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
