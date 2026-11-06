//! External WG peer forwards actual inner UDP/TCP to the held DNS fixture.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::{
    net::{TcpStream, UdpSocket},
    sync::mpsc,
    task::JoinHandle,
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{OutboundInput, PeerInput, validate_outbound},
};
use zero_platform_tokio::contract::CancellationToken;
use zero_stack::{UserNetworkStack, packet};
use zero_traits::TcpStack;
pub(super) fn public(seed: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([seed; 32])).as_bytes())
}
pub(super) struct Peer {
    pub address: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
impl Peer {
    pub async fn new(dns: SocketAddr) -> Self {
        Self::with_tcp(dns, None).await
    }
    pub(super) async fn with_tcp(dns: SocketAddr, business: Option<SocketAddr>) -> Self {
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
        let (send, mut responses) = mpsc::channel(128);
        let (tcp, _) = UserNetworkStack::new(send.clone(), 1380).into_parts();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            let mut endpoint = None;
            let mut bytes = [0; 65535];
            loop {
                let actions = tokio::select! {biased;
                    _=cancel.cancelled()=>break,
                    result=children.join_next(), if !children.is_empty()=>{result.unwrap().unwrap();continue;},
                    accepted=tcp.accept()=>{
                        let (mut stream,_,destination)=accepted.unwrap();
                        let target = business.filter(|target|target.port()==destination.port).unwrap_or(dns);
                        assert_eq!(destination.port,target.port());
                        let cancel=cancel.clone();
                        children.spawn(async move {
                            tokio::select! {biased;_=cancel.cancelled()=>{},_=async {
                                let mut target=TcpStream::connect(target).await.unwrap();
                                // EOF/reset at this external peer is not used
                                // as any controller or Scope close evidence.
                                let _wire=tokio::io::copy_bidirectional(&mut stream,&mut target).await;
                            }=>{}}
                        });continue;
                    },
                    result=socket.recv_from(&mut bytes)=>{
                        let (size,source)=result.unwrap();endpoint=Some(source);
                        tunnel.receive_datagram(Some(source),&bytes[..size]).unwrap()
                    },
                    response=responses.recv()=>tunnel.send_ip_packet(&response.unwrap()).unwrap(),
                };
                for action in actions {
                    match action {
                        TunnelAction::SendNetwork(bytes) => {
                            socket.send_to(&bytes, endpoint.unwrap()).await.unwrap();
                        }
                        TunnelAction::ReceiveIp { packet: inner, .. } => {
                            if let Some(udp) = packet::parse_udp(&inner) {
                                assert_eq!(udp.dst.port, dns.port());
                                let source = udp.src;
                                let destination = udp.dst;
                                let query = udp.payload.to_vec();
                                let send = send.clone();
                                let cancel = cancel.clone();
                                children.spawn(async move {
                                    tokio::select! {biased;_=cancel.cancelled()=>{},_=async {
                                        let socket=UdpSocket::bind("127.0.0.1:0").await.unwrap();
                                        socket.send_to(&query,dns).await.unwrap();
                                        let mut answer=[0;65535];
                                        let (size,peer)=socket.recv_from(&mut answer).await.unwrap();
                                        assert_eq!(peer,dns);
                                        let packet=packet::build_udp(destination.ip,source.ip,destination.port,source.port,&answer[..size]);
                                        send.send(packet).await.unwrap();
                                    }=>{}}
                                });
                            } else {
                                tcp.feed(&inner).await;
                            }
                        }
                    }
                }
            }
            // Cancel and join every real external forwarding operation.
            while let Some(result) = children.join_next().await {
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
