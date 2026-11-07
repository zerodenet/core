//! Exact SOCKS wire peer: its BND domain differs from the literal TCP endpoint.
use super::*;
use zero_core::Address;
use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
};

pub(super) struct Peer {
    pub(super) node: zero_config::OutboundConfig,
    pub(super) control: SocketAddr,
    pub(super) relay: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    pub(super) async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control = listener.local_addr().unwrap();
        let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_address = relay.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tcp = zero_platform_tokio::TokioSocket::new(tcp);
            assert!(matches!(
                socks5::Socks5Inbound
                    .accept_command(&mut tcp)
                    .await
                    .unwrap(),
                socks5::Socks5Request::UdpAssociate(_)
            ));
            socks5::Socks5Inbound
                .send_success_response_with_bound(
                    &mut tcp,
                    &zero_core::Address::Domain("relay.binding.test".into()),
                    relay_address.port(),
                )
                .await
                .unwrap();
            let mut tcp = tcp.into_inner().unwrap();
            let codec = socks5::udp::Socks5InboundUdpCodec;
            let mut bytes = vec![0; 65535];
            let mut extra = [0; 1];
            let mut client = None;
            loop {
                tokio::select! { biased;
                    result = tcp.read(&mut extra) => {
                        assert_eq!(result.unwrap(), 0, "UDP control has no further application bytes");
                        tcp.shutdown().await.unwrap();
                        break;
                    },
                    received = relay.recv_from(&mut bytes) => {
                        let (size, peer) = received.unwrap();
                        if client.is_none() || client == Some(peer) {
                            client = Some(peer);
                            let packet = codec.decode_request(&bytes[..size]).unwrap();
                            let target = match packet.target() {
                                Address::Ipv4(ip) => SocketAddr::new((*ip).into(), packet.port()),
                                Address::Ipv6(ip) => SocketAddr::new((*ip).into(), packet.port()),
                                Address::Domain(_) => panic!("fixture's Mieru physical target is literal"),
                            };
                            relay.send_to(packet.payload(), target).await.unwrap();
                        } else {
                            let source = match peer.ip() {
                                std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                                std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
                            };
                            let encoded = codec.encode_response_to_client(&source, peer.port(), &bytes[..size]).unwrap();
                            relay.send_to(&encoded, client.unwrap()).await.unwrap();
                        }
                    }
                }
            }
        });
        let node = serde_json::from_value(serde_json::json!({"tag":"socks","protocol":{
            "type":"socks5","server":"127.0.0.1","port":control.port()
        }}))
        .unwrap();
        Self {
            node,
            control,
            relay: relay_address,
            task,
        }
    }
    pub(super) async fn close(self) {
        tokio::time::timeout_at(deadline(), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
