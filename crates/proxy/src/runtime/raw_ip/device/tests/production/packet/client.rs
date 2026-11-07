use super::*;
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
pub(super) struct Client {
    socket: tokio::net::UdpSocket,
    tunnel: PeerTunnel,
    source: SocketAddr,
}
impl Client {
    pub(super) fn into_socket(self) -> tokio::net::UdpSocket {
        self.socket
    }

    pub(super) fn from_original(socket: tokio::net::UdpSocket, source: SocketAddr) -> Self {
        let private = STANDARD.encode([112; 32]);
        let remote = peer::public(111);
        let peers = [PeerInput {
            public_key: &remote,
            pre_shared_key: None,
            endpoint: "127.0.0.1:9",
            allowed_ips: &["0.0.0.0/0"],
            keepalive_secs: 0,
            reserved: &[],
        }];
        let profile = validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &["10.42.0.2/32"],
            mtu: 1420,
            peers: &peers,
        })
        .unwrap();
        let tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
        // Read from the same actual native owner's existing observation, never
        // a freed-port reservation, guessed endpoint or readiness marker.
        Self {
            socket,
            tunnel,
            source,
        }
    }
    pub(super) async fn echo(&mut self, payload: &[u8]) -> Vec<u8> {
        let request = zero_stack::packet::build_udp(
            "10.42.0.2".parse().unwrap(),
            "198.51.100.10".parse().unwrap(),
            40000,
            8000,
            payload,
        );
        for action in self.tunnel.send_ip_packet(&request).unwrap() {
            if let TunnelAction::SendNetwork(bytes) = action {
                self.socket.send_to(&bytes, self.source).await.unwrap();
            }
        }
        tokio::time::timeout_at(end(), async {
            let mut buffer = [0; 65535];
            loop {
                let (size, source) = self.socket.recv_from(&mut buffer).await.unwrap();
                assert_eq!(source, self.source, "same native source listener");
                for action in self
                    .tunnel
                    .receive_datagram(Some(source), &buffer[..size])
                    .unwrap()
                {
                    match action {
                        TunnelAction::SendNetwork(bytes) => {
                            self.socket.send_to(&bytes, source).await.unwrap();
                        }
                        TunnelAction::ReceiveIp { packet, .. } => {
                            let reply = zero_stack::packet::parse_udp(&packet)
                                .expect("actual returned UDP envelope");
                            assert_eq!(
                                reply.src.ip,
                                "198.51.100.10".parse::<std::net::IpAddr>().unwrap()
                            );
                            assert_eq!(
                                reply.dst.ip,
                                "10.42.0.2".parse::<std::net::IpAddr>().unwrap()
                            );
                            assert_eq!((reply.src.port, reply.dst.port), (8000, 40000));
                            assert_eq!(reply.payload, payload);
                            return packet.into_vec();
                        }
                    }
                }
            }
        })
        .await
        .expect("actual standard Packet round trip")
    }
}
