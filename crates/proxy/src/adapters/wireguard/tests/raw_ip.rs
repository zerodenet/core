use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use boringtun::x25519::{PublicKey, StaticSecret};
use wireguard::{
    routing::PeerRoutes,
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_core::Address;
use zero_engine::EngineError;
use zero_platform_tokio::TokioDatagramSocket;
use zero_stack::packet;

use crate::runtime::{
    raw_ip::{RawIpAction, RawIpDevicePool, RawIpTunnel, SharedRawIpDevice},
    udp_flow::managed::{raw_ip::RawIpUdpFlow, ManagedTupleUdpFlowConnection},
};

struct TestTunnel {
    tunnel: PeerTunnel,
    routes: PeerRoutes,
}

impl RawIpTunnel for TestTunnel {
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(convert(self.tunnel.initiate_handshake().unwrap()))
    }

    fn send_ip_packet(&mut self, packet: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(convert(self.tunnel.send_ip_packet(packet).unwrap()))
    }

    fn receive_datagram(
        &mut self,
        source: Option<IpAddr>,
        datagram: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(convert(
            self.tunnel.receive_datagram(source, datagram).unwrap(),
        ))
    }

    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        Ok(convert(self.tunnel.tick().unwrap()))
    }

    fn allows_source(&self, source: IpAddr) -> bool {
        self.routes.allows_authenticated_source(0, source)
    }

    fn time_since_last_handshake(&self) -> Option<Duration> {
        self.tunnel.time_since_last_handshake()
    }
}

fn convert(actions: Vec<TunnelAction>) -> Vec<RawIpAction> {
    actions
        .into_iter()
        .map(|action| match action {
            TunnelAction::SendNetwork(packet) => RawIpAction::SendNetwork(packet),
            TunnelAction::ReceiveIp { packet, source } => RawIpAction::ReceiveIp { packet, source },
        })
        .collect()
}

fn profile(
    private: u8,
    peer_private: u8,
    address: &str,
    allowed: &str,
) -> (PeerTunnel, PeerRoutes) {
    let private_key = STANDARD.encode([private; 32]);
    let peer_public = PublicKey::from(&StaticSecret::from([peer_private; 32]));
    let public_key = STANDARD.encode(peer_public.as_bytes());
    let addresses = [address];
    let allowed_ips = [allowed];
    let peers = [PeerInput {
        public_key: &public_key,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &allowed_ips,
        keepalive_secs: 0,
        reserved: &[],
    }];
    let validated = validate_outbound(OutboundInput {
        private_key: &private_key,
        addresses: &addresses,
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    (
        PeerTunnel::from_validated(&validated, 0).unwrap(),
        PeerRoutes::from_validated(&validated),
    )
}

#[tokio::test]
async fn wireguard_udp_device_shares_handshake_across_flows() {
    let local = Ipv4Addr::new(10, 0, 0, 1);
    let remote = Ipv4Addr::new(10, 0, 0, 2);
    let server_socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let server_endpoint = server_socket.local_addr().unwrap();
    let client_socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let (client_tunnel, client_routes) = profile(1, 2, "10.0.0.1/32", "10.0.0.2/32");
    let (mut server_tunnel, _) = profile(2, 1, "10.0.0.2/32", "10.0.0.1/32");
    let server = async move {
        let mut wire = [0_u8; 65_535];
        let mut replies = 0;
        loop {
            let (len, sender) = server_socket.recv_from_addr(&mut wire).await.unwrap();
            let actions = server_tunnel
                .receive_datagram(Some(sender.ip()), &wire[..len])
                .unwrap();
            for action in actions {
                match action {
                    TunnelAction::SendNetwork(packet) => {
                        server_socket.send_to_addr(&packet, sender).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => {
                        let datagram = packet::parse_udp(&packet).unwrap();
                        assert_eq!(datagram.payload, b"ping");
                        let response = packet::build_udp(
                            IpAddr::V4(remote),
                            IpAddr::V4(local),
                            53,
                            datagram.src.port,
                            b"pong",
                        );
                        for action in server_tunnel.send_ip_packet(&response).unwrap() {
                            if let TunnelAction::SendNetwork(packet) = action {
                                server_socket.send_to_addr(&packet, sender).await.unwrap();
                            }
                        }
                        replies += 1;
                        if replies == 2 {
                            return;
                        }
                    }
                }
            }
        }
    };
    let client = async move {
        let target = Address::Ipv4(remote.octets());
        let device = SharedRawIpDevice::start(
            vec![IpAddr::V4(local)],
            1_420,
            server_endpoint,
            client_socket,
            Box::new(TestTunnel {
                tunnel: client_tunnel,
                routes: client_routes,
            }),
        )
        .unwrap();
        let flow = RawIpUdpFlow::open(
            device.clone(),
            target.clone(),
            53,
            SocketAddr::new(IpAddr::V4(remote), 53),
            IpAddr::V4(local),
        )
        .unwrap();
        let mut responses = flow.subscribe_responses();
        assert_eq!(flow.send(&target, 53, b"ping").await.unwrap(), 4);
        let (source, port, payload) =
            tokio::time::timeout(Duration::from_secs(5), responses.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(source, target);
        assert_eq!(port, 53);
        assert_eq!(payload, b"pong");
        assert_eq!(
            device.health_snapshot("wg".to_owned(), 0).state,
            zero_api::OutboundDeviceHealthState::Reachable,
        );
        let second = RawIpUdpFlow::open(
            device.clone(),
            target.clone(),
            53,
            SocketAddr::new(IpAddr::V4(remote), 53),
            IpAddr::V4(local),
        )
        .unwrap();
        let mut second_responses = second.subscribe_responses();
        assert_eq!(second.send(&target, 53, b"ping").await.unwrap(), 4);
        let (source, port, payload) =
            tokio::time::timeout(Duration::from_secs(5), second_responses.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(source, target);
        assert_eq!(port, 53);
        assert_eq!(payload, b"pong");
        device.close_now();
    };
    tokio::join!(server, client);
}

#[tokio::test]
async fn wireguard_udp_flow_reports_authenticated_icmp_unreachable() {
    let local = Ipv4Addr::new(10, 0, 0, 1);
    let remote = Ipv4Addr::new(10, 0, 0, 2);
    let server_socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let server_endpoint = server_socket.local_addr().unwrap();
    let client_socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let (client_tunnel, client_routes) = profile(11, 12, "10.0.0.1/32", "10.0.0.2/32");
    let (mut server_tunnel, _) = profile(12, 11, "10.0.0.2/32", "10.0.0.1/32");
    let server = async move {
        let mut wire = [0_u8; 65_535];
        loop {
            let (len, sender) = server_socket.recv_from_addr(&mut wire).await.unwrap();
            let actions = server_tunnel
                .receive_datagram(Some(sender.ip()), &wire[..len])
                .unwrap();
            for action in actions {
                match action {
                    TunnelAction::SendNetwork(packet) => {
                        server_socket.send_to_addr(&packet, sender).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => {
                        let response =
                            packet::build_udp_unreachable_response(&packet, 1_420).unwrap();
                        for action in server_tunnel.send_ip_packet(&response).unwrap() {
                            if let TunnelAction::SendNetwork(packet) = action {
                                server_socket.send_to_addr(&packet, sender).await.unwrap();
                            }
                        }
                        return;
                    }
                }
            }
        }
    };
    let client = async move {
        let target = Address::Ipv4(remote.octets());
        let device = SharedRawIpDevice::start(
            vec![IpAddr::V4(local)],
            1_420,
            server_endpoint,
            client_socket,
            Box::new(TestTunnel {
                tunnel: client_tunnel,
                routes: client_routes,
            }),
        )
        .unwrap();
        let flow = RawIpUdpFlow::open(
            device.clone(),
            target.clone(),
            53,
            SocketAddr::new(IpAddr::V4(remote), 53),
            IpAddr::V4(local),
        )
        .unwrap();
        flow.send(&target, 53, b"query").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !flow.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("authenticated ICMP error must close the flow");
        let error = flow.send(&target, 53, b"retry").await.unwrap_err();
        assert!(
            error.to_string().contains("DestinationUnreachable"),
            "{error}"
        );
        device.close_now();
    };
    tokio::join!(server, client);
}

struct FailedTunnel;

impl RawIpTunnel for FailedTunnel {
    fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        Err(EngineError::Io(std::io::Error::other(
            "injected handshake failure",
        )))
    }

    fn send_ip_packet(&mut self, _packet: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
        unreachable!()
    }

    fn receive_datagram(
        &mut self,
        _source: Option<IpAddr>,
        _datagram: &[u8],
    ) -> Result<Vec<RawIpAction>, EngineError> {
        unreachable!()
    }

    fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
        unreachable!()
    }

    fn allows_source(&self, _source: IpAddr) -> bool {
        false
    }
}

#[tokio::test]
async fn raw_ip_staging_failure_keeps_published_device() {
    struct IdleTunnel;
    impl RawIpTunnel for IdleTunnel {
        fn initiate_handshake(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(vec![])
        }
        fn send_ip_packet(&mut self, _: &[u8]) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(vec![])
        }
        fn receive_datagram(
            &mut self,
            _: Option<IpAddr>,
            _: &[u8],
        ) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(vec![])
        }
        fn tick(&mut self) -> Result<Vec<RawIpAction>, EngineError> {
            Ok(vec![])
        }
        fn allows_source(&self, _: IpAddr) -> bool {
            true
        }
    }
    let socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let pool = RawIpDevicePool::default();
    let first = SharedRawIpDevice::start(
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        1_420,
        "127.0.0.1:51820".parse().unwrap(),
        socket,
        Box::new(IdleTunnel),
    )
    .unwrap();
    first.wait_ready().await.unwrap();
    let first_cell = Arc::new(tokio::sync::OnceCell::new());
    first_cell
        .set(first.clone())
        .unwrap_or_else(|_| panic!("cell initialized twice"));
    let mut active = pool.begin_stage();
    active
        .insert("wg".to_owned(), 0, [1; 32], 1, first_cell.clone(), true)
        .unwrap();
    pool.publish(active);
    assert!(pool.is_current("wg", 0, [1; 32], 1, &first));
    assert!(pool.health_snapshot("wg", 0, [1; 32]).is_some());
    assert!(pool.health_snapshot("wg", 0, [2; 32]).is_none());
    pool.mark_endpoint_unresolved("wg", 0, [2; 32]);
    assert!(
        !pool
            .health_snapshot("wg", 0, [1; 32])
            .unwrap()
            .endpoint_resolution_failed
    );
    assert_eq!(
        pool.health_snapshot("wg", 0, [2; 32]).unwrap().state,
        zero_api::OutboundDeviceHealthState::EndpointUnresolved
    );

    let socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let device = SharedRawIpDevice::start(
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        1_420,
        "127.0.0.1:51820".parse().unwrap(),
        socket,
        Box::new(FailedTunnel),
    )
    .unwrap();
    assert!(device.wait_ready().await.is_err());
    assert!(device.is_closed());
    assert!(pool.is_current("wg", 0, [1; 32], 1, &first));
    assert!(first.is_usable());

    let second_cell = Arc::new(tokio::sync::OnceCell::new());
    let socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let second = SharedRawIpDevice::start(
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        1_420,
        "127.0.0.1:51820".parse().unwrap(),
        socket,
        Box::new(IdleTunnel),
    )
    .unwrap();
    second.wait_ready().await.unwrap();
    second_cell
        .set(second.clone())
        .unwrap_or_else(|_| panic!("cell initialized twice"));
    let mut candidate = pool.begin_stage();
    candidate
        .insert("wg".to_owned(), 0, [2; 32], 1, second_cell, true)
        .unwrap();
    drop(candidate);
    assert!(second.is_closed(), "unpublished candidate is closed");
    assert!(pool.is_current("wg", 0, [1; 32], 1, &first));
    let replacement_socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let replacement = SharedRawIpDevice::start(
        vec![IpAddr::V4(Ipv4Addr::LOCALHOST)],
        1_420,
        "127.0.0.1:51820".parse().unwrap(),
        replacement_socket,
        Box::new(IdleTunnel),
    )
    .unwrap();
    replacement.wait_ready().await.unwrap();
    let replacement_cell = Arc::new(tokio::sync::OnceCell::new());
    replacement_cell
        .set(replacement.clone())
        .unwrap_or_else(|_| panic!("cell initialized twice"));
    let mut committed = pool.begin_stage();
    committed
        .insert("wg".to_owned(), 0, [2; 32], 1, replacement_cell, true)
        .unwrap();
    pool.publish(committed);
    assert!(pool.health_snapshot("wg", 0, [1; 32]).is_none());
    assert!(
        !pool
            .health_snapshot("wg", 0, [2; 32])
            .unwrap()
            .endpoint_resolution_failed
    );
    assert!(!first.is_usable(), "replaced device rejects new flows");
    assert!(pool.is_current("wg", 0, [2; 32], 1, &replacement));
    pool.shutdown();
    assert!(first.is_closed());
    assert!(replacement.is_closed());
}
