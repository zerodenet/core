#![cfg(all(feature = "wireguard", feature = "socks5"))]

mod support;

use std::net::{IpAddr, Ipv4Addr};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use boringtun::x25519::{PublicKey, StaticSecret};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{sleep, timeout, Duration},
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};
use zero_stack::packet::{build_udp, checksum, parse_udp};

use support::interop::socks5_udp_echo_to;
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

fn public_key(private: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([private; 32])).as_bytes())
}

fn non_loopback_host_ipv4() -> Ipv4Addr {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    let IpAddr::V4(ip) = socket.local_addr().unwrap().ip() else {
        panic!("IPv4 required")
    };
    ip
}

#[tokio::test]
async fn zero_wireguard_inbound_routes_tcp_and_udp_from_zero_outbound() {
    support::interop::init_logs("wireguard=debug");
    let server_port = free_udp_port();
    let socks_port = free_port();
    let host_ip = non_loopback_host_ipv4();
    let server_private = STANDARD.encode([61_u8; 32]);
    let client_private = STANDARD.encode([62_u8; 32]);
    let server_public = public_key(61);
    let client_public = public_key(62);

    let server_config = RuntimeConfig::parse(&format!(
        r#"{{
        "inbounds": [{{
            "tag": "wg-in", "listen": {{"address": "127.0.0.1", "port": {server_port}}},
            "protocol": {{"type": "wireguard", "private_key": "{server_private}",
                "peers": [{{"public_key": "{client_public}", "allowed_ips": ["10.0.0.2/32"]}}]}}
        }}],
        "outbounds": [{{"tag": "direct", "protocol": {{"type": "direct"}}}}],
        "route": {{"rules": [], "final": {{"type": "route", "outbound": "direct"}}}}
    }}"#
    ))
    .unwrap();
    let server_proxy = Proxy::new(server_config.clone()).unwrap();
    let server_handle = ProxyHandle::new(
        EngineHandle::new(server_proxy.engine().clone()),
        server_proxy.clone(),
    );
    let server = spawn_engine(server_proxy);
    sleep(Duration::from_millis(100)).await;

    let client_config = RuntimeConfig::parse(&format!(r#"{{
        "inbounds": [{{"tag": "socks-in", "listen": {{"address": "127.0.0.1", "port": {socks_port}}}, "protocol": {{"type": "socks5"}}}}],
        "outbounds": [{{"tag": "wg-out", "protocol": {{
            "type": "wireguard", "private_key": "{client_private}", "addresses": ["10.0.0.2/32"],
            "peers": [{{"public_key": "{server_public}", "endpoint": "127.0.0.1:{server_port}", "allowed_ips": ["0.0.0.0/0"]}}]
        }}}}],
        "route": {{"rules": [], "final": {{"type": "route", "outbound": "wg-out"}}}}
    }}"#)).unwrap();
    let client = spawn_engine(Proxy::new(client_config).unwrap());
    wait_for_listener(socks_port).await;

    let udp_echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let udp_port = udp_echo.local_addr().unwrap().port();
    let udp_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        for _ in 0..3 {
            let (size, source) = udp_echo.recv_from(&mut buffer).await.unwrap();
            udp_echo.send_to(&buffer[..size], source).await.unwrap();
        }
    });
    // Larger than the inner MTU, exercising both outbound fragmentation and
    // inbound response reassembly over the same authenticated peer.
    let payload = b"zero-to-zero-wireguard-udp".repeat(60);
    let echoed = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_to(
            socks_port,
            Address::Ipv4(host_ip.octets()),
            udp_port,
            &payload,
        ),
    )
    .await
    .expect("WireGuard inbound UDP round trip timed out");
    assert_eq!(echoed, payload);

    let continuity_echo = TcpListener::bind((host_ip, 0)).await.unwrap();
    let continuity_port = continuity_echo.local_addr().unwrap().port();
    let continuity_task = tokio::spawn(async move {
        let (mut stream, _) = continuity_echo.accept().await.unwrap();
        for _ in 0..2 {
            let mut data = [0_u8; 6];
            stream.read_exact(&mut data).await.unwrap();
            stream.write_all(&data).await.unwrap();
        }
    });
    let mut continuity = timeout(
        Duration::from_secs(15),
        socks5_connect(socks_port, host_ip, continuity_port),
    )
    .await
    .expect("WireGuard TCP connection before reload timed out");
    continuity.write_all(b"before").await.unwrap();
    let mut reply = [0_u8; 6];
    timeout(Duration::from_secs(15), continuity.read_exact(&mut reply))
        .await
        .expect("WireGuard TCP response before reload timed out")
        .unwrap();
    assert_eq!(&reply, b"before");

    let mut same_port_reload = server_config.clone();
    if let zero_config::InboundProtocolConfig::Wireguard { mtu, .. } =
        &mut same_port_reload.inbounds[0].protocol
    {
        *mtu = 1_300;
    }
    server_handle
        .apply_config_and_wait(same_port_reload, Duration::from_secs(5))
        .await
        .expect("same-port WireGuard reload");
    continuity.write_all(b"after!").await.unwrap();
    timeout(Duration::from_secs(15), continuity.read_exact(&mut reply))
        .await
        .expect("WireGuard TCP response after reload timed out")
        .unwrap();
    assert_eq!(&reply, b"after!");
    timeout(Duration::from_secs(15), continuity_task)
        .await
        .expect("WireGuard TCP echo task timed out")
        .unwrap();
    drop(continuity);
    // The authenticated peer also starts a new UDP flow on the same socket.
    let after_update = b"wireguard-inbound-same-port-update";
    let echoed = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_to(
            socks_port,
            Address::Ipv4(host_ip.octets()),
            udp_port,
            after_update,
        ),
    )
    .await
    .expect("WireGuard inbound same-port update lost its listener");
    assert_eq!(echoed, after_update);

    let occupied = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let mut failed_reload = server_config;
    failed_reload.inbounds[0].listen.port = occupied.local_addr().unwrap().port();
    assert!(server_handle
        .apply_config_and_wait(failed_reload, Duration::from_secs(5))
        .await
        .is_err());
    let after_rollback = b"wireguard-inbound-still-works";
    let echoed = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_to(
            socks_port,
            Address::Ipv4(host_ip.octets()),
            udp_port,
            after_rollback,
        ),
    )
    .await
    .expect("WireGuard inbound did not recover its old listener after failed reload");
    assert_eq!(echoed, after_rollback);
    drop(occupied);

    let tcp_echo = TcpListener::bind((host_ip, 0)).await.unwrap();
    let tcp_port = tcp_echo.local_addr().unwrap().port();
    let tcp_payload = b"zero-to-zero-wireguard-tcp".repeat(256);
    let tcp_len = tcp_payload.len();
    let tcp_task = tokio::spawn(async move {
        let (mut stream, _) = tcp_echo.accept().await.unwrap();
        let mut data = vec![0; tcp_len];
        stream.read_exact(&mut data).await.unwrap();
        stream.write_all(&data).await.unwrap();
    });
    timeout(Duration::from_secs(20), async {
        let mut stream = socks5_connect(socks_port, host_ip, tcp_port).await;
        stream.write_all(&tcp_payload).await.unwrap();
        let mut echoed = vec![0; tcp_payload.len()];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(echoed, tcp_payload);
    })
    .await
    .expect("WireGuard inbound TCP round trip timed out");

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    assert!(
        UdpSocket::bind((Ipv4Addr::LOCALHOST, server_port))
            .await
            .is_ok(),
        "WireGuard inbound shutdown must release its UDP listener"
    );
    udp_task.await.unwrap();
    tcp_task.await.unwrap();
}

async fn socks5_connect(socks_port: u16, host_ip: Ipv4Addr, target_port: u16) -> TcpStream {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, socks_port))
        .await
        .unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut greeting = [0; 2];
    stream.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 0]);
    let mut request = vec![5, 1, 0, 1];
    request.extend_from_slice(&host_ip.octets());
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut response = [0; 10];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(response[1], 0);
    stream
}

#[tokio::test]
async fn authenticated_wireguard_peer_roams_to_new_udp_source_port() {
    let server_port = free_udp_port();
    let host_ip = non_loopback_host_ipv4();
    let server_private = STANDARD.encode([81_u8; 32]);
    let client_private = STANDARD.encode([82_u8; 32]);
    let server_public = public_key(81);
    let client_public = public_key(82);
    let config = RuntimeConfig::parse(&format!(
        r#"{{
        "inbounds": [{{"tag":"wg-in", "listen":{{"address":"127.0.0.1","port":{server_port}}},
            "protocol":{{"type":"wireguard","private_key":"{server_private}",
                "peers":[{{"public_key":"{client_public}","allowed_ips":["10.0.0.2/32"]}}]}}}}],
        "outbounds":[{{"tag":"direct","protocol":{{"type":"direct"}}}}],
        "route":{{"rules":[],"final":{{"type":"route","outbound":"direct"}}}}
    }}"#
    ))
    .unwrap();
    let server = spawn_engine(Proxy::new(config).unwrap());
    sleep(Duration::from_millis(100)).await;

    let echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let echo_port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        for _ in 0..2 {
            let (size, sender) = echo.recv_from(&mut buffer).await.unwrap();
            echo.send_to(&buffer[..size], sender).await.unwrap();
        }
    });

    let peers = [PeerInput {
        public_key: &server_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &client_private,
        addresses: &["10.0.0.2/32"],
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    let mut tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
    let first = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    first
        .connect((Ipv4Addr::LOCALHOST, server_port))
        .await
        .unwrap();
    let second = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    second
        .connect((Ipv4Addr::LOCALHOST, server_port))
        .await
        .unwrap();
    assert_ne!(first.local_addr().unwrap(), second.local_addr().unwrap());

    for (socket, payload) in [
        (&first, b"before-roam".as_slice()),
        (&second, b"after-roam".as_slice()),
    ] {
        let packet = build_udp(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            IpAddr::V4(host_ip),
            40_001,
            echo_port,
            payload,
        );
        for action in tunnel.send_ip_packet(&packet).unwrap() {
            if let TunnelAction::SendNetwork(datagram) = action {
                socket.send(&datagram).await.unwrap();
            }
        }
        let reply = timeout(Duration::from_secs(10), async {
            let mut buffer = [0_u8; 2_048];
            loop {
                let size = socket.recv(&mut buffer).await.unwrap();
                for action in tunnel
                    .receive_datagram(Some(IpAddr::V4(Ipv4Addr::LOCALHOST)), &buffer[..size])
                    .unwrap()
                {
                    match action {
                        TunnelAction::SendNetwork(datagram) => {
                            socket.send(&datagram).await.unwrap();
                        }
                        TunnelAction::ReceiveIp { packet, .. } => return packet,
                    }
                }
            }
        })
        .await
        .expect("roamed peer did not receive UDP response on its current socket");
        assert_eq!(parse_udp(&reply).unwrap().payload, payload);
    }

    timeout(Duration::from_secs(10), echo_task)
        .await
        .unwrap()
        .unwrap();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn outbound_peer_uses_authenticated_new_udp_source_port() {
    let first = UdpSocket::bind((Ipv4Addr::LOCALHOST, free_udp_port()))
        .await
        .unwrap();
    let second = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let socks_port = free_port();
    let first_port = first.local_addr().unwrap().port();
    let client_private = STANDARD.encode([91_u8; 32]);
    let server_private = STANDARD.encode([92_u8; 32]);
    let client_public = public_key(91);
    let server_public = public_key(92);
    let config = RuntimeConfig::parse(&format!(
        r#"{{
        "inbounds":[{{"tag":"socks","listen":{{"address":"127.0.0.1","port":{socks_port}}},"protocol":{{"type":"socks5"}}}}],
        "outbounds":[{{"tag":"wg","protocol":{{"type":"wireguard","private_key":"{client_private}",
            "addresses":["10.0.0.2/32"],"peers":[{{"public_key":"{server_public}",
            "endpoint":"127.0.0.1:{first_port}","allowed_ips":["10.0.0.1/32"]}}]}}}}],
        "route":{{"rules":[],"final":{{"type":"route","outbound":"wg"}}}}
    }}"#
    ))
    .unwrap();
    let client = spawn_engine(Proxy::new(config).unwrap());
    wait_for_listener(socks_port).await;

    let peers = [PeerInput {
        public_key: &client_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["10.0.0.2/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &server_private,
        addresses: &["10.0.0.1/32"],
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    let mut server = PeerTunnel::from_validated(&profile, 0).unwrap();
    let mut wire = [0_u8; 2_048];
    let (size, client_endpoint) = timeout(Duration::from_secs(5), first.recv_from(&mut wire))
        .await
        .unwrap()
        .unwrap();
    let handshake = server
        .receive_datagram(Some(client_endpoint.ip()), &wire[..size])
        .unwrap();
    for action in handshake {
        if let TunnelAction::SendNetwork(packet) = action {
            second.send_to(&packet, client_endpoint).await.unwrap();
        }
    }
    sleep(Duration::from_millis(40)).await;
    let forged = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    forged.send_to(&[4_u8; 32], client_endpoint).await.unwrap();
    sleep(Duration::from_millis(40)).await;

    let request = tokio::spawn(async move {
        socks5_udp_echo_to(
            socks_port,
            Address::Ipv4([10, 0, 0, 1]),
            50_001,
            b"outbound-roaming",
        )
        .await
    });
    timeout(Duration::from_secs(10), async {
        loop {
            let (size, sender) = second.recv_from(&mut wire).await.unwrap();
            assert_eq!(sender, client_endpoint);
            for action in server
                .receive_datagram(Some(sender.ip()), &wire[..size])
                .unwrap()
            {
                match action {
                    TunnelAction::SendNetwork(packet) => {
                        second.send_to(&packet, sender).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => {
                        let udp = parse_udp(&packet).unwrap();
                        assert_eq!(udp.payload, b"outbound-roaming");
                        let reply = build_udp(
                            udp.dst.ip,
                            udp.src.ip,
                            udp.dst.port,
                            udp.src.port,
                            udp.payload,
                        );
                        for action in server.send_ip_packet(&reply).unwrap() {
                            if let TunnelAction::SendNetwork(packet) = action {
                                second.send_to(&packet, sender).await.unwrap();
                            }
                        }
                        return;
                    }
                }
            }
        }
    })
    .await
    .expect("authenticated endpoint did not receive outbound payload");
    assert_eq!(
        timeout(Duration::from_secs(10), request)
            .await
            .unwrap()
            .unwrap(),
        b"outbound-roaming"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires live ICMP sockets"]
async fn wireguard_inbound_returns_real_direct_icmp_echo_reply() {
    let server_port = free_udp_port();
    let server_private = STANDARD.encode([71_u8; 32]);
    let client_private = STANDARD.encode([72_u8; 32]);
    let server_public = public_key(71);
    let client_public = public_key(72);
    let server_config = RuntimeConfig::parse(&format!(
        r#"{{
        "inbounds": [{{
            "tag": "wg-in", "listen": {{"address": "127.0.0.1", "port": {server_port}}},
            "protocol": {{"type": "wireguard", "private_key": "{server_private}",
                "peers": [{{"public_key": "{client_public}", "allowed_ips": ["10.0.0.2/32"]}}]}}
        }}],
        "outbounds": [{{"tag": "direct", "protocol": {{"type": "direct"}}}}],
        "route": {{"rules": [], "final": {{"type": "route", "outbound": "direct"}}}}
    }}"#
    ))
    .unwrap();
    let server = spawn_engine(Proxy::new(server_config).unwrap());
    sleep(Duration::from_millis(100)).await;

    let peers = [PeerInput {
        public_key: &server_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["0.0.0.0/0"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let addresses = ["10.0.0.2/32"];
    let profile = validate_outbound(OutboundInput {
        private_key: &client_private,
        addresses: &addresses,
        mtu: 1_420,
        peers: &peers,
    })
    .unwrap();
    let mut tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    socket
        .connect((Ipv4Addr::LOCALHOST, server_port))
        .await
        .unwrap();

    let source = Ipv4Addr::new(10, 0, 0, 2);
    let destination = Ipv4Addr::LOCALHOST;
    let mut packet = vec![0_u8; 20 + 8 + 9];
    packet[0] = 0x45;
    let packet_len = packet.len() as u16;
    packet[2..4].copy_from_slice(&packet_len.to_be_bytes());
    packet[8] = 64;
    packet[9] = 1;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet[20] = 8;
    packet[24..26].copy_from_slice(&0x1234_u16.to_be_bytes());
    packet[26..28].copy_from_slice(&1_u16.to_be_bytes());
    packet[28..].copy_from_slice(b"wg-ping!!");
    let icmp_checksum = checksum(&packet[20..]);
    packet[22..24].copy_from_slice(&icmp_checksum.to_be_bytes());
    let ip_checksum = checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());

    for action in tunnel.send_ip_packet(&packet).unwrap() {
        if let TunnelAction::SendNetwork(datagram) = action {
            socket.send(&datagram).await.unwrap();
        }
    }
    let reply = timeout(Duration::from_secs(10), async {
        let mut buffer = [0_u8; 2048];
        'receive: loop {
            let size = socket.recv(&mut buffer).await.unwrap();
            for action in tunnel
                .receive_datagram(Some(IpAddr::V4(destination)), &buffer[..size])
                .unwrap()
            {
                match action {
                    TunnelAction::SendNetwork(datagram) => {
                        socket.send(&datagram).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => break 'receive packet,
                }
            }
        }
    })
    .await
    .expect("WireGuard ICMP echo round trip timed out");
    assert_eq!(&reply[12..16], &destination.octets());
    assert_eq!(&reply[16..20], &source.octets());
    assert_eq!(reply[20], 0);
    assert_eq!(&reply[24..], &packet[24..]);
    assert_eq!(checksum(&reply[20..]), 0);

    server.shutdown().await.unwrap();
}
