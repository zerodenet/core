#![cfg(all(feature = "wireguard", feature = "socks5"))]

#[path = "support/host.rs"]
mod host;
mod support;

use host::non_loopback_host_ipv4;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{sleep, timeout, Duration},
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_api::{EndpointGetQuery, QueryRequest, QueryResponse, QueryService};
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

#[tokio::test]
async fn linked_wireguard_endpoints_share_socket_and_peer_state_in_both_directions() {
    let a_port = free_udp_port();
    let b_port = free_udp_port();
    let a_socks = free_port();
    let b_socks = free_port();
    let host_ip = non_loopback_host_ipv4();
    let a_private = STANDARD.encode([111_u8; 32]);
    let b_private = STANDARD.encode([112_u8; 32]);
    let a_public = public_key(111);
    let b_public = public_key(112);
    let peer_config = |name: &str,
                       listen: u16,
                       socks: u16,
                       private: &str,
                       remote_public: &str,
                       remote_port: u16,
                       local: &str,
                       remote: &str| {
        RuntimeConfig::parse(&format!(
            r#"{{
            "inbounds": [
                {{"tag":"wg-in","listen":{{"address":"127.0.0.1","port":{listen}}},
                  "protocol":{{"type":"wireguard","private_key":"{private}",
                    "peers":[{{"public_key":"{remote_public}",
                      "allowed_ips":["{remote}/32","{host_ip}/32"],"keepalive_secs":1}}]}}}},
                {{"tag":"socks-in","listen":{{"address":"127.0.0.1","port":{socks}}},
                  "protocol":{{"type":"socks5"}}}}
            ],
            "outbounds": [
                {{"tag":"direct","protocol":{{"type":"direct"}}}},
                {{"tag":"wg-out","protocol":{{"type":"wireguard",
                    "private_key":"{private}","addresses":["{local}/32"],
                    "inbound_tag":"wg-in",
                    "peers":[{{"public_key":"{remote_public}","endpoint":"127.0.0.1:{remote_port}",
                      "allowed_ips":["{remote}/32","{host_ip}/32"],"keepalive_secs":1}}]}}}}
            ],
            "route":{{"rules":[{{"condition":{{"type":"inbound","values":["socks-in"]}},
                "action":{{"type":"route","outbound":"wg-out"}}}}],
                "final":{{"type":"route","outbound":"direct"}}}}
        }}"#
        ))
        .unwrap_or_else(|error| panic!("{name} config: {error}"))
    };
    let b = spawn_engine(
        Proxy::new(peer_config(
            "b", b_port, b_socks, &b_private, &a_public, a_port, "10.0.0.2", "10.0.0.1",
        ))
        .unwrap(),
    );
    wait_for_listener(b_socks).await;
    let a_config = peer_config(
        "a", a_port, a_socks, &a_private, &b_public, b_port, "10.0.0.1", "10.0.0.2",
    );
    let a_proxy = Proxy::new(a_config.clone()).unwrap();
    let a_handle = ProxyHandle::new(EngineHandle::new(a_proxy.engine().clone()), a_proxy.clone());
    let a = spawn_engine(a_proxy);
    wait_for_listener(a_socks).await;

    let echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        for _ in 0..2 {
            let (size, source) = echo.recv_from(&mut buffer).await.unwrap();
            echo.send_to(&buffer[..size], source).await.unwrap();
        }
    });
    for socks in [a_socks, b_socks] {
        let payload = format!("linked-wireguard-{socks}").repeat(80);
        let reply = timeout(
            Duration::from_secs(20),
            socks5_udp_echo_to(
                socks,
                Address::Ipv4(host_ip.octets()),
                port,
                payload.as_bytes(),
            ),
        )
        .await
        .expect("linked endpoint UDP timed out");
        assert_eq!(reply, payload.as_bytes());
    }
    echo_task.await.unwrap();

    let listener = TcpListener::bind((host_ip, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let tcp_task = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut data = [0_u8; 8];
            stream.read_exact(&mut data).await.unwrap();
            stream.write_all(&data).await.unwrap();
        }
    });
    for socks in [a_socks, b_socks] {
        timeout(Duration::from_secs(20), async {
            let mut stream = socks5_connect(socks, host_ip, port).await;
            stream.write_all(b"duplex!!").await.unwrap();
            let mut reply = [0_u8; 8];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"duplex!!");
        })
        .await
        .expect("linked endpoint TCP timed out");
    }
    tcp_task.await.unwrap();

    let mut changed_endpoint = a_config.clone();
    if let zero_config::InboundProtocolConfig::Wireguard { peers, .. } =
        &mut changed_endpoint.inbounds[0].protocol
    {
        peers[0].keepalive_secs = 2;
    }
    if let zero_config::OutboundProtocolConfig::Wireguard { peers, .. } =
        &mut changed_endpoint.outbounds[1].protocol
    {
        peers[0].keepalive_secs = 2;
    }
    let active_config = changed_endpoint.clone();
    assert!(a_handle
        .apply_config_and_wait(changed_endpoint, Duration::from_secs(5))
        .await
        .is_ok());
    let echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        let (size, source) = echo.recv_from(&mut buffer).await.unwrap();
        echo.send_to(&buffer[..size], source).await.unwrap();
    });
    let reply = timeout(
        Duration::from_secs(20),
        socks5_udp_echo_to(
            a_socks,
            Address::Ipv4(host_ip.octets()),
            port,
            b"after-rejected-reload",
        ),
    )
    .await
    .expect("linked endpoint lost its session after live reload");
    assert_eq!(reply, b"after-rejected-reload");
    echo_task.await.unwrap();

    let occupied = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut failed_bind = a_config.clone();
    let reserved_port = free_udp_port();
    failed_bind.inbounds[0].listen.port = reserved_port;
    failed_bind.inbounds[1].listen.port = occupied.local_addr().unwrap().port();
    assert!(a_handle
        .apply_config_and_wait(failed_bind, Duration::from_secs(5))
        .await
        .is_err());
    UdpSocket::bind(("127.0.0.1", reserved_port))
        .await
        .expect("failed candidate releases its reserved socket before acknowledgement");
    let echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        let (size, source) = echo.recv_from(&mut buffer).await.unwrap();
        echo.send_to(&buffer[..size], source).await.unwrap();
    });
    let reply = timeout(
        Duration::from_secs(20),
        socks5_udp_echo_to(
            a_socks,
            Address::Ipv4(host_ip.octets()),
            port,
            b"after-failed-bind",
        ),
    )
    .await
    .expect("linked endpoint lost its session after failed bind");
    assert_eq!(reply, b"after-failed-bind");
    echo_task.await.unwrap();

    let mut failed_path = a_config.clone();
    if let zero_config::OutboundProtocolConfig::Wireguard {
        outer_udp_proxy, ..
    } = &mut failed_path.outbounds[1].protocol
    {
        *outer_udp_proxy = Some("direct".to_owned());
    }
    assert!(a_handle
        .apply_config_and_wait(failed_path, Duration::from_secs(5))
        .await
        .is_err());
    let echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        let (size, source) = echo.recv_from(&mut buffer).await.unwrap();
        echo.send_to(&buffer[..size], source).await.unwrap();
    });
    let reply = timeout(
        Duration::from_secs(20),
        socks5_udp_echo_to(
            a_socks,
            Address::Ipv4(host_ip.octets()),
            port,
            b"after-failed-path",
        ),
    )
    .await
    .expect("linked endpoint lost its session after rejected outer path");
    assert_eq!(reply, b"after-failed-path");
    echo_task.await.unwrap();

    let mut unlinked = active_config.clone();
    unlinked
        .outbounds
        .retain(|outbound| outbound.tag != "wg-out");
    unlinked.route.rules.clear();
    a_handle
        .apply_config_and_wait(unlinked, Duration::from_secs(5))
        .await
        .expect("unlink active WireGuard endpoint");
    assert_udp_echo_via_peer(host_ip, b_socks, b"after-unlink", "unlink").await;

    let occupied = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let mut failed_relink = active_config.clone();
    failed_relink.inbounds[1].listen.port = occupied.local_addr().unwrap().port();
    assert!(a_handle
        .apply_config_and_wait(failed_relink, Duration::from_secs(5))
        .await
        .is_err());
    assert_udp_echo_via_peer(host_ip, b_socks, b"after-failed-relink", "failed relink").await;

    a_handle
        .apply_config_and_wait(active_config, Duration::from_secs(5))
        .await
        .expect("relink active WireGuard endpoint");
    assert_udp_echo_via_peer(host_ip, a_socks, b"after-relink", "relink").await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

async fn assert_udp_echo_via_peer(host_ip: Ipv4Addr, socks_port: u16, payload: &[u8], stage: &str) {
    let echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let port = echo.local_addr().unwrap().port();
    let echo_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        let (size, source) = echo.recv_from(&mut buffer).await.unwrap();
        echo.send_to(&buffer[..size], source).await.unwrap();
    });
    let reply = timeout(
        Duration::from_secs(20),
        socks5_udp_echo_to(socks_port, Address::Ipv4(host_ip.octets()), port, payload),
    )
    .await
    .unwrap_or_else(|_| panic!("WireGuard endpoint UDP timed out after {stage}"));
    assert_eq!(reply, payload);
    echo_task.await.unwrap();
}

#[tokio::test]
async fn zero_wireguard_inbound_routes_tcp_and_udp_from_zero_outbound() {
    support::interop::init_logs("wireguard=debug");
    let server_port = free_udp_port();
    let socks_port = free_port();
    let client_wg_port = free_udp_port();
    let outer_socks_port = free_port();
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
        }}, {{
            "tag": "outer-socks-in", "listen": {{"address": "127.0.0.1", "port": {outer_socks_port}}},
            "protocol": {{"type": "socks5"}}
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
    wait_for_listener(outer_socks_port).await;

    let client_config = RuntimeConfig::parse(&format!(r#"{{
        "inbounds": [{{"tag": "socks-in", "listen": {{"address": "127.0.0.1", "port": {socks_port}}}, "protocol": {{"type": "socks5"}}}},
            {{"tag": "wg-client-in", "listen": {{"address": "127.0.0.1", "port": {client_wg_port}}},
                "protocol": {{"type": "wireguard", "private_key": "{client_private}",
                    "peers": [{{"public_key": "{server_public}", "allowed_ips": ["0.0.0.0/0"]}}]}}}}],
        "outbounds": [{{"tag": "outer-socks", "protocol": {{"type": "socks5", "server": "127.0.0.1", "port": {outer_socks_port}}}}}, {{"tag": "wg-out", "protocol": {{
            "type": "wireguard", "private_key": "{client_private}", "addresses": ["10.0.0.2/32"],
            "inbound_tag": "wg-client-in",
            "outer_udp_proxy": "outer-socks",
            "peers": [{{"public_key": "{server_public}", "endpoint": "127.0.0.1:{server_port}", "allowed_ips": ["0.0.0.0/0"]}}]
        }}}}],
        "route": {{"rules": [], "final": {{"type": "route", "outbound": "wg-out"}}, "final_mode": "packet"}}
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
    let mut failed_reload = server_config.clone();
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

    server.shutdown().await.unwrap();
    sleep(Duration::from_secs(2)).await;
    let server = spawn_engine(Proxy::new(server_config).unwrap());
    wait_for_listener(outer_socks_port).await;
    let recovered_echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let recovered_port = recovered_echo.local_addr().unwrap().port();
    let recovered_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        loop {
            let (size, source) = recovered_echo.recv_from(&mut buffer).await.unwrap();
            recovered_echo
                .send_to(&buffer[..size], source)
                .await
                .unwrap();
        }
    });
    let recovered = timeout(Duration::from_secs(35), async {
        loop {
            if let Ok(response) = timeout(
                Duration::from_secs(3),
                socks5_udp_echo_to(
                    socks_port,
                    Address::Ipv4(host_ip.octets()),
                    recovered_port,
                    b"linked-outer-recovered",
                ),
            )
            .await
            {
                break response;
            }
            sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("linked outer UDP carrier did not recover after proxy restart");
    assert_eq!(recovered, b"linked-outer-recovered");
    recovered_task.abort();

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

#[tokio::test]
#[cfg(feature = "shadowsocks")]
async fn wireguard_outer_udp_uses_socks5_to_shadowsocks_relay_group() {
    let server_port = free_udp_port();
    let socks_port = free_port();
    let outer_socks_port = free_port();
    let outer_ss_port = free_port();
    let host_ip = non_loopback_host_ipv4();
    let server_private = STANDARD.encode([101_u8; 32]);
    let client_private = STANDARD.encode([102_u8; 32]);
    let server_public = public_key(101);
    let client_public = public_key(102);
    let server_config = RuntimeConfig::parse(&format!(r#"{{
        "inbounds": [{{"tag":"wg-in","listen":{{"address":"127.0.0.1","port":{server_port}}},
            "protocol":{{"type":"wireguard","private_key":"{server_private}",
                "peers":[{{"public_key":"{client_public}","allowed_ips":["10.0.0.2/32"]}}]}}}},
            {{"tag":"outer-socks-in","listen":{{"address":"127.0.0.1","port":{outer_socks_port}}},
                "protocol":{{"type":"socks5"}}}},
            {{"tag":"outer-ss-in","listen":{{"address":"127.0.0.1","port":{outer_ss_port}}},
                "protocol":{{"type":"shadowsocks","password":"outer-test-password","cipher":"aes-128-gcm"}}}}],
        "outbounds":[{{"tag":"direct","protocol":{{"type":"direct"}}}}],
        "route":{{"rules":[],"final":{{"type":"route","outbound":"direct"}}}}
    }}"#)).unwrap();
    let server = spawn_engine(Proxy::new(server_config.clone()).unwrap());
    wait_for_listener(outer_socks_port).await;
    wait_for_listener(outer_ss_port).await;

    let client_config = RuntimeConfig::parse(&format!(r#"{{
        "inbounds":[{{"tag":"socks-in","listen":{{"address":"127.0.0.1","port":{socks_port}}},
            "protocol":{{"type":"socks5"}}}}],
        "outbounds":[
            {{"tag":"outer-socks","protocol":{{"type":"socks5","server":"127.0.0.1","port":{outer_socks_port}}}}},
            {{"tag":"outer-ss","protocol":{{"type":"shadowsocks","server":"127.0.0.1","port":{outer_ss_port},
                "password":"outer-test-password","cipher":"aes-128-gcm"}}}},
            {{"tag":"wg-out","protocol":{{"type":"wireguard","private_key":"{client_private}",
                "addresses":["10.0.0.2/32"],"outer_udp_proxy":"outer-chain",
                "peers":[{{"public_key":"{server_public}","endpoint":"127.0.0.1:{server_port}",
                    "allowed_ips":["0.0.0.0/0"]}}]}}}}],
        "outbound_groups":[{{"tag":"outer-chain","type":"relay","proxies":["outer-socks","outer-ss"]}}],
        "route":{{"rules":[],"final":{{"type":"route","outbound":"wg-out"}},"final_mode":"packet"}}
    }}"#)).unwrap();
    let client = spawn_engine(Proxy::new(client_config).unwrap());
    wait_for_listener(socks_port).await;

    let udp_echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let udp_port = udp_echo.local_addr().unwrap().port();
    let udp_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        let (size, source) = udp_echo.recv_from(&mut buffer).await.unwrap();
        udp_echo.send_to(&buffer[..size], source).await.unwrap();
    });
    let payload = b"wireguard-over-relay-udp";
    let echoed = timeout(
        Duration::from_secs(20),
        socks5_udp_echo_to(
            socks_port,
            Address::Ipv4(host_ip.octets()),
            udp_port,
            payload,
        ),
    )
    .await
    .expect("outer relay WireGuard UDP timed out");
    assert_eq!(echoed, payload);

    let tcp_echo = TcpListener::bind((host_ip, 0)).await.unwrap();
    let tcp_port = tcp_echo.local_addr().unwrap().port();
    let tcp_task = tokio::spawn(async move {
        let (mut stream, _) = tcp_echo.accept().await.unwrap();
        let mut buffer = [0_u8; 64];
        let size = stream.read(&mut buffer).await.unwrap();
        stream.write_all(&buffer[..size]).await.unwrap();
    });
    timeout(Duration::from_secs(20), async {
        let mut stream = socks5_connect(socks_port, host_ip, tcp_port).await;
        stream.write_all(b"wireguard-over-relay-tcp").await.unwrap();
        let mut echoed = [0_u8; 24];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"wireguard-over-relay-tcp");
    })
    .await
    .expect("outer relay WireGuard TCP timed out");

    udp_task.await.unwrap();
    tcp_task.await.unwrap();
    server.shutdown().await.unwrap();
    // The first SOCKS5 control connection and its Shadowsocks relay disappear.
    // Keep the WireGuard outbound device alive while the outer chain returns.
    sleep(Duration::from_secs(2)).await;
    let server = spawn_engine(Proxy::new(server_config).unwrap());
    wait_for_listener(outer_socks_port).await;
    wait_for_listener(outer_ss_port).await;
    let recovered_echo = UdpSocket::bind((host_ip, 0)).await.unwrap();
    let recovered_port = recovered_echo.local_addr().unwrap().port();
    let recovered_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 256];
        loop {
            let (size, source) = recovered_echo.recv_from(&mut buffer).await.unwrap();
            recovered_echo
                .send_to(&buffer[..size], source)
                .await
                .unwrap();
        }
    });
    let recovered = timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(response) = timeout(
                Duration::from_secs(3),
                socks5_udp_echo_to(
                    socks_port,
                    Address::Ipv4(host_ip.octets()),
                    recovered_port,
                    b"outer-relay-recovered",
                ),
            )
            .await
            {
                break response;
            }
            sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .expect("outer UDP relay did not recover after restart");
    assert_eq!(recovered, b"outer-relay-recovered");

    client.shutdown().await.unwrap();
    server.shutdown().await.unwrap();
    recovered_task.abort();
    let _ = recovered_task.await;
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
    let proxy = Proxy::new(config).unwrap();
    let control = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let server = spawn_engine(proxy);
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
                    .receive_datagram(
                        Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
                        &buffer[..size],
                    )
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
        let QueryResponse::EndpointDetails(details) = control
            .query(QueryRequest::EndpointDetails(EndpointGetQuery {
                endpoint_id: "legacy:inbound:wg-in".into(),
            }))
            .unwrap()
        else {
            panic!("wrong endpoint detail response");
        };
        assert_eq!(details.details["peers"][0]["source_known"], true);
        assert_eq!(
            details.details["peers"][0]["authenticated_endpoint"],
            socket.local_addr().unwrap().to_string()
        );
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
        .receive_datagram(Some(client_endpoint), &wire[..size])
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
                .receive_datagram(Some(sender), &wire[..size])
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
                .receive_datagram(
                    Some(SocketAddr::new(IpAddr::V4(destination), 0)),
                    &buffer[..size],
                )
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
