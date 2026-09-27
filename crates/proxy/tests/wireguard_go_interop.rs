#![cfg(all(feature = "wireguard", feature = "socks5"))]

mod support;

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    time::{sleep, timeout},
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;

use support::interop::{require_env, socks5_udp_echo_to, ExternalProcess, TempMaterial};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

fn key_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn public_key(secret: u8) -> [u8; 32] {
    *PublicKey::from(&StaticSecret::from([secret; 32])).as_bytes()
}

async fn tcp_echo(proxy_port: u16, target: IpAddr, port: u16, payload: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, proxy_port))
        .await
        .unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0];
    match target {
        IpAddr::V4(ip) => {
            request.push(1);
            request.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            request.push(4);
            request.extend_from_slice(&ip.octets());
        }
    }
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0; 4];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0, "SOCKS CONNECT failed: {reply:?}");
    let address_len = match reply[3] {
        1 => 4,
        4 => 16,
        3 => {
            let mut len = [0];
            stream.read_exact(&mut len).await.unwrap();
            len[0] as usize
        }
        atyp => panic!("unexpected SOCKS address type: {atyp}"),
    };
    let mut ignored = vec![0; address_len + 2];
    stream.read_exact(&mut ignored).await.unwrap();
    stream.write_all(payload).await.unwrap();
    let mut echoed = vec![0; payload.len()];
    stream.read_exact(&mut echoed).await.unwrap();
    echoed
}

async fn tcp_echo_domain(proxy_port: u16, domain: &str, port: u16, payload: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, proxy_port))
        .await
        .unwrap();
    stream.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    stream.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 3, u8::try_from(domain.len()).unwrap()];
    request.extend_from_slice(domain.as_bytes());
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await.unwrap();
    let mut reply = [0; 4];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply[1], 0, "SOCKS domain CONNECT failed: {reply:?}");
    let address_len = match reply[3] {
        1 => 4,
        4 => 16,
        3 => {
            let mut len = [0];
            stream.read_exact(&mut len).await.unwrap();
            len[0] as usize
        }
        atyp => panic!("unexpected SOCKS address type: {atyp}"),
    };
    let mut ignored = vec![0; address_len + 2];
    stream.read_exact(&mut ignored).await.unwrap();
    stream.write_all(payload).await.unwrap();
    let mut echoed = vec![0; payload.len()];
    stream.read_exact(&mut echoed).await.unwrap();
    echoed
}

#[tokio::test]
#[ignore = "requires pinned WIREGUARD_GO_BIN reference helper"]
async fn zero_linked_endpoint_interops_with_wireguard_go_ipv4_ipv6_tcp_udp() {
    let Some(reference_bin) = require_env("WIREGUARD_GO_BIN") else {
        return;
    };
    let material = TempMaterial::new("zero-wireguard-go-interop");
    let peer_port = free_udp_port();
    let echo_port = free_port();
    let socks_port = free_port();
    let zero_port = free_udp_port();
    let zero_private = STANDARD.encode([1_u8; 32]);
    let go_public = STANDARD.encode(public_key(2));
    let peer_port_string = peer_port.to_string();
    let echo_port_string = echo_port.to_string();
    let private_hex = key_hex(&[2_u8; 32]);
    let zero_public_hex = key_hex(&public_key(1));
    let mut reference = ExternalProcess::start_with_env(
        reference_bin,
        &[],
        &[
            ("WG_PRIVATE_HEX", &private_hex),
            ("WG_PEER_PUBLIC_HEX", &zero_public_hex),
            ("WG_PORT", &peer_port_string),
            ("WG_ECHO_PORT", &echo_port_string),
        ],
        &material,
        "wireguard-go",
    );
    timeout(Duration::from_secs(10), async {
        loop {
            if reference.logs().contains("READY") {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("wireguard-go did not start: {}", reference.logs()));

    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds": [
            {"tag":"socks-in", "listen":{"address":"127.0.0.1", "port":socks_port}, "protocol":{"type":"socks5"}},
            {"tag":"wg-in", "listen":{"address":"127.0.0.1", "port":zero_port},
                "protocol":{"type":"wireguard", "private_key":zero_private, "mtu":1420,
                    "peers":[{"public_key":go_public,
                        "allowed_ips":["10.77.0.2/32", "fd77::2/128"], "keepalive_secs":25}]}}
        ],
        "outbounds": [{"tag":"wg-out", "protocol":{
            "type":"wireguard", "private_key":zero_private, "inbound_tag":"wg-in",
            "addresses":["10.77.0.1/32", "fd77::1/128"], "mtu":1420,
            "peers":[{"public_key":go_public, "endpoint":format!("127.0.0.1:{peer_port}"),
                "allowed_ips":["10.77.0.2/32", "fd77::2/128"], "keepalive_secs":25}]
        }}],
        "route":{"rules":[], "final":{"type":"route", "outbound":"wg-out"}}
    }).to_string()).unwrap();
    let zero = spawn_engine(Proxy::new(config).unwrap());
    wait_for_listener(socks_port).await;
    for target in [
        IpAddr::V4(Ipv4Addr::new(10, 77, 0, 2)),
        IpAddr::V6("fd77::2".parse::<Ipv6Addr>().unwrap()),
    ] {
        let tcp_payload = b"zero-to-wireguard-go-tcp".repeat(64);
        let echoed = timeout(
            Duration::from_secs(15),
            tcp_echo(socks_port, target, echo_port, &tcp_payload),
        )
        .await
        .unwrap_or_else(|_| panic!("TCP timed out for {target}; {}", reference.logs()));
        assert_eq!(echoed, tcp_payload, "TCP payload mismatch for {target}");
        let udp_payload = b"zero-to-wireguard-go-udp".repeat(32);
        let address = match target {
            IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let echoed = timeout(
            Duration::from_secs(15),
            socks5_udp_echo_to(socks_port, address, echo_port, &udp_payload),
        )
        .await
        .unwrap_or_else(|_| panic!("UDP timed out for {target}; {}", reference.logs()));
        assert_eq!(echoed, udp_payload, "UDP payload mismatch for {target}");
    }
    let sustained_rounds = std::env::var("WG_SUSTAINED_ROUNDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    for round in 0..sustained_rounds {
        let target = if round % 2 == 0 {
            IpAddr::V4(Ipv4Addr::new(10, 77, 0, 2))
        } else {
            IpAddr::V6("fd77::2".parse().unwrap())
        };
        let payload = format!("wireguard-go-sustained-round-{round}").into_bytes();
        let tcp = timeout(
            Duration::from_secs(10),
            tcp_echo(socks_port, target, echo_port, &payload),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "sustained TCP round {round} timed out; {}",
                reference.logs()
            )
        });
        assert_eq!(tcp, payload, "sustained TCP round {round}");
        let address = match target {
            IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        let udp = timeout(
            Duration::from_secs(10),
            socks5_udp_echo_to(socks_port, address, echo_port, &payload),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "sustained UDP round {round} timed out; {}",
                reference.logs()
            )
        });
        assert_eq!(udp, payload, "sustained UDP round {round}");
        if round + 1 < sustained_rounds {
            sleep(Duration::from_secs(3)).await;
        }
    }
    zero.shutdown().await.unwrap();
    reference.kill();
}

#[tokio::test]
#[ignore = "requires pinned WIREGUARD_GO_BIN reference helper"]
async fn two_wireguard_outbounds_route_overlapping_prefixes_concurrently() {
    let Some(reference_bin) = require_env("WIREGUARD_GO_BIN") else {
        return;
    };
    let material = TempMaterial::new("zero-two-wireguard-outbounds");
    let echo_port = free_port();
    let socks_port = free_port();
    let first_port = free_udp_port();
    let mut second_port = free_udp_port();
    while second_port == first_port {
        second_port = free_udp_port();
    }
    let ports = [first_port, second_port];
    let mut peers = Vec::new();
    for (index, (zero_key, peer_key, address, local)) in [
        (1_u8, 2_u8, "10.10.0.2", "10.10.0.11/32"),
        (3_u8, 4_u8, "10.68.1.2", "10.68.1.1/32"),
    ]
    .into_iter()
    .enumerate()
    {
        let port = ports[index].to_string();
        let echo = echo_port.to_string();
        let private = key_hex(&[peer_key; 32]);
        let zero_public = key_hex(&public_key(zero_key));
        let allowed = local.to_owned();
        let peer = ExternalProcess::start_with_env(
            reference_bin.clone(),
            &[],
            &[
                ("WG_PRIVATE_HEX", &private),
                ("WG_PEER_PUBLIC_HEX", &zero_public),
                ("WG_PORT", &port),
                ("WG_ECHO_PORT", &echo),
                ("WG_ADDR4", address),
                ("WG_ADDR6", if index == 0 { "fd10::2" } else { "fd68::2" }),
                ("WG_ALLOWED4", &allowed),
                (
                    "WG_DNS_ADDR4",
                    if index == 0 { "192.168.1.180" } else { "" },
                ),
                ("WG_DNS_ANSWER4", "10.10.0.2"),
            ],
            &material,
            &format!("wireguard-go-{index}"),
        );
        timeout(Duration::from_secs(10), async {
            while !peer.logs().contains("READY") {
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("wireguard-go peer {index} did not start: {}", peer.logs()));
        peers.push(peer);
    }

    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [{"tag":"socks-in", "listen":{"address":"127.0.0.1", "port":socks_port}, "protocol":{"type":"socks5"}}],
            "outbounds": [
                {"tag":"wg-a", "protocol":{"type":"wireguard", "private_key":STANDARD.encode([1_u8; 32]),
                    "addresses":["10.10.0.11/32"], "peers":[{"public_key":STANDARD.encode(public_key(2)),
                        "endpoint":format!("127.0.0.1:{}", ports[0]),
                        "allowed_ips":["10.10.0.0/24", "192.168.0.0/23"], "keepalive_secs":25}]}},
                {"tag":"wg-b", "protocol":{"type":"wireguard", "private_key":STANDARD.encode([3_u8; 32]),
                    "addresses":["10.68.1.1/32"], "peers":[{"public_key":STANDARD.encode(public_key(4)),
                        "endpoint":format!("127.0.0.1:{}", ports[1]),
                        "allowed_ips":["10.0.0.0/8"], "keepalive_secs":25}]}}
            ],
            "route":{"auto_outbounds":["wg-b", "wg-a"], "rules":[], "final":{"type":"reject"}},
            "runtime":{"dns":{
                "servers":{
                    "system":{"type":"system"},
                    "wg-a-dns":{"type":"udp", "host":"192.168.1.180", "detour":"wg-a"}
                },
                "default_server":"system",
                "dispatch":[{"condition":{"type":"domain", "values":["office.internal.test"]},
                    "server":"wg-a-dns"}],
                "policy":{"node_server":"system", "fallback_servers":[]}
            }}
        })
        .to_string(),
    )
    .unwrap();
    let zero = spawn_engine(Proxy::new(config).unwrap());
    wait_for_listener(socks_port).await;
    for address in [Ipv4Addr::new(10, 10, 0, 2), Ipv4Addr::new(10, 68, 1, 2)] {
        let payload = format!("two-wireguard-outbounds-{address}").into_bytes();
        let target = IpAddr::V4(address);
        assert_eq!(
            timeout(
                Duration::from_secs(15),
                tcp_echo(socks_port, target, echo_port, &payload)
            )
            .await
            .unwrap(),
            payload
        );
        assert_eq!(
            timeout(
                Duration::from_secs(15),
                socks5_udp_echo_to(
                    socks_port,
                    Address::Ipv4(address.octets()),
                    echo_port,
                    &payload
                )
            )
            .await
            .unwrap(),
            payload
        );
    }
    let payload = b"wireguard-a-dns-through-udp";
    assert_eq!(
        timeout(
            Duration::from_secs(15),
            tcp_echo_domain(socks_port, "office.internal.test", echo_port, payload)
        )
        .await
        .unwrap(),
        payload
    );
    zero.shutdown().await.unwrap();
    for peer in &mut peers {
        peer.kill();
    }
}

fn non_loopback_host_ipv4() -> Ipv4Addr {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.connect("192.0.2.1:9").unwrap();
    let IpAddr::V4(address) = socket.local_addr().unwrap().ip() else {
        panic!("IPv4 required")
    };
    address
}

async fn serve_tcp_count(listener: TcpListener, count: usize) {
    for _ in 0..count {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut payload = vec![0_u8; b"wireguard-go-to-zero-payload".len()];
        stream.read_exact(&mut payload).await.unwrap();
        stream.write_all(&payload).await.unwrap();
    }
}

async fn serve_udp_count(socket: UdpSocket, count: usize) {
    let mut payload = [0_u8; 64];
    for _ in 0..count {
        let (size, sender) = socket.recv_from(&mut payload).await.unwrap();
        socket.send_to(&payload[..size], sender).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires pinned WIREGUARD_GO_BIN reference helper"]
async fn two_wireguard_go_peers_interop_with_zero_direct_inbound() {
    support::interop::init_logs("zero_proxy=trace,zero_stack=trace,wireguard=trace");
    let Some(reference_bin) = require_env("WIREGUARD_GO_BIN") else {
        return;
    };
    let material = TempMaterial::new("wireguard-go-zero-inbound");
    let host4 = non_loopback_host_ipv4();
    let tcp4 = TcpListener::bind((host4, 0)).await.unwrap();
    let port4 = tcp4.local_addr().unwrap().port();
    let udp4 = UdpSocket::bind((host4, port4)).await.unwrap();
    let tasks = [
        tokio::spawn(serve_tcp_count(tcp4, 2)),
        tokio::spawn(serve_udp_count(udp4, 2)),
    ];
    let zero_port = free_udp_port();
    let zero_private = STANDARD.encode([1_u8; 32]);
    let go_public_a = STANDARD.encode(public_key(2));
    let go_public_b = STANDARD.encode(public_key(5));
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [{"tag":"wg-in", "listen":{"address":"127.0.0.1", "port":zero_port},
                "protocol":{"type":"wireguard", "private_key":zero_private,
                    "peers":[{"public_key":go_public_a,
                        "allowed_ips":["10.77.0.2/32", "fd77::2/128"]},
                        {"public_key":go_public_b,
                        "allowed_ips":["10.77.0.3/32", "fd77::3/128"]}]}}],
            "outbounds":[{"tag":"direct", "protocol":{"type":"direct"}}],
            "route":{"rules":[], "final":{"type":"route", "outbound":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let zero = spawn_engine(Proxy::new(config).unwrap());
    sleep(Duration::from_millis(150)).await;
    let zero_public_hex = key_hex(&public_key(1));
    let zero_endpoint = format!("127.0.0.1:{zero_port}");
    let target4 = format!("{host4}:{port4}");
    for (peer, address4, address6) in [(2_u8, "10.77.0.2", "fd77::2"), (5, "10.77.0.3", "fd77::3")]
    {
        let go_port = free_udp_port().to_string();
        let go_private_hex = key_hex(&[peer; 32]);
        let mut reference = ExternalProcess::start_with_env(
            reference_bin.clone(),
            &[],
            &[
                ("WG_MODE", "client"),
                ("WG_PRIVATE_HEX", &go_private_hex),
                ("WG_PEER_PUBLIC_HEX", &zero_public_hex),
                ("WG_PORT", &go_port),
                ("WG_ECHO_PORT", "1"),
                ("WG_ENDPOINT", &zero_endpoint),
                ("WG_TARGET4", &target4),
                ("WG_ADDR4", address4),
                ("WG_ADDR6", address6),
            ],
            &material,
            &format!("wireguard-go-client-{peer}"),
        );
        let status = timeout(Duration::from_secs(45), reference.wait())
            .await
            .unwrap_or_else(|_| panic!("wireguard-go peer {peer} timed out: {}", reference.logs()))
            .unwrap();
        assert!(
            status.success(),
            "wireguard-go peer {peer} failed: {}",
            reference.logs()
        );
        assert!(reference.logs().contains("SUCCESS"));
    }
    for task in tasks {
        timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
    }
    zero.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires pinned WIREGUARD_GO_BIN reference helper"]
async fn wireguard_go_ipv4_ipv6_packets_cross_zero_between_two_peers() {
    let Some(reference_bin) = require_env("WIREGUARD_GO_BIN") else {
        return;
    };
    let material = TempMaterial::new("wireguard-go-zero-packet-route");
    let server_port = free_udp_port();
    let inbound_port = free_udp_port();
    let echo_port = free_port();
    let server_port_string = server_port.to_string();
    let echo_port_string = echo_port.to_string();
    let server_private_hex = key_hex(&[3_u8; 32]);
    let zero_outbound_public_hex = key_hex(&public_key(4));
    let server_addr4 = "10.78.0.2";
    let server_addr6 = "fd78::2";
    let mut server = ExternalProcess::start_with_env(
        reference_bin.clone(),
        &[],
        &[
            ("WG_PRIVATE_HEX", &server_private_hex),
            ("WG_PEER_PUBLIC_HEX", &zero_outbound_public_hex),
            ("WG_PORT", &server_port_string),
            ("WG_ECHO_PORT", &echo_port_string),
            ("WG_ADDR4", server_addr4),
            ("WG_ADDR6", server_addr6),
            ("WG_ALLOWED4", "10.77.0.2/32"),
            ("WG_ALLOWED6", "fd77::2/128"),
        ],
        &material,
        "wireguard-go-server",
    );
    timeout(Duration::from_secs(10), async {
        loop {
            if server.logs().contains("READY") {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("wireguard-go server did not start: {}", server.logs()));

    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "inbounds": [{"tag":"wg-in", "listen":{"address":"127.0.0.1", "port":inbound_port},
                "protocol":{"type":"wireguard", "private_key":STANDARD.encode([1_u8; 32]),
                    "peers":[{"public_key":STANDARD.encode(public_key(2)),
                        "allowed_ips":["10.77.0.2/32", "fd77::2/128"]}]}}],
            "outbounds":[{"tag":"wg-hop", "protocol":{"type":"wireguard",
                "private_key":STANDARD.encode([4_u8; 32]),
                "addresses":["10.78.0.1/32", "fd78::1/128"], "mtu":1420,
                "peers":[{"public_key":STANDARD.encode(public_key(3)),
                    "endpoint":format!("127.0.0.1:{server_port}"),
                    "allowed_ips":["10.78.0.2/32", "fd78::2/128"]}]}}],
            "route":{"rules":[], "final":{"type":"route", "outbound":"wg-hop"}}
        })
        .to_string(),
    )
    .unwrap();
    let zero = spawn_engine(Proxy::new(config).unwrap());
    sleep(Duration::from_millis(150)).await;
    let client_port = free_udp_port().to_string();
    let client_private_hex = key_hex(&[2_u8; 32]);
    let zero_inbound_public_hex = key_hex(&public_key(1));
    let zero_endpoint = format!("127.0.0.1:{inbound_port}");
    let target4 = format!("{server_addr4}:{echo_port}");
    let target6 = format!("[{server_addr6}]:{echo_port}");
    let mut client = ExternalProcess::start_with_env(
        reference_bin,
        &[],
        &[
            ("WG_MODE", "client"),
            ("WG_PRIVATE_HEX", &client_private_hex),
            ("WG_PEER_PUBLIC_HEX", &zero_inbound_public_hex),
            ("WG_PORT", &client_port),
            ("WG_ECHO_PORT", "1"),
            ("WG_ENDPOINT", &zero_endpoint),
            ("WG_TARGET4", &target4),
            ("WG_TARGET6", &target6),
        ],
        &material,
        "wireguard-go-client",
    );
    let status = timeout(Duration::from_secs(45), client.wait())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "wireguard-go client timed out: {}; server: {}",
                client.logs(),
                server.logs()
            )
        })
        .unwrap();
    assert!(
        status.success(),
        "wireguard-go client failed: {}; server: {}",
        client.logs(),
        server.logs()
    );
    assert!(client.logs().contains("SUCCESS"));
    zero.shutdown().await.unwrap();
    server.kill();
}
