//! The peer keeps one Noise session across actual host replacement/recovery.
use super::*;
use zero_api::{TrafficGetQuery, TrafficPlane, TrafficScope};

fn outer_drops(control: &ProxyHandle) -> u64 {
    let QueryResponse::TrafficStat(snapshot) = control
        .query(QueryRequest::TrafficStat(TrafficGetQuery {
            scope: TrafficScope::Endpoint {
                endpoint_id: "endpoint:wg".into(),
            },
        }))
        .unwrap()
    else {
        panic!("actual endpoint traffic required")
    };
    snapshot
        .planes
        .iter()
        .find(|plane| plane.plane == TrafficPlane::Outer)
        .unwrap()
        .counters
        .dropped_packets
        .expect("actual outer drop counter")
}

fn source(control: &ProxyHandle) -> serde_json::Value {
    let QueryResponse::EndpointDetails(details) = control
        .query(QueryRequest::EndpointDetails(EndpointGetQuery {
            endpoint_id: "endpoint:wg".into(),
        }))
        .unwrap()
    else {
        panic!("actual endpoint details required")
    };
    details.details["peers"][0]["authenticated_endpoint"].clone()
}

fn data_counter(bytes: &[u8]) -> Option<u64> {
    (bytes.len() > 32 && bytes[..4] == 4_u32.to_le_bytes())
        .then(|| u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
}

async fn send_actions(
    socket: &UdpSocket,
    actions: Vec<TunnelAction>,
    ciphertext: &mut Option<Vec<u8>>,
) {
    for action in actions {
        if let TunnelAction::SendNetwork(datagram) = action {
            if data_counter(&datagram).is_some() {
                *ciphertext = Some(datagram.to_vec());
            }
            socket.send(&datagram).await.unwrap();
        }
    }
}

async fn exchange(
    socket: &UdpSocket,
    tunnel: &mut PeerTunnel,
    host: Ipv4Addr,
    echo: u16,
    payload: &[u8],
) -> (Vec<u8>, u64) {
    let packet = build_udp(
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
        IpAddr::V4(host),
        40001,
        echo,
        payload,
    );
    let mut ciphertext = None;
    send_actions(
        socket,
        tunnel.send_ip_packet(&packet).unwrap(),
        &mut ciphertext,
    )
    .await;
    timeout(Duration::from_secs(10), async {
        let mut buffer = [0; 2048];
        loop {
            let size = socket.recv(&mut buffer).await.unwrap();
            let counter = data_counter(&buffer[..size]);
            for action in tunnel.receive_datagram(None, &buffer[..size]).unwrap() {
                match action {
                    TunnelAction::SendNetwork(datagram) => {
                        if data_counter(&datagram).is_some() {
                            ciphertext = Some(datagram.to_vec());
                        }
                        socket.send(&datagram).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => {
                        assert_eq!(parse_udp(&packet).unwrap().payload, payload);
                        return (
                            ciphertext.take().expect("actual encrypted request"),
                            counter.expect("actual encrypted response counter"),
                        );
                    }
                }
            }
        }
    })
    .await
    .expect("same Noise session must survive actual host replacement")
}

#[tokio::test]
async fn same_private_port_move_keeps_noise_counters_replay_history_and_authenticated_source() {
    exercise(false).await;
}

#[tokio::test]
async fn failed_port_move_restores_noise_counters_replay_history_and_authenticated_source() {
    exercise(true).await;
}

async fn exercise(fail: bool) {
    let old_guard = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let old_port = old_guard.local_addr().unwrap().port();
    let socks = free_port();
    let config = RuntimeConfig::parse(&serde_json::json!({
        "endpoints":[{"tag":"wg","directions":{"inbound":true,"outbound":false},
            "listen":{"address":"127.0.0.1","port":old_port},
            "protocol":{"type":"wireguard","private_key":STANDARD.encode([121;32]),
                "addresses":["10.0.0.1/32"],"peers":[{"public_key":public_key(122),
                    "allowed_ips":["10.0.0.2/32"]}]}}],
        "inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"direct","protocol":{"type":"direct"}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let control = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    drop(old_guard); // Release only at actual server start, not while preparing config.
    let running = spawn_engine(proxy);
    wait_for_listener(socks).await;
    let server_public = public_key(121);
    let client_private = STANDARD.encode([122; 32]);
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
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let mut tunnel = PeerTunnel::from_validated(&profile, 0).unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket
        .connect((Ipv4Addr::LOCALHOST, old_port))
        .await
        .unwrap();
    let echo = UdpSocket::bind((non_loopback_host_ipv4(), 0))
        .await
        .unwrap();
    let echo_address = echo.local_addr().unwrap();
    let IpAddr::V4(host) = echo_address.ip() else {
        unreachable!()
    };
    let echo_task = tokio::spawn(async move {
        let mut bytes = [0; 256];
        for payload in [
            b"before-host-transition".as_slice(),
            b"after-host-transition".as_slice(),
        ] {
            let (size, peer) = echo.recv_from(&mut bytes).await.unwrap();
            assert_eq!(
                &bytes[..size],
                payload,
                "old ciphertext cannot become a second business packet"
            );
            echo.send_to(&bytes[..size], peer).await.unwrap();
        }
    });
    let (old_ciphertext, old_counter) = exchange(
        &socket,
        &mut tunnel,
        host,
        echo_address.port(),
        b"before-host-transition",
    )
    .await;
    let authenticated = socket.local_addr().unwrap().to_string();
    assert_eq!(source(&control), authenticated);
    let new_guard = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let new_port = new_guard.local_addr().unwrap().port();
    // Reparse the canonical source so derived legacy roles do not mask changes.
    let mut changed = serde_json::to_value(&config).unwrap();
    changed["endpoints"][0]["listen"]["port"] = serde_json::json!(new_port);
    let changed = RuntimeConfig::parse(&changed.to_string()).unwrap();
    if fail {
        let error = control
            .apply_config_and_wait(changed, Duration::from_secs(5))
            .await
            .expect_err("occupied candidate must fail without replacing last-good state");
        eprintln!("expected failed WireGuard port move: {error}");
    } else {
        drop(new_guard);
        control
            .apply_config_and_wait(changed, Duration::from_secs(5))
            .await
            .unwrap();
    }
    let active_port = if fail { old_port } else { new_port };
    let old_probe = if fail {
        None
    } else {
        Some(
            UdpSocket::bind((Ipv4Addr::LOCALHOST, old_port))
                .await
                .expect("original native listener actually closed"),
        )
    };
    assert_eq!(source(&control), authenticated);
    socket
        .connect((Ipv4Addr::LOCALHOST, active_port))
        .await
        .unwrap();
    let forged = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let before_drops = outer_drops(&control);
    forged
        .send_to(&old_ciphertext, (Ipv4Addr::LOCALHOST, active_port))
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while outer_drops(&control) <= before_drops {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original replay must be actually processed and rejected");
    assert_eq!(
        source(&control),
        authenticated,
        "replay from another source cannot roam the peer"
    );
    let (next_ciphertext, next_counter) = exchange(
        &socket,
        &mut tunnel,
        host,
        echo_address.port(),
        b"after-host-transition",
    )
    .await;
    assert!(data_counter(&next_ciphertext).unwrap() > data_counter(&old_ciphertext).unwrap());
    assert!(
        next_counter > old_counter,
        "server must retain the actual transport key/counter, not rehandshake"
    );
    assert_eq!(source(&control), authenticated);
    timeout(Duration::from_secs(3), echo_task)
        .await
        .unwrap()
        .unwrap();
    drop(old_probe);
    running.shutdown().await.unwrap();
}
