//! Real shared-listener sends must honor neutral physical-egress admission.
use super::*;

#[tokio::test]
async fn shared_listener_rejects_unavailable_egress_and_recovers_without_rebinding() {
    exercise_listener(false).await;
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn shared_listener_specific_ipv4_bind_honors_forced_egress_and_recovers() {
    exercise_listener(true).await;
}

async fn exercise_listener(specific: bool) {
    let host = non_loopback_host_ipv4();
    let remote = UdpSocket::bind((host, 0)).await.unwrap();
    let port = free_udp_port();
    let socks = free_port();
    let private = STANDARD.encode([121; 32]);
    let public = public_key(122);
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds": [
            {"tag":"wg-in", "listen":{"address":if specific { host.to_string() } else { "0.0.0.0".into() }, "port":port},
                "protocol":{"type":"wireguard", "private_key":private,
                    "peers":[{"public_key":public, "allowed_ips":["10.211.0.2/32"]}]}},
            {"tag":"socks", "listen":{"address":"127.0.0.1", "port":socks}, "protocol":{"type":"socks5"}}
        ],
        "outbounds":[{"tag":"direct", "protocol":{"type":"direct"}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let egress = proxy.egress_interface_control();
    egress.replace_tunnel_addresses(["10.0.0.1".parse().unwrap()]);
    egress.mark_unavailable_for(false, "physical_link_lost");
    let running = spawn_engine(proxy);
    wait_for_listener(socks).await;
    let server = SocketAddr::new(IpAddr::V4(host), port);
    let client_private = STANDARD.encode([122; 32]);
    let server_public = public_key(121);
    let peers = [PeerInput {
        public_key: &server_public,
        pre_shared_key: None,
        endpoint: "127.0.0.1:51820",
        allowed_ips: &["10.211.0.1/32"],
        keepalive_secs: 0,
        reserved: &[],
    }];
    let profile = validate_outbound(OutboundInput {
        private_key: &client_private,
        addresses: &["10.211.0.2/32"],
        mtu: 1420,
        peers: &peers,
    })
    .unwrap();
    let packet = build_udp(
        "10.211.0.2".parse().unwrap(),
        "10.211.0.1".parse().unwrap(),
        1234,
        9,
        b"probe",
    );
    let mut bytes = [0; 2048];
    let mut rejected = PeerTunnel::from_validated(&profile, 0).unwrap();
    for action in rejected.send_ip_packet(&packet).unwrap() {
        if let TunnelAction::SendNetwork(bytes) = action {
            remote.send_to(&bytes, server).await.unwrap();
        }
    }
    assert!(timeout(Duration::from_millis(150), remote.recv_from(&mut bytes)).await.is_err(),
        "shared listener must not send an unbound handshake response when TUN egress is unavailable");
    // A fresh initiation has a newer authenticated timestamp; no replay of the
    // previous initiation or endpoint restart is required for this transition.
    egress.clear();
    #[cfg(target_os = "macos")]
    if specific {
        let selection = zero_platform_tokio::EgressInterfaceControl::default()
            .select_for_peer_with_policy(
                server,
                &zero_traits::DialPolicy {
                    interface: Some("lo0".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        // A private controller forces the same platform mechanism as an active
        // TUN. The host route table and installed kernel remain untouched.
        egress.replace(Some(selection.interface().unwrap().clone()));
        assert!(egress
            .select_for_peer(remote.local_addr().unwrap())
            .interface()
            .is_some());
    }
    let mut recovered = PeerTunnel::from_validated(&profile, 0).unwrap();
    for action in recovered.send_ip_packet(&packet).unwrap() {
        if let TunnelAction::SendNetwork(bytes) = action {
            remote.send_to(&bytes, server).await.unwrap();
        }
    }
    let (size, source) = timeout(Duration::from_secs(2), remote.recv_from(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        source.port(),
        port,
        "listen port must survive network recovery"
    );
    assert!(
        recovered
            .receive_datagram_with_authentication(Some(source), &bytes[..size])
            .unwrap()
            .authenticated
    );
    running.shutdown().await.unwrap();
}
