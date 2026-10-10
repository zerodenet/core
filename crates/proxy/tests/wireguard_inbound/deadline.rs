//! Exercise the real linked listener with a long deadline and no business I/O.
use super::*;

#[tokio::test]
async fn shared_listener_reloads_timer_configuration_without_waiting_for_maintenance() {
    let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let wg_port = free_udp_port();
    let socks = free_port();
    let private = STANDARD.encode([91; 32]);
    let public = public_key(92);
    let config = RuntimeConfig::parse(&serde_json::json!({
        "inbounds": [
            {"tag":"wg-in", "listen":{"address":"127.0.0.1", "port":wg_port},
                "protocol":{"type":"wireguard", "private_key":private,
                    "peers":[{"public_key":public, "allowed_ips":["10.0.0.2/32"], "keepalive_secs":0}]}},
            {"tag":"socks-in", "listen":{"address":"127.0.0.1", "port":socks},
                "protocol":{"type":"socks5"}}
        ],
        "outbounds": [
            {"tag":"direct", "protocol":{"type":"direct"}},
            {"tag":"wg-out", "protocol":{"type":"wireguard", "private_key":private,
                "inbound_tag":"wg-in", "addresses":["10.0.0.1/32"],
                "peers":[{"public_key":public, "endpoint":remote.local_addr().unwrap().to_string(),
                    "allowed_ips":["10.0.0.2/32"], "keepalive_secs":0}]}}
        ],
        "route":{"final":{"type":"route", "outbound":"direct"}}
    }).to_string()).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let running = spawn_engine(proxy);
    wait_for_listener(socks).await;
    let mut buffer = [0; 2048];
    assert!(
        timeout(Duration::from_millis(150), remote.recv_from(&mut buffer))
            .await
            .is_err()
    );
    let mut changed = config;
    if let zero_config::InboundProtocolConfig::Wireguard { peers, .. } =
        &mut changed.inbounds[0].protocol
    {
        peers[0].keepalive_secs = 1;
    }
    if let zero_config::OutboundProtocolConfig::Wireguard { peers, .. } =
        &mut changed.outbounds[1].protocol
    {
        peers[0].keepalive_secs = 1;
    }
    handle
        .apply_config_and_wait(changed, Duration::from_secs(5))
        .await
        .unwrap();
    let (size, source) = timeout(Duration::from_secs(2), remote.recv_from(&mut buffer))
        .await
        .expect(
            "configuration watch must wake the shared listener before its five-second maintenance",
        )
        .unwrap();
    assert_eq!(size, 148);
    assert_eq!(source.port(), wg_port);
    assert_eq!(&buffer[..4], &1_u32.to_le_bytes());
    running.shutdown().await.unwrap();
}
