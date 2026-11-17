use super::*;
pub(super) async fn pair(
    carrier: serde_json::Value,
    tail: Option<serde_json::Value>,
) -> (
    crate::Proxy,
    crate::RunningProxy,
    std::net::SocketAddr,
    crate::Proxy,
    crate::RunningProxy,
) {
    pair_carriers(carrier.clone(), carrier, tail).await
}
pub(super) async fn pair_carriers(
    incoming: serde_json::Value,
    outgoing: serde_json::Value,
    tail: Option<serde_json::Value>,
) -> (
    crate::Proxy,
    crate::RunningProxy,
    std::net::SocketAddr,
    crate::Proxy,
    crate::RunningProxy,
) {
    pair_with_bridge_graph(incoming, outgoing, tail, false).await
}
pub(super) async fn pair_with_bridge_graph(
    incoming: serde_json::Value,
    outgoing: serde_json::Value,
    tail: Option<serde_json::Value>,
    unrelated_branch: bool,
) -> (
    crate::Proxy,
    crate::RunningProxy,
    std::net::SocketAddr,
    crate::Proxy,
    crate::RunningProxy,
) {
    let mut inbound =
        serde_json::json!({"type":"vless","users":[{"id":ID,"reverse_tag":"portal"}]});
    inbound
        .as_object_mut()
        .unwrap()
        .extend(incoming.as_object().unwrap().clone());
    let mut outbounds =
        vec![serde_json::json!({"tag":"portal","protocol":{"type":"vless_reverse"}})];
    let target = if let Some(tail) = tail {
        outbounds.push(serde_json::json!({"tag":"last","protocol":tail}));
        "chain"
    } else {
        "portal"
    };
    let (portal,portal_running,wire)=framed::start(serde_json::json!({
        "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":inbound},{"tag":"clients","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
        "outbounds":outbounds,
        "outbound_groups":if target=="chain"{serde_json::json!([{"tag":"chain","type":"relay","proxies":["portal","last"]}])}else{serde_json::json!([])},
        "route":{"final":{"type":"route","outbound":target}}
    })).await;
    let clients = portal
        .system_tun_source_registry()
        .owner("clients")
        .unwrap();
    clients.wait_ready().await.unwrap();
    let socks = *clients.original_listener_address.get().unwrap();
    let mut outbound = serde_json::json!({"type":"vless","server":"127.0.0.1","port":wire.port(),"id":ID,"reverse_tag":"return"});
    outbound
        .as_object_mut()
        .unwrap()
        .extend(outgoing.as_object().unwrap().clone());
    let bridge_outbounds = if unrelated_branch {
        serde_json::json!([
            {"tag":"wire","protocol":outbound},
            {"tag":"plain","protocol":{"type":"direct"}}
        ])
    } else {
        serde_json::json!([{"tag":"wire","protocol":outbound}])
    };
    // This unused business alias used to demote wire's shared owner and reject
    // its separate active Source. The Source itself opens the direct wire node.
    let groups = if unrelated_branch {
        serde_json::json!([
            {"tag":"business","type":"relay","proxies":["wire","plain"]},
            {"tag":"choice","type":"selector","outbounds":["plain","business"],"selected":"plain"}
        ])
    } else {
        serde_json::json!([])
    };
    let (bridge,bridge_running,_)=framed::start(serde_json::json!({
        "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
        "outbounds":bridge_outbounds,"outbound_groups":groups,"route":{"final":{"type":"direct"}}
    })).await;
    let plan = portal.tcp_runtime_services().components.clone().unwrap();
    let slot = plan.configured("portal").unwrap();
    let provider = slot.accepted_carrier_provider().unwrap();
    // The production entry must select this genuine accepted-source provider;
    // a virtual reverse owner has no Packet device or native node endpoint.
    assert!(!plan.has_device_carrier_provider(slot.role_id()));
    assert!(!slot
        .recipe
        .spec()
        .roles
        .contains(&zero_platform_tokio::contract::component::RoleKind::Packet));
    timeout(Duration::from_secs(5), async {
        while provider.originals().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(plan.has_stream_loan_provider(slot.role_id()));
    assert!(plan.has_background_carrier_pool(slot.role_id()));
    (portal, portal_running, socks, bridge, bridge_running)
}
pub(super) async fn empty_and_multiple_targets(socks: std::net::SocketAddr) {
    let mut control = framed::authenticate(socks).await;
    control
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    let mut reply = [0; 10];
    control.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply[..4], &[5, 0, 0, 1]);
    let relay =
        std::net::SocketAddr::from(([127, 0, 0, 1], u16::from_be_bytes([reply[8], reply[9]])));
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    // Retain both originals until their actual worker owns the bound socket.
    let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let targets = [first.local_addr().unwrap(), second.local_addr().unwrap()];
    let echo = |socket: UdpSocket| {
        tokio::spawn(async move {
            let mut bytes = [0; 128];
            for _ in 0..2 {
                let (n, peer) = socket.recv_from(&mut bytes).await.unwrap();
                socket.send_to(&bytes[..n], peer).await.unwrap();
            }
        })
    };
    let workers = [echo(first), echo(second)];
    for target in targets {
        for payload in [b"".as_slice(), b"rvs".as_slice()] {
            let mut packet = vec![0, 0, 0, 1, 127, 0, 0, 1];
            packet.extend(target.port().to_be_bytes());
            packet.extend(payload);
            client.send_to(&packet, relay).await.unwrap();
            let mut bytes = [0; 128];
            let n = client.recv(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..n], packet);
        }
    }
    drop(control);
    for worker in workers {
        worker.await.unwrap();
    }
}
