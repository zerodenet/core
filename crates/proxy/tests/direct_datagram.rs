//! Fixed-target datagram forwarding must preserve client isolation and rollback.
#![cfg(feature = "managed-datagram-runtime")]
use crate::support;
use std::time::Duration;
use support::{free_port, spawn_engine, wait_for_listener};
use tokio::net::UdpSocket;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[tokio::test]
async fn direct_datagrams_keep_two_clients_isolated() {
    let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target_port = target.local_addr().unwrap().port();
    let port = free_port();
    let config = RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"direct","target":"127.0.0.1","port":target_port}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
    let handle = spawn_engine(Proxy::new(config).unwrap());
    wait_for_listener(port).await;
    let first = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    first.send_to(b"first", ("127.0.0.1", port)).await.unwrap();
    second
        .send_to(b"second", ("127.0.0.1", port))
        .await
        .unwrap();
    let mut buffer = [0; 64];
    let (n, peer_one) = tokio::time::timeout(Duration::from_secs(3), target.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    let one = buffer[..n].to_vec();
    let (n, peer_two) = tokio::time::timeout(Duration::from_secs(3), target.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    let two = buffer[..n].to_vec();
    assert_ne!(
        peer_one, peer_two,
        "independent clients need distinct upstream mappings"
    );
    target.send_to(&two, peer_two).await.unwrap();
    target.send_to(&one, peer_one).await.unwrap();
    for (client, expected) in [(first, b"first".as_slice()), (second, b"second".as_slice())] {
        let (n, _) = tokio::time::timeout(Duration::from_secs(3), client.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..n], expected);
    }
    handle.shutdown().await.unwrap();
    let released = UdpSocket::bind(("127.0.0.1", port)).await;
    assert!(released.is_ok(), "shutdown must release UDP listener");
}

#[tokio::test]
async fn direct_udp_occupied_port_fails_before_start() {
    let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    let config = RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":port},"protocol":{"type":"direct","target":"127.0.0.1","port":9}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap();
    let proxy = Proxy::new(config).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), proxy.run())
        .await
        .expect("bind failure should be immediate");
    assert!(
        result.is_err(),
        "must reject partial TCP-only activation when UDP is occupied"
    );
}

#[tokio::test]
async fn disabled_udp_does_not_bind_even_when_port_is_occupied() {
    for global_disable in [true, false] {
        let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = occupied.local_addr().unwrap().port();
        let config = policy_config(port, !global_disable, global_disable);
        let handle = spawn_engine(Proxy::new(config).unwrap());
        wait_for_listener(port).await;
        handle.shutdown().await.unwrap();
    }
}

fn policy_config(port: u16, global: bool, inbound: bool) -> RuntimeConfig {
    RuntimeConfig::parse(&serde_json::json!({
        "runtime":{"udp":{"enabled":global}},
        "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":port},
            "udp":{"enabled":inbound},"protocol":{"type":"direct","target":"127.0.0.1","port":9}}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}

#[tokio::test]
async fn udp_policy_reload_rebinds_and_rolls_back_on_conflict() {
    use zero_engine::EngineHandle;
    use zero_proxy::ProxyHandle;
    for global_toggle in [false, true] {
        let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = occupied.local_addr().unwrap().port();
        let disabled = policy_config(port, !global_toggle, global_toggle);
        let enabled = policy_config(port, true, true);
        let proxy = Proxy::new(disabled.clone()).unwrap();
        let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
        let running = proxy.spawn();
        wait_for_listener(port).await;
        assert!(handle
            .apply_config_and_wait(enabled.clone(), Duration::from_secs(5))
            .await
            .is_err());
        wait_for_listener(port).await;
        drop(occupied);
        let released = UdpSocket::bind(("127.0.0.1", port)).await.unwrap();
        drop(released);
        handle
            .apply_config_and_wait(enabled, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(UdpSocket::bind(("127.0.0.1", port)).await.is_err());
        handle
            .apply_config_and_wait(disabled, Duration::from_secs(5))
            .await
            .unwrap();
        // The acknowledgement is after TCP rebind; allow the UDP shutdown task to finish.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if UdpSocket::bind(("127.0.0.1", port)).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        running.shutdown().await.unwrap();
    }
}

fn dial_reload_config(first: u16, second: u16, target: u16, use_ipv6: bool) -> RuntimeConfig {
    let dial = if use_ipv6 {
        serde_json::json!({"address_family":"only_ipv6","source_ip":"::1"})
    } else {
        serde_json::json!({"address_family":"only_ipv4","source_ip":"127.0.0.1"})
    };
    RuntimeConfig::parse(&serde_json::json!({
        "inbounds":[
            {"tag":"changing-entry","listen":{"address":"127.0.0.1","port":first},"protocol":{"type":"direct","target":"localhost","port":target}},
            {"tag":"stable-entry","listen":{"address":"127.0.0.1","port":second},"protocol":{"type":"direct","target":"127.0.0.1","port":target}}
        ],
        "outbounds":[
            {"tag":"changing-exit","protocol":{"type":"direct"},"dial":dial},
            {"tag":"stable-exit","protocol":{"type":"direct"},"dial":{"address_family":"only_ipv4","source_ip":"127.0.0.1"}}
        ],
        "route":{"rules":[
            {"condition":{"type":"inbound","values":["changing-entry"]},"action":{"type":"route","outbound":"changing-exit"}},
            {"condition":{"type":"inbound","values":["stable-entry"]},"action":{"type":"route","outbound":"stable-exit"}}
        ],"final":{"type":"reject"}}
    }).to_string()).unwrap()
}

async fn dial_exchange(
    client: &UdpSocket,
    entry: u16,
    target: &UdpSocket,
    payload: &[u8],
) -> std::net::SocketAddr {
    client.send_to(payload, ("127.0.0.1", entry)).await.unwrap();
    let mut buffer = [0; 64];
    let (len, peer) = tokio::time::timeout(Duration::from_secs(3), target.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..len], payload);
    target.send_to(payload, peer).await.unwrap();
    let (len, _) = tokio::time::timeout(Duration::from_secs(3), client.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..len], payload);
    peer
}

#[tokio::test]
async fn udp_policy_reload_rebuilds_existing_client_flow_and_preserves_other_tag_mapping() {
    let ipv4 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = ipv4.local_addr().unwrap().port();
    let ipv6 = UdpSocket::bind(("::1", target))
        .await
        .expect("IPv6 loopback is required for the dual-family reload test");
    let first_port = free_port();
    let second_port = free_port();
    let proxy = Proxy::new(dial_reload_config(first_port, second_port, target, false)).unwrap();
    let handle = spawn_engine(proxy.clone());
    wait_for_listener(first_port).await;
    wait_for_listener(second_port).await;
    let first_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let changing_before = dial_exchange(&first_client, first_port, &ipv4, b"before").await;
    let stable_before = dial_exchange(&second_client, second_port, &ipv4, b"stable before").await;
    assert_eq!(
        changing_before.ip(),
        "127.0.0.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_ne!(changing_before, stable_before);
    handle
        .apply_config_and_wait(
            dial_reload_config(first_port, second_port, target, true),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    // The first datagram from the same client/target tuple must use the new
    // policy, without waiting for flow expiry or sending a sacrificial packet.
    let changing_after = dial_exchange(&first_client, first_port, &ipv6, b"after").await;
    assert_eq!(
        changing_after.ip(),
        "::1".parse::<std::net::IpAddr>().unwrap()
    );
    let active = proxy
        .engine()
        .active_sessions()
        .into_iter()
        .find(|session| session.outbound_tag.as_deref() == Some("changing-exit"))
        .expect("reloaded Direct UDP flow is observable");
    let network = active.path.network.expect("Direct UDP network observation");
    let local = network.local_address.expect("bound socket local address");
    assert_eq!(local.host, "::1");
    assert_eq!(local.port, changing_after.port());
    let remote = network.remote_address.expect("selected remote address");
    assert_eq!(remote.host, "::1");
    assert_eq!(remote.port, target);
    assert_eq!(network.address_family_policy.as_deref(), Some("only_ipv6"));
    let stable_after = dial_exchange(&second_client, second_port, &ipv4, b"stable after").await;
    assert_eq!(
        stable_before, stable_after,
        "unrelated Direct mapping must survive reload"
    );
    let mut buffer = [0; 64];
    ipv4.send_to(b"stale response", changing_before)
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_millis(50),
        first_client.recv_from(&mut buffer)
    )
    .await
    .is_err());
    handle.shutdown().await.unwrap();
}
