//! Fixed-target datagram forwarding must preserve client isolation and rollback.
#![cfg(feature = "managed-datagram-runtime")]
mod support;
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
