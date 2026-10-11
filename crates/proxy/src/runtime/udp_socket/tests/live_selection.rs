use super::*;
use std::time::Duration;

async fn receive(target: &tokio::net::UdpSocket, buffer: &mut [u8]) -> usize {
    tokio::time::timeout(Duration::from_secs(2), target.recv_from(buffer))
        .await
        .expect("accepted packet must arrive")
        .unwrap()
        .0
}

#[tokio::test]
async fn warm_socket_revalidates_source_against_new_tun_state_without_sending() {
    let config = zero_config::RuntimeConfig::parse(r#"{
        "outbounds": [{"tag":"bound","protocol":{"type":"direct"},"dial":{"source_ip":"127.0.0.1"}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let mut sockets = socket_set(&proxy, None);
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let bound = policy(&proxy, Some("bound"));
    sockets
        .send_to_addr(b"warm", peer, 1, &bound)
        .await
        .unwrap();
    let mut buffer = [0; 16];
    receive(&target, &mut buffer).await;

    // Publish only on this isolated selector; the host and active TUN are untouched.
    proxy.egress_interface.replace_tunnel_addresses([peer.ip()]);
    let error = sockets
        .send_to_addr(b"blocked", peer, 1, &bound)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("TUN"));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), target.recv_from(&mut buffer))
            .await
            .is_err()
    );
    proxy.egress_interface.clear();
    sockets
        .send_to_addr(b"restored", peer, 1, &bound)
        .await
        .unwrap();
    let len = receive(&target, &mut buffer).await;
    assert_eq!(&buffer[..len], b"restored");
}

#[tokio::test]
async fn warm_socket_rejects_changed_live_interface_policy_then_recovers() {
    // The tagged policy itself is current, but publishing a strict interface
    // conflicting with its source must invalidate the warm native socket.
    let config = zero_config::RuntimeConfig::parse(r#"{
        "outbounds": [{"tag":"bound","protocol":{"type":"direct"},"dial":{"source_ip":"127.0.0.1"}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }"#).unwrap();
    let proxy = crate::runtime::Proxy::new(config).unwrap();
    let mut sockets = socket_set(&proxy, None);
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let bound = policy(&proxy, Some("bound"));
    sockets
        .send_to_addr(b"warm", peer, 1, &bound)
        .await
        .unwrap();
    let mut buffer = [0; 16];
    receive(&target, &mut buffer).await;
    proxy.egress_interface.replace(Some(
        zero_platform_tokio::EgressInterface::new("missing", u32::MAX)
            .unwrap()
            .with_socket_mark(17)
            .unwrap(),
    ));
    assert!(sockets
        .send_to_addr(b"blocked", peer, 1, &bound)
        .await
        .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), target.recv_from(&mut buffer))
            .await
            .is_err()
    );
    proxy.egress_interface.clear();
    sockets
        .send_to_addr(b"restored", peer, 1, &bound)
        .await
        .unwrap();
    let len = receive(&target, &mut buffer).await;
    assert_eq!(&buffer[..len], b"restored");
}
