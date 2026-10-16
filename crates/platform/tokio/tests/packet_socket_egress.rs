use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use zero_platform_tokio::{
    EgressInterface, EgressInterfaceControl, PacketSocket, PacketSocketSender,
};

async fn receive(socket: &UdpSocket) -> (Vec<u8>, SocketAddr) {
    let mut buffer = [0; 256];
    let (size, peer) = tokio::time::timeout(Duration::from_secs(2), socket.recv_from(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    (buffer[..size].to_vec(), peer)
}

async fn loopback_roundtrip(listen: &str) {
    let socket = Arc::new(UdpSocket::bind(listen).await.unwrap());
    let peer = UdpSocket::bind(listen).await.unwrap();
    let local = socket.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    control.mark_unavailable_for(local.is_ipv6(), "no_default_route");
    control.replace_tunnel_addresses(["10.0.0.1".parse().unwrap()]);
    let mut sender = PacketSocketSender::new(PacketSocket::Udp(socket.clone()), control);
    for bytes in [b"request".as_slice(), &[]] {
        assert_eq!(
            sender
                .send_to(bytes, peer.local_addr().unwrap())
                .await
                .unwrap(),
            bytes.len()
        );
        let (actual, source) = receive(&peer).await;
        assert_eq!(actual, bytes);
        assert_eq!(source, local, "retain listener source port");
        peer.send_to(b"reply", source).await.unwrap();
        assert_eq!(
            receive(&socket).await,
            (b"reply".to_vec(), peer.local_addr().unwrap())
        );
    }
}

#[tokio::test]
async fn packet_sender_preserves_shared_ipv4_port_replies_and_loopback_bypass() {
    loopback_roundtrip("127.0.0.1:0").await;
}

#[tokio::test]
async fn packet_sender_preserves_shared_ipv6_port_replies_and_loopback_bypass() {
    loopback_roundtrip("[::1]:0").await;
}

async fn non_loopback_peer() -> UdpSocket {
    // No packet is sent by this route probe; only the controller below changes.
    let probe = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    probe.connect("192.0.2.1:9").unwrap();
    let local = std::env::var("ZERO_TEST_HOST_IPV4")
        .map(|value| {
            value
                .parse::<std::net::IpAddr>()
                .expect("test host address")
        })
        .unwrap_or_else(|_| probe.local_addr().unwrap().ip());
    assert!(!local.is_loopback() && !local.is_unspecified());
    UdpSocket::bind((local, 0)).await.unwrap()
}

#[tokio::test]
async fn packet_sender_binding_failure_never_retries_unbound_and_generation_recovers() {
    let peer = non_loopback_peer().await;
    let target = peer.local_addr().unwrap();
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
    let port = socket.local_addr().unwrap().port();
    let control = EgressInterfaceControl::default();
    control.replace(Some(
        EgressInterface::new("zero-missing0", u32::MAX).unwrap(),
    ));
    let mut sender = PacketSocketSender::new(socket.into(), control.clone());
    for _ in 0..2 {
        assert!(sender.send_to(b"must-not-leak", target).await.is_err());
    }
    let mut bytes = [0; 32];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    control.clear();
    sender.send_to(b"recovered", target).await.unwrap();
    let (actual, source) = receive(&peer).await;
    assert_eq!(actual, b"recovered");
    assert_eq!(source.port(), port);
}

#[tokio::test]
async fn packet_sender_cached_route_cannot_bypass_new_unavailable_family() {
    let peer = non_loopback_peer().await;
    let target = peer.local_addr().unwrap();
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await.unwrap());
    let control = EgressInterfaceControl::default();
    let mut sender = PacketSocketSender::new(socket.into(), control.clone());
    sender.send_to(b"before", target).await.unwrap();
    assert_eq!(receive(&peer).await.0, b"before");
    control.replace_tunnel_addresses(["10.0.0.1".parse().unwrap()]);
    control.mark_unavailable_for(false, "physical_link_lost");
    let error = sender.send_to(b"must-not-leak", target).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
    let mut bytes = [0; 32];
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv_from(&mut bytes))
            .await
            .is_err()
    );
    control.clear();
    sender.send_to(b"after", target).await.unwrap();
    assert_eq!(receive(&peer).await.0, b"after");
}
