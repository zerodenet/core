use tokio::net::TcpListener;
use zero_platform_tokio::{EgressInterface, EgressInterfaceControl, TokioSocket};

#[test]
fn controller_replaces_and_clears_interface_atomically() {
    let controller = EgressInterfaceControl::default();
    let interface = EgressInterface::new("physical0", 7).expect("valid interface");

    assert!(controller.current().is_none());
    assert!(controller.replace(Some(interface.clone())).is_none());
    assert_eq!(controller.current(), Some(interface.clone()));
    assert_eq!(controller.replace(None), Some(interface));
    assert!(controller.current().is_none());
}

#[test]
fn controller_selects_independent_ipv4_and_ipv6_interfaces() {
    let controller = EgressInterfaceControl::default();
    let ipv4 = EgressInterface::new("ethernet", 7).expect("valid IPv4 interface");
    let ipv6 = EgressInterface::new("teredo", 14).expect("valid IPv6 interface");

    controller.replace_for(false, Some(ipv4.clone()));
    controller.replace_for(true, Some(ipv6.clone()));
    assert_eq!(controller.current_for(false), Some(ipv4));
    assert_eq!(controller.current_for(true), Some(ipv6));

    controller.clear();
    assert!(controller.current_for(false).is_none());
    assert!(controller.current_for(true).is_none());
}

#[test]
fn interface_identity_rejects_incomplete_values() {
    assert!(EgressInterface::new("", 1).is_err());
    assert!(EgressInterface::new("physical0", 0).is_err());
}

#[tokio::test]
async fn loopback_connection_does_not_bind_to_physical_egress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let invalid_physical = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();

    let (client, accepted) = tokio::join!(
        TokioSocket::connect_addr_on(address, Some(&invalid_physical)),
        listener.accept()
    );
    let client = client.expect("loopback connection must ignore physical egress binding");
    assert!(client.egress_interface().is_none());
    accepted.expect("accept loopback connection");
}

#[tokio::test]
async fn loopback_datagram_does_not_bind_to_physical_egress() {
    let peer = "127.0.0.1:53".parse().unwrap();
    let invalid_physical = EgressInterface::new("not-a-real-interface", u32::MAX).unwrap();

    let socket =
        zero_platform_tokio::TokioDatagramSocket::bind_for_peer_on(peer, Some(&invalid_physical))
            .await
            .expect("loopback datagram must ignore physical egress binding");

    assert!(socket.local_addr().unwrap().is_ipv4());
}
