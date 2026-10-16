use super::*;

#[tokio::test]
async fn native_doh_binding_failure_never_falls_back_to_an_unbound_socket() {
    // Select a local non-loopback address without sending any datagram. Only
    // the private controller below is changed; host routes and TUN stay intact.
    let probe = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    probe.connect("192.0.2.1:9").unwrap();
    let local = probe.local_addr().unwrap().ip();
    assert!(!local.is_unspecified() && !local.is_loopback());
    let listener = TcpListener::bind((local, 0)).await.unwrap();
    let peer = listener.local_addr().unwrap();

    let egress = EgressInterfaceControl::default();
    egress.replace(Some(
        zero_platform_tokio::EgressInterface::new("zero-missing0", u32::MAX).unwrap(),
    ));
    let source = {
        let probe = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
        probe.connect(peer).unwrap();
        probe.local_addr().unwrap().ip()
    };
    // Simulate capture of this route's source, requiring physical binding.
    egress.replace_tunnel_addresses([source]);
    assert!(egress.select_for_peer(peer).interface().is_some());
    let resolver = DohDnsResolver::new_cleartext(
        local.to_string(),
        peer.port(),
        "/dns-query".into(),
        vec![],
        egress,
    )
    .unwrap();
    let error = exchange(&resolver)
        .await
        .expect_err("required physical interface does not exist");
    assert!(error.to_string().contains("DoH connect failed"), "{error}");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "binding failure must not result in an unbound loopback through TUN"
    );
}
