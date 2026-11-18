//! Missing IPv6 egress is an actual known-no-native attempt, not a peer fault.
use super::*;

fn has_no_native_acquisition(error: &(dyn Error + 'static), depth: usize) -> bool {
    assert!(depth < 64);
    if let Some(acquired) = error.downcast_ref::<DatagramAcquisitionError>() {
        return matches!(acquired.outcome, AcquisitionOutcome::KnownNoNative);
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        if io
            .get_ref()
            .is_some_and(|inner| has_no_native_acquisition(inner, depth + 1))
        {
            return true;
        }
    }
    error
        .source()
        .is_some_and(|inner| has_no_native_acquisition(inner, depth + 1))
}

#[tokio::test]
async fn unavailable_ipv6_egress_returns_real_no_native_evidence_and_preserves_the_udp_peer() {
    let mut fixture = BoundFixture::configured(1, |egress| {
        // Passive published topology only; this test installs no OS TUN/route.
        egress.replace_tunnel_addresses(["10.77.0.1".parse().unwrap()]);
        egress.mark_unavailable_for(true, "no_default_route");
    })
    .await;
    let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = peer.local_addr().unwrap();
    let unavailable: SocketAddr = "[ff02::1:3]:5355".parse().unwrap();
    let source = &fixture.binding.source;
    let selected = source
        .select(unavailable, fixture.binding.generation)
        .unwrap();
    assert!(selected.ensure_connectable().is_err());
    assert!(
        fixture
            .live
            .runtime
            .services()
            .upstream()
            .egress_control()
            .select_for_peer_with_policy(unavailable, source.policy())
            .is_err(),
        "ordinary selection must still reject the unavailable route"
    );
    let (failed, _failed_handle) = fixture.admit(unavailable).await;
    let error = fixture
        .bridge
        .send_new_packet(
            failed,
            &Address::Ipv6("ff02::1:3".parse::<std::net::Ipv6Addr>().unwrap().octets()),
            &[unavailable],
            b"unroutable LLMNR",
        )
        .await
        .err()
        .unwrap();
    assert!(
        has_no_native_acquisition(&error, 0),
        "retain the actual original provider's evidence: {error:?}"
    );
    assert!(!fixture.live.context.binding().cancellation().is_cancelled());
    assert!(fixture.registration.closed().is_none());
    fixture
        .bridge
        .finish_session(failed, deadline())
        .await
        .unwrap();

    // The same real dispatcher and Source must still carry another business
    // flow and close its original native socket through the same controller.
    let (healthy, _healthy_handle) = fixture.admit(address).await;
    let original = send_bridge(&mut fixture.bridge, healthy, &peer, b"still live").await;
    assert_no_packet(&peer).await;
    fixture
        .bridge
        .finish_session(healthy, deadline())
        .await
        .unwrap();
    drop(fixture.bridge);
    fixture.live.close().await;
    assert!(fixture
        .registration
        .closed()
        .unwrap()
        .matches(&fixture.registration));
    UdpSocket::bind(original).await.unwrap();
}
