//! DNS fallback retains resolver-owned outcomes without business health effects.
use super::*;

#[tokio::test]
async fn actual_dns_detour_bypasses_traffic_quarantine_without_clearing_it() {
    let mut dns = crate::dns_fixture::Dns::new().await;
    let endpoint = dns.address;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::new(config(endpoint, &[listener.local_addr().unwrap().port()])).unwrap();
    start(&proxy).await;
    for _ in 0..5 {
        proxy.engine.record_outbound_failure("socks-0");
    }
    assert_eq!(
        proxy
            .engine
            .check_outbound_health("socks-0")
            .unwrap_err()
            .code(),
        "unhealthy_outbound"
    );
    let relay = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        udp::socks(io, endpoint).await;
    });
    let answer = tokio::spawn(async move {
        let request = dns.request().await;
        assert!(
            !request.tcp,
            "the quarantined selection still uses its actual Datagram role"
        );
        request.answer_ips(false, false, &[IpAddress::V4([198, 51, 100, 61])]);
        dns
    });
    let result = proxy
        .protocols
        .drive_component_supervision(Box::pin(
            proxy.resolver.resolve_real_type("health-isolated.test", 1),
        ))
        .await
        .unwrap();
    assert_eq!(result, vec![IpAddress::V4([198, 51, 100, 61])]);
    assert_eq!(
        proxy
            .engine
            .check_outbound_health("socks-0")
            .unwrap_err()
            .code(),
        "unhealthy_outbound"
    );
    assert!(
        proxy.engine.active_sessions().is_empty() && proxy.engine.completed_sessions().is_empty()
    );
    let dns = answer.await.unwrap();
    relay.await.unwrap();
    dns.close().await;
    proxy
        .protocols
        .drive_component_supervision(Box::pin(proxy.resolver.shutdown_queries(deadline())))
        .await
        .unwrap();
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_dns_handshake_failures_do_not_quarantine_business_traffic() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::new(config(
        dns.local_addr().unwrap(),
        &[listener.local_addr().unwrap().port()],
    ))
    .unwrap();
    start(&proxy).await;
    let rejected = tokio::spawn(async move {
        for _ in 0..5 {
            let (mut io, _) = listener.accept().await.unwrap();
            let mut greeting = [0; 3];
            io.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [5, 1, 0]);
            io.write_all(&[5, 255]).await.unwrap();
            let mut byte = [0];
            assert_eq!(io.read(&mut byte).await.unwrap(), 0);
        }
    });
    for index in 0..5 {
        assert!(proxy
            .protocols
            .drive_component_supervision(Box::pin(
                proxy
                    .resolver
                    .resolve_real_type(&format!("failed-{index}.health-isolated.test"), 1),
            ))
            .await
            .is_err());
    }
    proxy
        .engine
        .check_outbound_health("socks-0")
        .expect("resolver failures cannot quarantine business traffic");
    assert!(
        proxy.engine.active_sessions().is_empty() && proxy.engine.completed_sessions().is_empty()
    );
    rejected.await.unwrap();
    proxy
        .protocols
        .drive_component_supervision(Box::pin(proxy.resolver.shutdown_queries(deadline())))
        .await
        .unwrap();
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}
