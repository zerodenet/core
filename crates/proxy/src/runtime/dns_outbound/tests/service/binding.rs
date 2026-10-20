use super::*;
use crate::runtime::dns_outbound::ProxyDnsOutboundConnector;
use zero_dns::{DnsConnectContext, DnsOutboundConnector};
fn callback(proxy: &Proxy) -> ProxyDnsOutboundConnector {
    ProxyDnsOutboundConnector::new(
        proxy.engine.clone(),
        &proxy.resolver,
        proxy.protocols.clone(),
        proxy.egress_interface.clone(),
        proxy.principal_rate_limits.clone(),
        proxy.retired_datagrams.clone(),
    )
}
fn direct() -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[{"tag":"a","protocol":{"type":"direct"}},{"tag":"b","protocol":{"type":"direct"}}],
        "outbound_groups":[{"tag":"choose","type":"selector","outbounds":["a","b"],"selected":"a"}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()
}

#[tokio::test]
async fn actual_native_carrier_binding_observes_selector_aba_without_closing_transferred_io() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::new(direct()).unwrap();
    start(&proxy).await;
    let mut carrier = callback(&proxy)
        .connect_carrier_with_context(
            "choose".into(),
            listener.local_addr().unwrap(),
            DnsConnectContext::default(),
        )
        .await
        .unwrap();
    let (mut remote, _) = listener.accept().await.unwrap();
    let binding = carrier
        .binding
        .as_ref()
        .expect("actual native carrier receipt");
    binding.check_current().unwrap();
    let mut revoked = binding.revoked();
    std::future::poll_fn(|cx| {
        assert!(revoked.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    proxy.engine.set_selector_target("choose", "a").unwrap();
    binding.check_current().unwrap();
    proxy.engine.set_selector_target("choose", "b").unwrap();
    proxy.engine.set_selector_target("choose", "a").unwrap();
    assert!(binding.check_current().is_err());
    tokio::time::timeout_at(deadline(), &mut revoked)
        .await
        .unwrap();
    drop(revoked);
    // This is a revocation receipt. Its consumer still owns the actual stream
    // and must explicitly close/join its driver; a watch is not an I/O ACK.
    carrier.stream.write_all(b"x").await.unwrap();
    assert_eq!(remote.read_u8().await.unwrap(), b'x');
    drop(carrier);
    assert_eq!(
        remote.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_native_carrier_binding_observes_host_generation_and_physical_network_changes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = Proxy::new(direct()).unwrap();
    start(&proxy).await;
    let original = proxy.engine.runtime_snapshot();
    proxy
        .protocols
        .commit_components(original.config(), || Ok(()))
        .unwrap();
    let carrier = callback(&proxy)
        .connect_carrier_with_context(
            "a".into(),
            listener.local_addr().unwrap(),
            DnsConnectContext::default(),
        )
        .await
        .unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    let binding = carrier.binding.as_ref().unwrap();
    binding.check_current().unwrap();
    proxy.egress_interface.invalidate_network();
    assert!(binding.check_current().is_err());
    tokio::time::timeout_at(deadline(), binding.revoked())
        .await
        .unwrap();
    drop(carrier);
    drop(remote);
    let carrier = callback(&proxy)
        .connect_carrier_with_context(
            "a".into(),
            listener.local_addr().unwrap(),
            DnsConnectContext::default(),
        )
        .await
        .unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    let binding = carrier.binding.as_ref().unwrap();
    binding.check_current().unwrap();
    let mut candidate = (**original.config()).clone();
    candidate.outbounds[0].dial.address_family = zero_config::OutboundAddressFamily::OnlyIpv4;
    proxy.engine.stage_runtime_config(candidate).unwrap();
    let config = proxy.engine.runtime_snapshot().config().clone();
    proxy
        .protocols
        .start_components(config.clone(), deadline())
        .await
        .unwrap();
    binding.check_current().unwrap(); // The actual old owner is still current.
    proxy
        .protocols
        .commit_components(&config, || Ok(()))
        .unwrap();
    assert!(binding.check_current().is_err());
    tokio::time::timeout_at(deadline(), binding.revoked())
        .await
        .unwrap();
    drop(carrier);
    drop(remote);
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}

#[tokio::test]
async fn fallback_receipt_tracks_only_the_branch_that_actually_connected() {
    let dns = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = dns.local_addr().unwrap();
    let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(
        endpoint,
        &[
            first.local_addr().unwrap().port(),
            second.local_addr().unwrap().port(),
        ],
    );
    config.outbound_groups = serde_json::from_value(serde_json::json!([
        {"tag":"first","type":"selector","outbounds":["socks-0","socks-1"],"selected":"socks-0"},
        {"tag":"second","type":"selector","outbounds":["socks-0","socks-1"],"selected":"socks-1"},
        {"tag":"dns-chain","type":"fallback","outbounds":["first","second"]}
    ]))
    .unwrap();
    let proxy = Proxy::new(config).unwrap();
    start(&proxy).await;
    let rejected = tokio::spawn(async move {
        let (mut io, _) = first.accept().await.unwrap();
        let mut greeting = [0; 3];
        io.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 1, 0]);
        io.write_all(&[5, 255]).await.unwrap();
    });
    let accepted = tokio::spawn(async move {
        let (io, _) = second.accept().await.unwrap();
        socks(io, endpoint, false).await;
    });
    let carrier = callback(&proxy)
        .connect_carrier_with_context("dns-chain".into(), endpoint, DnsConnectContext::default())
        .await
        .unwrap();
    let (remote, _) = dns.accept().await.unwrap();
    let binding = carrier
        .binding
        .as_ref()
        .expect("unambiguous actually successful fallback branch");
    proxy
        .engine
        .set_selector_target("first", "socks-1")
        .unwrap();
    binding.check_current().unwrap(); // Failed branch is not a dependency.
    proxy
        .engine
        .set_selector_target("second", "socks-0")
        .unwrap();
    assert!(binding.check_current().is_err());
    tokio::time::timeout_at(deadline(), binding.revoked())
        .await
        .unwrap();
    drop(carrier);
    drop(remote);
    rejected.await.unwrap();
    accepted.await.unwrap();
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}

#[tokio::test]
async fn native_load_balance_capture_and_reuse_do_not_consume_a_second_scheduling_pick() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = direct();
    config.outbound_groups[0].group = zero_config::OutboundGroupKind::LoadBalance {
        outbounds: vec!["a".into(), "b".into()],
        default: None,
        strategy: zero_config::LoadBalanceStrategy::RoundRobin,
    };
    let proxy = Proxy::new(config).unwrap();
    start(&proxy).await;
    let carrier = callback(&proxy)
        .connect_carrier_with_context(
            "choose".into(),
            listener.local_addr().unwrap(),
            DnsConnectContext::default(),
        )
        .await
        .unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    let binding = carrier
        .binding
        .as_ref()
        .expect("actual selected load-balance branch");
    for _ in 0..8 {
        binding.check_current().unwrap();
    }
    let mut revoked = binding.revoked();
    std::future::poll_fn(|cx| {
        assert!(revoked.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(revoked);
    let snapshot = proxy.engine.runtime_snapshot();
    let target = snapshot.plan().target_id("choose").unwrap();
    let (next, _) = proxy
        .engine
        .resolve_target_id_in_snapshot(&snapshot, target)
        .unwrap();
    let zero_engine::ResolvedOutbound::Fallback { candidates } = next else {
        panic!("load-balance fallback grammar")
    };
    assert!(
        matches!(
            candidates.first(),
            Some(zero_engine::ResolvedLeafOutbound::Direct { tag: Some("b"), .. })
        ),
        "only creating the original actual connection consumed the a scheduling pick"
    );
    drop(carrier);
    drop(remote);
    proxy
        .protocols
        .shutdown_components(deadline())
        .await
        .unwrap();
}
