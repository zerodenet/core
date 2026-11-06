//! Outbound-only WG DNS owns its existing Bound wire; no inbound Source substitute.
use super::*;
pub(super) async fn close(
    proxy: &Proxy,
    running: crate::RunningProxy,
    root: &CarrierOrigin,
    children: &[CarrierOrigin],
) {
    assert!(
        proxy
            .system_tun_source_registry()
            .owner("wg-source")
            .is_none()
    );
    assert!(
        proxy.engine().active_sessions().is_empty()
            && proxy.engine().completed_sessions().is_empty()
    );
    let driver = root.driver().unwrap().clone();
    assert!(!driver.is_joined() && root.parent().is_none() && root.closed().is_none());
    running.shutdown().await.unwrap();
    let closed = root
        .closed()
        .expect("same outbound-only Host Bound socket closed");
    assert!(closed.matches(root) && closed.had_native_acquisition());
    assert!(root.owner().closed().unwrap().matches(root.owner()));
    let joined = driver.try_joined().expect("actual same wire task joined");
    assert!(driver.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
    for child in children {
        returned(child);
        assert!(child.owner().closed().unwrap().matches(child.owner()));
        assert!(child.owner().users_closed().unwrap().matches(child.owner()));
    }
    assert_eq!(
        proxy
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .snapshot()
            .charged,
        0
    );
}
#[tokio::test]
async fn ordinary_standalone_wireguard_dns_udp_cancel_returns_query_socket_and_preserves_same_native_bound_wire()
 {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let peer = peer::Peer::new(dns.address).await;
        let (proxy, running) = start_server_mode(&peer, dns.server(), false).await;
        let roots = proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref());
        assert_eq!(roots.len(), 1);
        let root = &roots[0];
        let mut children = Vec::new();
        for (index, cancel) in [false, true, false].into_iter().enumerate() {
            let mut query = query(&proxy, &format!("{index}.standalone.wg.dns.test"));
            let request = tokio::select! {
                request = dns.request() => request,
                result = &mut query => panic!("actual standalone WG DNS failed before encrypted UDP delivery: {result:?}"),
            };
            assert!(!request.tcp);
            let actual = pending(&proxy).await;
            let child = actual.original.as_ref().unwrap();
            native_pending(child, root);
            assert!(!actual.task.scope_identity().same_scope(&root.driver().unwrap().scope_identity()));
            if cancel {
                query.abort(); assert!(query.await.unwrap_err().is_cancelled()); drop(request);
            } else {
                request.answer(false, false);
                assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
            }
            tokio::time::timeout_at(end(), actual.task.wait()).await.unwrap().result().unwrap();
            returned(child); native_pending(child, root); children.push(child.clone());
        }
        close(&proxy, running, root, &children).await;
        peer.close().await; dns.close().await;
    }).await.unwrap();
}
#[tokio::test]
async fn ordinary_standalone_wireguard_truncated_dns_closes_original_udp_before_fixed_tcp_fallback()
{
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let peer = peer::Peer::new(dns.address).await;
        let (proxy, running) = start_server_mode(&peer, dns.server(), false).await;
        let roots = proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref());
        assert_eq!(roots.len(), 1); let root = &roots[0];
        let mut query = query(&proxy, "truncated.standalone.wg.dns.test");
        let request = tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("actual WG UDP delivery failed: {result:?}")};
        assert!(!request.tcp);
        let udp = pending(&proxy).await; let first = udp.original.as_ref().unwrap();
        native_pending(first, root); request.answer(true, false);
        let request = tokio::select! {request=dns.request()=>request,result=&mut query=>panic!("actual WG TCP fallback failed: {result:?}")};
        assert!(request.tcp); returned(first);
        let tcp = pending(&proxy).await; let next = tcp.original.as_ref().unwrap();
        assert!(tcp.task.same_registration(&udp.task)); assert!(!next.same_original(first));
        native_pending(next, root); request.answer(false, false);
        assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
        tokio::time::timeout_at(end(), tcp.task.wait()).await.unwrap().result().unwrap();
        returned(next); native_pending(next, root);
        close(&proxy, running, root, &[first.clone(), next.clone()]).await;
        peer.close().await; dns.close().await;
    }).await.unwrap();
}
