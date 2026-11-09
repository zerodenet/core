//! Host source binding belongs to WG's original underlay, not inner sockets.
use super::*;

#[tokio::test]
async fn standalone_nondefault_wg_dns_udp_cancel_and_tc_keep_actual_underlay_dial_and_query_custody(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = crate::dns_fixture::Dns::new().await;
        let peer = peer::Peer::new(dns.address).await;
        let (proxy, running) = start_server_with_source(&peer, dns.server(), false, false, Some("127.0.0.2")).await;
        let roots = proxy.protocols.test_packet_source_origins(proxy.engine().config().as_ref());
        assert_eq!(roots.len(), 1);
        let root = &roots[0];
        let mut children = Vec::new();
        for (index, cancel) in [false, true, false].into_iter().enumerate() {
            let mut query = query(&proxy, &format!("{index}.nondefault.wg.dns.test"));
            let request = tokio::select! {
                request = dns.request() => request,
                result = &mut query => panic!("nondefault WG DNS failed before actual UDP delivery: {result:?}"),
            };
            assert!(!request.tcp);
            assert_eq!(peer.actual_outer_source().unwrap().ip(), "127.0.0.2".parse::<std::net::IpAddr>().unwrap());
            let actual = pending(&proxy).await;
            let first = actual.original.as_ref().unwrap();
            native_pending(first, root);
            assert!(!actual.task.scope_identity().same_scope(&root.driver().unwrap().scope_identity()));
            if cancel {
                query.abort();
                assert!(query.await.unwrap_err().is_cancelled());
                drop(request);
                tokio::time::timeout_at(end(), actual.task.wait()).await.unwrap().result().unwrap();
                returned(first);
                children.push(first.clone());
            } else {
                request.answer(index == 2, false);
                if index == 2 {
                    let request = tokio::select! {
                        request = dns.request() => request,
                        result = &mut query => panic!("nondefault WG TC fallback did not use its actual virtual TCP: {result:?}"),
                    };
                    assert!(request.tcp);
                    returned(first);
                    let next = pending(&proxy).await;
                    assert!(next.task.same_registration(&actual.task));
                    let child = next.original.as_ref().unwrap();
                    assert!(!child.same_original(first));
                    native_pending(child, root);
                    request.answer(false, false);
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
                    tokio::time::timeout_at(end(), next.task.wait()).await.unwrap().result().unwrap();
                    returned(child);
                    children.push(child.clone());
                } else {
                    assert_eq!(query.await.unwrap().unwrap(), vec![IpAddress::V4([127,0,0,1])]);
                    tokio::time::timeout_at(end(), actual.task.wait()).await.unwrap().result().unwrap();
                }
                returned(first);
                children.push(first.clone());
            }
            native_pending(first, root);
        }
        standalone::close(&proxy, running, root, &children).await;
        peer.close().await;
        dns.close().await;
    }).await.unwrap();
}

#[tokio::test]
async fn standalone_nondefault_wg_dot_and_persistent_doh_keep_underlay_and_pool_independent() {
    encrypted::exercise_with_source(false, Some("127.0.0.2")).await;
}
