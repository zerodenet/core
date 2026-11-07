//! The real Node DNS dependency retires transitive H2 custody on replacement.
use super::*;

#[tokio::test]
async fn domain_vmess_grpc_dns_reload_joins_old_query_h2_and_node_original_before_new_bootstrap() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let peer = Peer::new(false).await;
        let mut wire = crate::dns_fixture::Dns::new().await;
        let mut node = crate::dns_fixture::Dns::new().await;
        let mut next_node = crate::dns_fixture::Dns::new().await;
        let config = config(peer.address, false, &wire, Some(&node));
        let (proxy, running, handle) = start(config.clone()).await;
        let mut old_query = query(&proxy, "old.domain.grpc.dns.test");
        let bootstrap = tokio::select! {
            request = node.request() => request,
            result = &mut old_query => panic!("domain gRPC DNS failed before actual Node bootstrap: {result:?}"),
        };
        assert!(!bootstrap.tcp);
        let old_node = pending(&proxy).await;
        let old_node_native = old_node.original.clone().unwrap();
        assert!(old_node_native.parent().is_none());
        assert_eq!(peer.accepted(), 0);
        bootstrap.answer(false, false);
        tokio::time::timeout_at(deadline(), old_node.task.wait()).await.unwrap();
        let native = old_node_native.closed().unwrap();
        assert!(native.matches(&old_node_native) && native.had_native_acquisition());
        let old_request = tokio::select! {
            request = wire.request() => request,
            result = &mut old_query => panic!("domain gRPC DNS failed before actual detoured query: {result:?}"),
        };
        assert!(old_request.tcp);
        let old = pending(&proxy).await;
        assert!(!old.task.scope_identity().same_scope(&old_node.task.scope_identity()));
        let (child, original) = originals(&old);
        assert!(!original.driver().unwrap().is_joined());
        let mut candidate = config;
        candidate.runtime.dns.as_mut().unwrap().servers.insert("native".into(), next_node.server());
        let replacing = tokio::spawn(async move {
            handle.apply_runtime_config_and_wait(candidate, Duration::from_secs(10)).await
        });
        next_node.no_new_request();
        let retired=tokio::time::timeout_at(deadline(),old.task.wait()).await;
        if retired.is_err() && replacing.is_finished() {
            // Inspect only an already completed original replacement. Keep the
            // original query wait/deadline and every cancellation/close assertion.
            let actual=replacing.await;
            panic!("old actual query did not retire; actual replacement result: {actual:?}");
        }
        retired.unwrap();
        assert!(old_query.await.unwrap().is_err());
        closed(&child, &original);
        replacing.await.unwrap().unwrap();
        assert!(original.owner().closed().unwrap().matches(original.owner()));
        assert!(old_node_native.owner().closed().unwrap().matches(old_node_native.owner()));
        assert!(old_node_native.owner().users_closed().unwrap().matches(old_node_native.owner()));
        let mut fresh_query = query(&proxy, "fresh.domain.grpc.dns.test");
        let bootstrap = tokio::select! {
            request = next_node.request() => request,
            result = &mut fresh_query => panic!("replacement gRPC DNS failed before its new Node bootstrap: {result:?}"),
        };
        let fresh_node = pending(&proxy).await;
        assert!(!bootstrap.tcp);
        let fresh_node_native = fresh_node.original.clone().unwrap();
        assert_ne!(fresh_node_native.instance(), old_node_native.instance());
        assert!(!fresh_node.task.scope_identity().same_scope(&old_node.task.scope_identity()));
        assert!(original.driver().unwrap().is_joined());
        assert!(old_node_native.owner().closed().unwrap().matches(old_node_native.owner()));
        bootstrap.answer(false, false);
        tokio::time::timeout_at(deadline(), fresh_node.task.wait()).await.unwrap();
        let native = fresh_node_native.closed().unwrap();
        assert!(native.matches(&fresh_node_native) && native.had_native_acquisition());
        let request = tokio::select! {
            request = wire.request() => request,
            result = &mut fresh_query => panic!("replacement gRPC DNS failed before its actual query: {result:?}"),
        };
        let fresh = pending(&proxy).await;
        let (fresh_child, fresh_original) = originals(&fresh);
        assert_ne!(fresh_original.instance(), original.instance());
        assert!(!fresh.task.scope_identity().same_scope(&old.task.scope_identity()));
        assert_eq!(peer.accepted(), 2);
        request.answer(false, false);
        fresh_query.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(), fresh.task.wait()).await.unwrap();
        closed(&fresh_child, &fresh_original);
        drop(old_request);
        running.shutdown().await.unwrap();
        assert!(fresh_original.owner().closed().unwrap().matches(fresh_original.owner()));
        assert!(fresh_node_native.owner().closed().unwrap().matches(fresh_node_native.owner()));
        peer.close().await;
        wire.close().await;
        node.close().await;
        next_node.close().await;
    }).await.unwrap();
}
