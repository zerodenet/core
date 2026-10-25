use super::*;

#[tokio::test]
async fn actual_proxy_native_dns_reload_retires_original_query_and_transitive_mieru_pool_before_new_owner_start(
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut dns = Dns::new().await;
        let mut next = Dns::new().await;
        let a = Server::new(true).await;
        let b = Server::new(true).await;
        let c = Server::new(false).await;
        let mut config = config([a.address, b.address, c.address]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, handle) = start(config.clone()).await;
        let mut held = loan(&proxy).await;
        let pool = held.owner.original().clone();
        let querying = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("old-owner.node.test").await })
        };
        let old_request = dns.request().await;
        let previous = pending(&proxy).await;
        let prior = original(&previous);
        config.runtime.dns = Some(next.config());
        let replacing = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(config, Duration::from_secs(10))
                .await
        });
        previous.task.wait().await;
        assert!(querying.await.unwrap().is_err());
        assert!(
            !replacing.is_finished(),
            "retained actual pool custody blocks the replacement DNS dependency"
        );
        next.no_new_request();
        assert!(
            prior.owner().closed().is_none(),
            "an incomplete consumer blocks dependency cleanup"
        );
        drop(held.io);
        assert!(held.owner.close(deadline()).await.unwrap().is_complete());
        replacing.await.unwrap().unwrap();
        closed(&prior);
        assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        let querying = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("new-owner.node.test").await })
        };
        let request = next.request().await;
        assert!(previous.task.is_joined());
        closed(&prior);
        let current = pending(&proxy).await;
        let fresh = original(&current);
        assert_ne!(fresh.instance(), prior.instance());
        assert!(!previous
            .task
            .scope_identity()
            .same_scope(&current.task.scope_identity()));
        request.answer(false, false);
        querying.await.unwrap().unwrap();
        current.task.wait().await;
        closed(&fresh);
        let mut successor = loan(&proxy).await;
        assert_ne!(
            pool.instance(),
            successor.owner.original().instance(),
            "actual DNS dependency advances the real shared pool"
        );
        successor.io.write_all(b"new DNS owner").await.unwrap();
        successor.io.read_exact(&mut [0; 13]).await.unwrap();
        drop(successor.io);
        assert!(successor
            .owner
            .close(deadline())
            .await
            .unwrap()
            .is_complete());
        drop(old_request);
        running.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
        c.close().await;
        dns.close().await;
        next.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_native_dns_network_change_joins_pending_original_before_new_native_io() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(config).await;
        let querying = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("old-network.node.test").await })
        };
        let old_request = dns.request().await;
        let previous = pending(&proxy).await;
        let prior = original(&previous);
        proxy.egress_interface_control().invalidate_network();
        let querying_next = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("new-network.node.test").await })
        };
        let request = dns.request().await;
        assert!(
            previous.task.is_joined(),
            "actual old query join precedes the new socket"
        );
        closed(&prior);
        assert!(querying.await.unwrap().is_err());
        let current = pending(&proxy).await;
        let successor = original(&current);
        assert_eq!(
            prior.instance(),
            successor.instance(),
            "network transition reuses the one host DNS owner"
        );
        assert!(previous
            .task
            .scope_identity()
            .same_scope(&current.task.scope_identity()));
        assert!(!previous.task.same_registration(&current.task));
        request.answer(false, false);
        querying_next.await.unwrap().unwrap();
        current.task.wait().await;
        closed(&successor);
        drop(old_request);
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_shutdown_confirms_pending_native_dns_task_and_original_controller() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(config).await;
        let querying = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("shutdown.node.test").await })
        };
        let request = dns.request().await;
        let query = pending(&proxy).await;
        let prior = original(&query);
        assert!(prior.closed().is_none());
        running.shutdown().await.unwrap();
        assert!(query.task.is_joined());
        closed(&prior);
        assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        assert!(querying.await.unwrap().is_err());
        drop(request);
        dns.close().await;
    })
    .await
    .unwrap();
}
