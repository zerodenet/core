//! Same-generation address variation does not serialize independent real slots.
use super::*;
#[tokio::test]
async fn two_actual_ech_slots_deliver_a_new_peer_while_the_old_query_remains_pending() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut first = peer::Peer::cleartext().await;
        let mut second = peer::Peer::cleartext_at(std::net::SocketAddr::from((
            [127, 0, 0, 2], first.endpoint.port(),
        ))).await;
        let mut node = crate::dns_fixture::Dns::new().await;
        let endpoint = format!("h2c://endpoint.ech.test:{}/dns-query", first.endpoint.port());
        let mut config = config(&endpoint);
        config.runtime.native_dns_max_queries = None;
        config.runtime.native_dns_max_originals = Some(3);
        config.runtime.native_dns_max_h2_drivers = Some(2);
        let mut policy = node.config(); policy.policy.timeout_ms = 3000;
        config.runtime.dns = Some(policy);
        let proxy = Proxy::new(config.clone()).unwrap(); let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(3)).await.unwrap();
        let addresses = [IpAddress::V4([127,0,0,1]), IpAddress::V4([127,0,0,2])];
        let old = query_with_addresses(&proxy, &mut node, &endpoint, "warm.parallel.ech.test", &addresses, 1).await;
        assert_eq!(first.requests.recv().await.unwrap(), "warm.parallel.ech.test");
        let old_native = old.original.as_ref().unwrap();
        let (answer, result) = tokio::sync::oneshot::channel();
        let pending = proxy.resolver.spawn_ech_refresh("pending.ech.test".into(), endpoint.clone(), move |value| Box::pin(async move { let _ = answer.send(value); })).unwrap();
        loop {
            tokio::select! {
                wire = first.requests.recv() => { assert_eq!(wire.unwrap(), "pending.ech.test"); break; }
                request = node.request() => request.answer_ips(false, false, &addresses),
            }
        }
        let mut next = tokio::spawn({
            let dns = proxy.resolver.clone(); let endpoint = endpoint.clone();
            async move { dns.query_ech_config("parallel.next.ech.test", &endpoint).await }
        });
        let mut response = None;
        loop {
            tokio::select! {
                wire = second.requests.recv() => { assert_eq!(wire.unwrap(), "parallel.next.ech.test"); break; }
                request = node.request() => request.answer_ips(false, false, &[IpAddress::V4([127,0,0,2])]),
                answer = &mut next, if response.is_none() => { response = Some(answer.unwrap().unwrap()); },
            }
        }
        // The second real wire must already have progressed before releasing
        // the old query; no timer or invented cancellation marker is involved.
        assert!(!pending.task.is_joined());
        assert!(!old.task.is_joined() && old_native.closed().is_none());
        let current = proxy.resolver.native_query_observations().into_iter().find(|record| {
            !record.task.is_joined() && record.original.as_ref().and_then(|original| original.endpoints()).is_some_and(|endpoints| endpoints.peer() == second.endpoint)
        }).expect("actual second native driver on the same canonical pool Scope");
        assert!(current.task.scope_identity().same_scope(&old.task.scope_identity()));
        assert!(!current.task.same_registration(&old.task));
        assert_eq!(current.original.as_ref().unwrap().instance(), old_native.instance());
        let response = match response { Some(answer) => answer, None => next.await.unwrap().unwrap() };
        assert_eq!(response.config_list, None);
        pending.cancel.cancel(); assert!(result.await.is_err()); pending.task.wait().await.result().unwrap();
        assert!(!old.task.is_joined() && old_native.closed().is_none());
        assert!(!current.task.is_joined() && current.original.as_ref().unwrap().closed().is_none());
        assert_eq!((first.accepted.load(Ordering::Acquire), second.accepted.load(Ordering::Acquire)), (1,1));
        running.shutdown().await.unwrap();
        for original in [&old, &current] {
            assert!(original.task.is_joined());
            let native = original.original.as_ref().unwrap();
            assert!(native.closed().is_some_and(|closed| closed.matches(native) && closed.had_native_acquisition()));
        }
        first.finish().await;second.finish().await;node.close().await;
    }).await.unwrap();
}
