//! A different real peer waits for actual old logical users, then the same pool retires its original.
use super::*;
#[tokio::test]
async fn changed_ech_candidates_wait_for_original_users_and_close_before_next_native() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut first = peer::Peer::cleartext().await;
        let mut second = peer::Peer::cleartext_at(std::net::SocketAddr::from((
            [127, 0, 0, 2], first.endpoint.port(),
        ))).await;
        let mut node = crate::dns_fixture::Dns::new().await;
        let endpoint = format!("h2c://endpoint.ech.test:{}/dns-query", first.endpoint.port());
        let mut config = config(&endpoint);
        config.runtime.native_dns_max_queries = None;
        config.runtime.native_dns_max_originals = Some(2);
        config.runtime.native_dns_max_h2_drivers = Some(1);
        let mut policy = node.config(); policy.policy.timeout_ms = 3000;
        config.runtime.dns = Some(policy);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(3)).await.unwrap();
        let old = query_with_addresses(&proxy, &mut node, &endpoint, "warm.candidate.ech.test", &[IpAddress::V4([127,0,0,1])], 1).await;
        assert_eq!(first.requests.recv().await.unwrap(), "warm.candidate.ech.test");
        let old_native = old.original.as_ref().unwrap();
        let (answer, result) = tokio::sync::oneshot::channel();
        let pending = proxy.resolver.spawn_ech_refresh("pending.ech.test".into(), endpoint.clone(), move |value| Box::pin(async move { let _ = answer.send(value); })).unwrap();
        loop {
            tokio::select! {
                wire = first.requests.recv() => { assert_eq!(wire.unwrap(), "pending.ech.test"); break; }
                request = node.request() => request.answer_ips(false, false, &[IpAddress::V4([127,0,0,1])]),
            }
        }
        assert!(!old.task.is_joined() && old_native.closed().is_none());
        let mut next = tokio::spawn({
            let dns = proxy.resolver.clone(); let endpoint = endpoint.clone();
            async move { dns.query_ech_config("next.candidate.ech.test", &endpoint).await }
        });
        let request = tokio::select! {
            request = node.request() => request,
            response = &mut next => panic!("new candidate skipped its actual Node bootstrap: {response:?}"),
        };
        request.answer_ips(false, false, &[IpAddress::V4([127,0,0,2])]);
        assert!(!pending.task.is_joined());
        assert!(old_native.closed().is_none(), "a pending actual logical user retains its native original");
        pending.cancel.cancel();
        assert!(result.await.is_err());
        pending.task.wait().await.result().unwrap();
        let mut response = None;
        loop {
            tokio::select! {
                wire = second.requests.recv() => { assert_eq!(wire.unwrap(), "next.candidate.ech.test"); break; }
                request = node.request() => request.answer_ips(false, false, &[IpAddress::V4([127,0,0,2])]),
                answer = &mut next, if response.is_none() => { response = Some(answer.unwrap().unwrap()); },
            }
        }
        assert!(old.task.is_joined());
        assert!(old_native.closed().is_some_and(|closed| closed.matches(old_native) && closed.had_native_acquisition()));
        let response = match response { Some(answer) => answer, None => next.await.unwrap().unwrap() };
        assert_eq!(response.config_list, None);
        assert_eq!((first.accepted.load(Ordering::Acquire), second.accepted.load(Ordering::Acquire)), (1,1));
        let successor = proxy.resolver.native_query_observations().into_iter().find(|record| {
            !record.task.is_joined() && record.original.as_ref().and_then(|original| original.endpoints()).is_some_and(|endpoints| endpoints.peer() == second.endpoint)
        }).expect("actual successor driver and same native original");
        assert!(!successor.task.same_registration(&old.task));
        running.shutdown().await.unwrap();
        assert!(successor.task.is_joined());
        let native = successor.original.as_ref().unwrap();
        assert!(native.closed().is_some_and(|closed| closed.matches(native) && closed.had_native_acquisition()));
        first.finish().await;second.finish().await;node.close().await;
    }).await.unwrap();
}
