//! Default and Node questions borrow one actual persistent physical driver.
use super::*;
use std::sync::atomic::Ordering;
#[path = "peer.rs"]
mod peer;

#[tokio::test]
async fn ech_and_default_questions_share_actual_doh_pool_and_cancel_only_the_logical_request() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut peer = peer::Peer::start().await;
        let endpoint = format!("https://{}/dns-query", peer.endpoint);
        let mut config = config(&endpoint);
        config.runtime.native_dns_max_queries = None;
        config.runtime.dns = Some(serde_json::from_value(serde_json::json!({
            "servers":{"query":{"type":"doh", "host":"127.0.0.1", "port":peer.endpoint.port(), "path":"/dns-query"}},
            "default_server":"query", "policy":{"timeout_ms":3000}
        })).unwrap());
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(3)).await.unwrap();
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", peer.ca.clone()).unwrap();
        assert_eq!(proxy.resolver.resolve_real_type("first.default.ech.test", 1).await.unwrap(), vec![IpAddress::V4([203,0,113,9])]);
        assert_eq!(peer.requests.recv().await.unwrap(), "first.default.ech.test");
        let original = loop {
            if let Some(original) = proxy.resolver.native_query_observations().into_iter().find(|record| record.original.is_some() && !record.task.is_joined()) { break original; }
            tokio::task::yield_now().await;
        };
        let native = original.original.unwrap();
        assert_eq!(proxy.resolver.query_ech_config("cover.ech.test", &endpoint).await.unwrap().config_list, None);
        assert_eq!(peer.requests.recv().await.unwrap(), "cover.ech.test");
        assert_eq!(peer.accepted.load(Ordering::Acquire), 1, "same configured backend, connection lock and actual driver");
        let (answer, result) = tokio::sync::oneshot::channel();
        let query = proxy.resolver.spawn_ech_refresh("pending.ech.test".into(), endpoint.clone(), move |value| Box::pin(async move { let _ = answer.send(value); })).unwrap();
        assert_eq!(peer.requests.recv().await.unwrap(), "pending.ech.test");
        assert!(!query.task.scope_identity().same_scope(&original.task.scope_identity()), "actual Node query differs from the canonical Default physical driver's Scope");
        let held = zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query").unwrap();
        assert_eq!((held.registered_driver_tasks, held.active_leases), (1,1));
        query.cancel.cancel();
        assert!(result.await.is_err());
        query.task.wait().await.result().unwrap();
        assert!(!original.task.is_joined(), "logical query cancellation cannot retire the canonical pool driver");
        assert!(native.closed().is_none());
        assert_eq!(proxy.resolver.query_ech_config("reuse.ech.test", &endpoint).await.unwrap().config_list, None);
        assert_eq!(peer.requests.recv().await.unwrap(), "reuse.ech.test");
        assert_eq!(peer.accepted.load(Ordering::Acquire), 1);
        // Only the owning outbound ECH endpoint set changes. DNS document and
        // dispatch are byte-for-byte identical, but both real provider Scopes
        // change; a cold candidate cannot keep their old host-bound backend.
        let mut extra = config.outbounds[0].clone();
        extra.tag = "additional-ech-node".into();
        let zero_config::OutboundProtocolConfig::Vless { tls, .. } = &mut extra.protocol else { unreachable!() };
        tls.as_mut().unwrap().options.ech_config_list = "udp://127.0.0.1:9".into();
        config.outbounds.push(extra);
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(3)).await.unwrap();
        assert!(original.task.is_joined(), "same canonical old Scope joined before config ACK");
        assert!(native.closed().is_some_and(|closed| closed.matches(&native) && closed.had_native_acquisition()));
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", peer.ca.clone()).unwrap();
        assert_eq!(proxy.resolver.resolve_real_type("after.scope.ech.test", 1).await.unwrap(), vec![IpAddress::V4([203,0,113,9])]);
        assert_eq!(peer.requests.recv().await.unwrap(), "after.scope.ech.test");
        let successor = proxy.resolver.native_query_observations().into_iter().find(|record| record.original.is_some() && !record.task.is_joined()).expect("same configured endpoint has a new actual canonical provider Scope");
        assert!(!successor.task.scope_identity().same_scope(&original.task.scope_identity()));
        let successor_native = successor.original.unwrap();
        assert_eq!(proxy.resolver.query_ech_config("scope.reuse.ech.test", &endpoint).await.unwrap().config_list, None);
        assert_eq!(peer.requests.recv().await.unwrap(), "scope.reuse.ech.test");
        assert_eq!(peer.accepted.load(Ordering::Acquire), 2, "new actual Scope's one same configured DoH pool serves both purposes");
        proxy.egress_interface_control().invalidate_network();
        assert_eq!(proxy.resolver.query_ech_config("network.ech.test", &endpoint).await.unwrap().config_list, None);
        assert_eq!(peer.requests.recv().await.unwrap(), "network.ech.test");
        assert!(successor.task.is_joined());
        let closed = successor_native.closed().expect("same old physical original closed before new network driver");
        assert!(closed.matches(&successor_native) && closed.had_native_acquisition());
        assert_eq!(peer.accepted.load(Ordering::Acquire), 3);
        let current = proxy.resolver.native_query_observations().into_iter().find(|record| record.original.is_some() && !record.task.is_joined()).expect("new actual physical driver");
        let current_native = current.original.unwrap();
        assert!(!current.task.same_registration(&original.task));
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert!(current.task.is_joined());
        assert!(current_native.closed().is_some_and(|closed| closed.matches(&current_native) && closed.had_native_acquisition()));
        peer.finish().await;
    }).await.unwrap();
}

mod domain;
