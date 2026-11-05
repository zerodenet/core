//! Same actual Proxy, resolver, host query Scope and original native controls.
use super::*;
use std::sync::Arc;
use zero_platform_tokio::contract::CancellationToken;
mod fixture;
use fixture::Dns;
mod borrowed;
mod encrypted;
mod faults;
mod limits;
mod open_failure;
mod quic;
mod replacement;
mod system;

async fn pending(proxy: &Proxy) -> zero_dns::OwnedDnsQueryObservation {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(query) =
                proxy
                    .resolver
                    .native_query_observations()
                    .into_iter()
                    .find(|query| {
                        query.io_pending && query.original.is_some() && !query.task.is_joined()
                    })
            {
                return query;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual native query must be suspended in I/O")
}
fn original(
    query: &zero_dns::OwnedDnsQueryObservation,
) -> zero_platform_tokio::controlled_carrier::CarrierOrigin {
    let original = query.original.clone().unwrap();
    assert!(
        original.driver().is_none(),
        "native UDP/TCP custody must not invent a driver"
    );
    original
}
fn closed(original: &zero_platform_tokio::controlled_carrier::CarrierOrigin) {
    let closed = original
        .closed()
        .expect("actual original native controller receipt");
    assert!(closed.matches(original) && closed.had_native_acquisition());
}
async fn start(config: zero_config::RuntimeConfig) -> (Proxy, crate::RunningProxy, ProxyHandle) {
    let proxy = Proxy::new(config.clone()).unwrap();
    let running = proxy.spawn();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    handle
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
    (proxy, running, handle)
}

#[tokio::test]
async fn actual_proxy_native_dns_original_udp_closes_before_tcp_fallback_and_domain_mieru_uses_same_pool(
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut dns = Dns::new().await;
        let a = Server::new(true).await;
        let b = Server::new(true).await;
        let c = Server::new(false).await;
        let mut config = config([a.address, b.address, c.address]);
        config.runtime.dns = Some(dns.config());
        if let zero_config::OutboundProtocolConfig::Mieru { server, .. } =
            &mut config.outbounds[0].protocol
        {
            *server = "native.node.test".into();
        }
        let (proxy, running, _) = start(config).await;
        let task = {
            let proxy = proxy.clone();
            tokio::spawn(async move { service(&proxy).await })
        };
        let request = dns.request().await;
        assert!(!request.tcp);
        let udp_query = pending(&proxy).await;
        let udp = original(&udp_query);
        assert!(udp.closed().is_none());
        assert_eq!(a.accepts.load(Ordering::SeqCst), 0);
        request.answer(true, false);
        let request = dns.request().await;
        assert!(request.tcp);
        closed(&udp);
        let tcp_query = pending(&proxy).await;
        let tcp = original(&tcp_query);
        assert!(
            tcp_query.task.same_registration(&udp_query.task),
            "fallback stays inside the original supervised query"
        );
        assert!(!tcp.same_original(&udp));
        request.answer(false, false);
        let mut stream = task.await.unwrap();
        stream.write_all(b"native DNS").await.unwrap();
        stream.read_exact(&mut [0; 10]).await.unwrap();
        drop(stream);
        udp_query.task.wait().await;
        closed(&tcp);
        assert!(proxy.engine().active_sessions().is_empty());
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        }
        let mut second = loan(&proxy).await;
        second.io.write_all(b"shared").await.unwrap();
        second.io.read_exact(&mut [0; 6]).await.unwrap();
        drop(second.io);
        assert!(second.owner.close(deadline()).await.unwrap().is_complete());
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        }
        dns.no_new_request();
        running.shutdown().await.unwrap();
        assert!(udp.owner().closed().unwrap().matches(udp.owner()));
        assert!(tcp.owner().closed().unwrap().matches(tcp.owner()));
        a.close().await;
        b.close().await;
        c.close().await;
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_native_dns_backend_fallback_requires_original_close_and_keeps_the_query_scope(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut first = Dns::new().await;
        let mut next = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        let mut dns = first.config();
        dns.servers.insert("next".into(), next.server());
        dns.policy.node_server = Some("native".into());
        dns.policy.node_fallback_servers = vec!["next".into()];
        config.runtime.dns = Some(dns);
        let (proxy, running, _) = start(config).await;
        let query = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("fallback.node.test").await })
        };
        let request = first.request().await;
        let previous = pending(&proxy).await;
        let prior = original(&previous);
        next.no_new_request();
        request.answer(false, true);
        let request = next.request().await;
        closed(&prior);
        let current = pending(&proxy).await;
        assert!(previous.task.same_registration(&current.task));
        let successor = original(&current);
        request.answer(false, false);
        assert_eq!(
            query.await.unwrap().unwrap(),
            vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]
        );
        current.task.wait().await;
        closed(&successor);
        running.shutdown().await.unwrap();
        first.close().await;
        next.close().await;
    })
    .await
    .unwrap();
}
