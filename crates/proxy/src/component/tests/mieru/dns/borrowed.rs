//! Query custody returns independently from the same real donor's retirement.
use super::*;
mod concurrent;
mod doh;
#[cfg(all(feature = "vless", feature = "vmess", feature = "trojan"))]
mod taskless;

fn detoured(dns: &Dns, server: &Server) -> zero_config::RuntimeConfig {
    let mut config = config([server.address; 3]);
    config.outbounds.truncate(1);
    config.outbound_groups.clear();
    let mut settings = dns.config();
    let mut detour = dns.server();
    let zero_config::DnsServerConfig::Udp { detour: target, .. } = &mut detour else {
        unreachable!()
    };
    *target = Some("a".into());
    settings.servers.insert("query".into(), detour);
    settings.default_server = "query".into();
    settings.policy.node_server = Some("native".into());
    config.runtime.dns = Some(settings);
    config
}

#[tokio::test]
async fn actual_dns_queries_return_borrowed_originals_and_reuse_the_same_mieru_donor() {
    borrowed(false, false).await;
}

#[tokio::test]
async fn actual_pending_dns_cancellation_returns_its_loan_without_retiring_the_mieru_donor() {
    borrowed(true, false).await;
}

#[tokio::test]
async fn actual_domain_mieru_detour_uses_a_distinct_bootstrap_scope_and_the_same_donor_pool() {
    borrowed(false, true).await;
}

async fn borrowed(cancel: bool, domain: bool) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = Dns::new().await;
        let server = Server::new(true).await;
        let mut settings = detoured(&dns, &server);
        if domain {
            let zero_config::OutboundProtocolConfig::Mieru { server, .. } = &mut settings.outbounds[0].protocol else { unreachable!() };
            *server = "borrowed.node.test".into();
        }
        let (proxy, running, handle) = start(settings.clone()).await;
        let mut query = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move { resolver.resolve_real_type("first.borrowed.test", 1).await }
        });
        let bootstrap = if domain {
            let request = tokio::select! {
                request = dns.request() => request,
                result = &mut query => panic!("detour query failed before bootstrap: {result:?}"),
            };
            assert!(!request.tcp);
            let bootstrap = pending(&proxy).await;
            let original = bootstrap.original.as_ref().unwrap();
            assert!(original.parent().is_none());
            request.answer(false, false);
            bootstrap.task.wait().await;
            closed(original);
            Some(bootstrap)
        } else { None };
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut query => panic!("DNS query returned before its actual request reached the fixture: {result:?}"),
        };
        assert!(request.tcp);
        if let Some(bootstrap) = &bootstrap {
            let query = pending(&proxy).await;
            assert!(!query.task.scope_identity().same_scope(&bootstrap.task.scope_identity()),
                "bootstrap and the borrowing Default query must have distinct actual owners");
        }
        let first = pending(&proxy).await;
        let child = first.original.clone().expect("actual DNS child original");
        let donor = child
            .parent()
            .expect("actual selected donor lineage")
            .clone();
        let native = donor.native_origin().clone();
        assert!(child.closed().is_none());
        assert!(native.closed().is_none());
        assert!(!first.task.is_joined());
        assert!(!native.driver().unwrap().is_joined());
        assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        if cancel {
            query.abort();
            assert!(query.await.unwrap_err().is_cancelled());
            drop(request);
        } else {
            request.answer(false, false);
            assert_eq!(
                query.await.unwrap().unwrap(),
                vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]
            );
        }
        tokio::time::timeout_at(deadline(), first.task.wait())
            .await
            .unwrap();
        let returned = child.closed().expect("matching borrowed slot receipt");
        assert!(returned.matches(&child));
        assert!(
            !returned.had_native_acquisition(),
            "logical return cannot claim the donor's native socket closed"
        );
        assert!(native.closed().is_none());
        assert!(!native.driver().unwrap().is_joined());
        handle
            .apply_runtime_config_and_wait(settings, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(native.closed().is_none());
        assert!(!native.driver().unwrap().is_joined());
        let mut query = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move { resolver.resolve_real_type("second.borrowed.test", 1).await }
        });
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut query => panic!("DNS query returned before its actual request reached the fixture: {result:?}"),
        };
        assert!(request.tcp);
        let second = pending(&proxy).await;
        let next = second.original.clone().unwrap();
        assert!(!next.same_original(&child));
        assert!(next.parent().unwrap().same_original(&donor));
        assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        request.answer(false, false);
        query.await.unwrap().unwrap();
        second.task.wait().await;
        assert!(next.closed().unwrap().matches(&next));
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert!(native.driver().unwrap().is_joined());
        let closed = native.closed().expect("original donor controller receipt");
        assert!(closed.matches(&native) && closed.had_native_acquisition());
        assert!(native.owner().closed().unwrap().matches(native.owner()));
        assert!(native
            .owner()
            .users_closed()
            .unwrap()
            .matches(native.owner()));
        server.close().await;
        dns.close().await;
    })
    .await
    .unwrap();
}
