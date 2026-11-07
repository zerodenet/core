//! A negotiated Domain BND is resolved by the original Node query before UDP.
use super::*;
mod peer;

#[tokio::test]
async fn domain_bnd_keeps_the_actual_node_query_and_both_socks_originals_until_final_retirement() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let prefix = peer::Peer::new().await;
        let server = Server::packet(true).await;
        let mut dns = Dns::new().await;
        let mut settings = detoured(&dns, &server);
        let zero_config::OutboundProtocolConfig::Mieru { transport, .. } =
            &mut settings.outbounds[0].protocol
        else {
            unreachable!()
        };
        *transport = zero_config::MieruTransport::Udp;
        settings.outbounds.push(prefix.node.clone());
        settings.outbound_groups = zero_config::RuntimeConfig::parse(
            r#"{"outbounds":[{"tag":"socks","protocol":{"type":"direct"}},{"tag":"a","protocol":{"type":"direct"}}],"outbound_groups":[{"tag":"packet-chain","type":"relay","proxies":["socks","a"]}],"route":{"final":{"type":"direct"}}}"#,
        ).unwrap().outbound_groups;
        let zero_config::DnsServerConfig::Udp { detour, .. } = settings.runtime.dns.as_mut()
            .unwrap().servers.get_mut("query").unwrap()
        else {
            unreachable!()
        };
        *detour = Some("packet-chain".into());
        let (proxy, running, _) = start(settings).await;
        let resolver = proxy.resolver.clone();
        let mut first = tokio::spawn(async move {
            resolver.resolve_real_type("first.domain-bnd.test", 1).await
        });
        let bootstrap = tokio::select! {
            request = dns.request() => request,
            result = &mut first => panic!("actual Domain BND bootstrap failed: {result:?}"),
        };
        assert!(!bootstrap.tcp, "literal control needs only the negotiated BND lookup");
        let node = pending(&proxy).await;
        let node_original = node.original.as_ref().unwrap();
        assert!(node_original.parent().is_none());
        assert_eq!(server.accepts.load(Ordering::SeqCst), 0);
        bootstrap.answer(false, false);
        let node_joined = tokio::time::timeout_at(deadline(), node.task.wait_registration_released()).await.unwrap();
        node_joined.result().unwrap();
        closed(node_original);
        assert!(node.task.matches_joined(&node_joined) && node_joined.registration_released());
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut first => panic!("resolved BND must carry the original DNS request: {result:?}"),
        };
        assert!(request.tcp);
        let first_caller = pending(&proxy).await;
        assert!(!first_caller.task.scope_identity().same_scope(&node.task.scope_identity()));
        let child = first_caller.original.as_ref().unwrap().clone();
        let pool = child.parent().unwrap().clone();
        let udp = pool.native_origin().clone();
        let control = udp.related_control().unwrap();
        assert!(udp.parent().is_none());
        assert!(!udp.same_original(&control));
        assert_eq!(udp.endpoints().unwrap().peer(), prefix.relay);
        assert_eq!(control.endpoints().unwrap().peer(), prefix.control);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        drop(request);
        tokio::time::timeout_at(deadline(), first_caller.task.wait()).await.unwrap().result().unwrap();
        assert!(child.closed().unwrap().matches(&child));
        assert!(udp.closed().is_none() && control.closed().is_none());
        assert!(!pool.driver().unwrap().is_joined());
        let resolver = proxy.resolver.clone();
        let mut second = tokio::spawn(async move {
            resolver.resolve_real_type("second.domain-bnd.test", 1).await
        });
        let request = tokio::select! {
            request = dns.request() => request,
            result = &mut second => panic!("the same negotiated pair must survive caller cancellation: {result:?}"),
        };
        assert!(request.tcp, "reuse must not repeat the Node bootstrap");
        let next = pending(&proxy).await;
        let same_pool = next.original.as_ref().unwrap().parent().unwrap();
        assert!(same_pool.same_original(&pool));
        assert!(same_pool.native_origin().same_original(&udp));
        assert!(same_pool.native_origin().related_control().unwrap().same_original(&control));
        assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        request.answer(false, false);
        second.await.unwrap().unwrap();
        tokio::time::timeout_at(deadline(), next.task.wait()).await.unwrap().result().unwrap();
        dns.no_new_request();
        assert!(proxy.engine().active_sessions().is_empty());
        assert!(proxy.engine().completed_sessions().is_empty());
        running.shutdown().await.unwrap();
        assert!(pool.driver().unwrap().is_joined());
        let driver = pool.driver().unwrap();
        let joined = tokio::time::timeout_at(deadline(), driver.wait_registration_released()).await.unwrap();
        joined.result().unwrap();
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        native_closed(&udp);
        native_closed(&control);
        prefix.close().await;
        server.close().await;
        dns.close().await;
    }).await.unwrap();
}
