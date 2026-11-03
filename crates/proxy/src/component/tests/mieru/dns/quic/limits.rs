use super::*;
#[tokio::test]
async fn actual_proxy_native_doq_and_plain_udp_charge_one_global_original_budget_and_cleanup_scope()
{
    let mut peer = Peer::new(false).await;
    let mut udp = Dns::new().await;
    let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
    let mut settings = peer.config();
    settings.servers.insert("udp".into(), udp.server());
    settings.default_server = "udp".into();
    settings.policy.timeout_ms = 10000;
    config.runtime.dns = Some(settings);
    let (proxy, running, _) = start(config).await;
    peer.trust(&proxy);
    let quic = query(&proxy, "bounded.quic.test");
    let qrequest = peer.request().await;
    let first = pending_quic(&proxy).await;
    let mut jobs = Vec::new();
    let mut requests = Vec::new();
    // One metadata original reserves its real main + probe allowance. The
    // other fourteen actual originals are plain native UDP, in the same scope.
    for index in 0..crate::component::native_dns::LIMIT - 2 {
        let resolver = proxy.resolver.clone();
        jobs.push(tokio::spawn(async move {
            resolver
                .resolve_real_type(&format!("bounded-{index}.udp.test"), 1)
                .await
        }));
        requests.push(udp.request().await);
    }
    let held = proxy.resolver.native_query_observations();
    assert_eq!(held.len(), crate::component::native_dns::LIMIT - 1);
    assert!(held.iter().all(|query| query.io_pending
        && !query.task.is_joined()
        && query
            .task
            .scope_identity()
            .same_scope(&first.task.scope_identity())));
    let error = proxy
        .resolver
        .resolve_real_type("too-many.udp.test", 1)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    udp.no_new_request();
    peer.no_request();
    running.shutdown().await.unwrap();
    assert!(quic.await.unwrap().is_err());
    for job in jobs {
        assert!(job.await.unwrap().is_err());
    }
    for observation in held {
        assert!(observation.task.is_joined());
        if let Some(original) = observation.metadata {
            confirmed(&original);
            assert!(original.owner().closed().unwrap().matches(original.owner()));
        }
        if let Some(original) = observation.original {
            closed(&original);
        }
    }
    drop(qrequest);
    drop(requests);
    peer.close().await;
    udp.close().await;
}
#[tokio::test]
async fn actual_proxy_native_doq_verifies_hostname_and_never_uses_an_unowned_udp_fallback_after_failed_open(
) {
    let mut peer = Peer::new(true).await;
    let mut udp = Dns::new().await;
    let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
    let mut settings = peer.config();
    settings.servers.insert("udp".into(), udp.server());
    settings.policy.node_fallback_servers = vec!["udp".into()];
    if let zero_config::DnsServerConfig::Doq { server_name, .. } =
        settings.servers.get_mut("native").unwrap()
    {
        *server_name = Some("wrong.quic.test".into());
    }
    config.runtime.dns = Some(settings);
    let (proxy, running, _) = start(config).await;
    peer.trust(&proxy);
    let job = query(&proxy, "bad-name.quic.test");
    let release = peer.initial().await;
    let held = pending_quic(&proxy).await;
    let original = held.metadata.clone().unwrap();
    assert!(original.closed().is_none());
    release.send(()).unwrap();
    assert!(job.await.unwrap().is_err());
    udp.no_new_request();
    peer.no_request();
    running.shutdown().await.unwrap();
    assert!(held.task.is_joined());
    confirmed(&original);
    assert!(original.owner().closed().unwrap().matches(original.owner()));
    peer.close().await;
    udp.close().await;
}
