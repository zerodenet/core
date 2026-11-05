use super::*;
#[tokio::test]
async fn actual_proxy_native_doq_network_change_joins_old_query_and_closes_originals_before_new_io()
{
    let mut peer = Peer::new(false).await;
    let (proxy, running, _) = configured(&peer).await;
    let job = query(&proxy, "old-network.quic.test");
    let old_request = peer.request().await;
    let held = pending_quic(&proxy).await;
    let original = held.metadata.clone().unwrap();
    proxy.egress_interface_control().invalidate_network();
    let next = query(&proxy, "new-network.quic.test");
    let request = peer.request().await;
    assert!(held.task.is_joined());
    assert!(job.await.unwrap().is_err());
    confirmed(&original);
    let current = pending_quic(&proxy).await;
    let fresh = current.metadata.clone().unwrap();
    assert_eq!(original.instance(), fresh.instance());
    assert!(held
        .task
        .scope_identity()
        .same_scope(&current.task.scope_identity()));
    assert!(!original.same_original(&fresh));
    request.answer([198, 51, 100, 91]);
    next.await.unwrap().unwrap();
    drop(old_request);
    running.shutdown().await.unwrap();
    confirmed(&fresh);
    peer.close().await;
}
#[tokio::test]
async fn actual_proxy_native_doq_reload_waits_for_transitive_mieru_custody_and_original_close_before_start(
) {
    let mut peer = Peer::new(false).await;
    let a = Server::new(true).await;
    let b = Server::new(true).await;
    let c = Server::new(false).await;
    let mut config = config([a.address, b.address, c.address]);
    let zero_config::OutboundProtocolConfig::Mieru { server, .. } =
        &mut config.outbounds[0].protocol
    else {
        unreachable!()
    };
    *server = "replacement.quic.node.test".into();
    config.runtime.dns = Some(peer.config());
    let (proxy, running, handle) = start(config.clone()).await;
    peer.trust(&proxy);
    let mut held_pool = domain_loan(&proxy, &mut peer).await;
    let pool = held_pool.owner.original().clone();
    let querying = query(&proxy, "old-owner.quic.test");
    let old_request = peer.request().await;
    let held = pending_quic(&proxy).await;
    let original = held.metadata.clone().unwrap();
    config.runtime.dns.as_mut().unwrap().policy.timeout_ms = 6000;
    let replacing = tokio::spawn(async move {
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(10))
            .await
    });
    held.task.wait().await;
    assert!(querying.await.unwrap().is_err());
    assert!(!replacing.is_finished());
    peer.no_request();
    assert!(
        original.closed().is_none(),
        "incomplete real pool consumer blocks DNS dependency cleanup"
    );
    assert!(original.owner().closed().is_none());
    drop(held_pool.io);
    assert!(held_pool
        .owner
        .close(deadline())
        .await
        .unwrap()
        .is_complete());
    replacing.await.unwrap().unwrap();
    confirmed(&original);
    assert!(original.owner().closed().unwrap().matches(original.owner()));
    // A policy-only reload retains the actual TLS backend and its trust profile.
    // The fixture must not mutate a backend that has already performed I/O.
    let next = query(&proxy, "new-owner.quic.test");
    let request = peer.request().await;
    let current = pending_quic(&proxy).await;
    let fresh = current.metadata.clone().unwrap();
    assert_ne!(original.instance(), fresh.instance());
    assert!(!held
        .task
        .scope_identity()
        .same_scope(&current.task.scope_identity()));
    request.answer([198, 51, 100, 91]);
    next.await.unwrap().unwrap();
    let mut successor = domain_loan(&proxy, &mut peer).await;
    assert_ne!(pool.instance(), successor.owner.original().instance());
    successor.io.write_all(b"new DoQ owner").await.unwrap();
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
    confirmed(&fresh);
    a.close().await;
    b.close().await;
    c.close().await;
    peer.close().await;
}

async fn domain_loan(
    proxy: &Proxy,
    peer: &mut Peer,
) -> zero_platform_tokio::controlled_carrier::loan::StreamLoan {
    let borrowing = tokio::spawn({
        let proxy = proxy.clone();
        async move { loan(&proxy).await }
    });
    let request = peer.request().await;
    let bootstrap = pending_quic(proxy).await;
    request.answer([127, 0, 0, 1]);
    let borrowed = borrowing.await.unwrap();
    bootstrap.task.wait().await;
    confirmed(&bootstrap.metadata.unwrap());
    borrowed
}

#[tokio::test]
async fn actual_proxy_native_doq_worker_fault_preserves_original_error_and_still_closes_actual_native_owner(
) {
    let mut peer = Peer::new(false).await;
    let (proxy, running, _) = configured(&peer).await;
    let job = query(&proxy, "fault.quic.test");
    let request = peer.request().await;
    let held = pending_quic(&proxy).await;
    let original = held.metadata.clone().unwrap();
    let failure = zero_dns::test_support::panic_native_query_worker(&proxy.resolver).unwrap();
    let joined = failure.wait().await;
    let source = joined.result().unwrap_err();
    assert_eq!(
        joined.class(),
        zero_platform_tokio::contract::scope::TaskClass::Critical
    );
    assert!(running.shutdown().await.is_err());
    assert!(job.await.unwrap().is_err());
    assert!(held.task.is_joined());
    confirmed(&original);
    assert!(original.owner().closed().unwrap().matches(original.owner()));
    assert!(Arc::ptr_eq(
        &source,
        &failure.wait().await.result().unwrap_err()
    ));
    drop(request);
    peer.close().await;
}
