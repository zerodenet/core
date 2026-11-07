//! Warm DNS loans retire before Source pause, while real Business IO stays live.
use super::*;
mod fixture;
use fixture::*;
#[tokio::test]
async fn linked_and_standalone_warm_doh_budget_replacement_preserves_busy_business_then_closes_actual_old_owners(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        for linked in [true, false] {
            exercise(linked, true).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn linked_and_standalone_warm_doh_nonbudget_replacement_retires_only_actual_changed_graphs() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for linked in [true, false] {
            exercise(linked, false).await;
        }
    })
    .await
    .unwrap();
}
async fn exercise(linked: bool, budget: bool) {
    let mut tls = crate::dns_fixture::TlsDns::new(true).await;
    let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = peer::Peer::with_tcp(tls.address, Some(echo.local_addr().unwrap())).await;
    let (proxy, running) =
        start_server_with_business(&peer, tls.server(true), linked, budget).await;
    zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone())
        .unwrap();
    let registry = proxy.system_tun_source_registry();
    let old_source = linked.then(|| registry.owner("wg-source").unwrap());
    let entry = budget.then(|| registry.owner("entry").unwrap());
    let mut business = if let Some(entry) = &entry {
        let endpoint = *entry.original_listener_address.get().unwrap();
        let (front, remote) = tokio::time::timeout_at(end(), async {
            tokio::join!(
                connect(endpoint, echo.local_addr().unwrap().port()),
                echo.accept()
            )
        })
        .await
        .unwrap();
        Some((front, remote.unwrap().0))
    } else {
        None
    };
    if let Some((front, remote)) = business.as_mut() {
        live(front, remote).await;
    }
    let mut work = query(&proxy, "pending.warm.actual.wg.test");
    let (bytes, answer) = tokio::select! {
        request = tokio::time::timeout_at(end(),tls.requests.recv()) => request.unwrap().unwrap(),
        result = &mut work => panic!("warm WG DoH failed before actual HTTP Pending: {result:?}"),
    };
    let first = pending(&proxy).await;
    let child = first.original.as_ref().unwrap();
    let old_root = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    native_pending(child, &old_root);
    let old_devices = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(old_devices.len(), 1);
    let workers = old_devices[0].test_original_worker_witnesses();
    assert!(workers.len() >= 3 && workers.iter().all(|task| !task.is_joined()));
    let queries = proxy
        .resolver
        .native_query_observations()
        .into_iter()
        .map(|actual| actual.task)
        .collect();
    let root = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .carrier
        .clone();
    let old_limit = root.snapshot().limit;
    let mut candidate = (*proxy.engine().config()).clone();
    if budget {
        candidate.runtime.carrier_max_total_originals = Some(32);
    } else {
        candidate.outbounds[0].udp.enabled = false;
    }
    let handle = crate::ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    assert!(
        handle
            .apply_runtime_config_and_wait(candidate.clone(), Duration::from_secs(5))
            .await
            .is_err(),
        "real Pending query refuses replacement before old IO is stopped"
    );
    assert_eq!(root.snapshot().limit, old_limit);
    assert!(!first.task.is_joined() && old_root.closed().is_none());
    if let Some(source) = &old_source {
        assert!(source.is_running() && source.terminal_for_test().is_none());
    }
    if let Some((front, remote)) = business.as_mut() {
        live(front, remote).await;
        assert_eq!(proxy.engine().active_sessions().len(), 1);
    }
    answer
        .send(zero_dns::udp::build_dns_response(
            &bytes,
            &[IpAddress::V4([127, 0, 0, 1])],
        ))
        .unwrap();
    assert_eq!(
        work.await.unwrap().unwrap(),
        vec![IpAddress::V4([127, 0, 0, 1])]
    );
    idle(&proxy, &first.task, queries).await;
    assert!(
        !first.task.is_joined() && child.closed().is_none(),
        "idle DoH is still actual retained custody"
    );
    let mut resumed = None;
    if let Some((front, remote)) = business.as_mut() {
        assert!(
            handle
                .apply_runtime_config_and_wait(candidate.clone(), Duration::from_secs(5))
                .await
                .is_err(),
            "Source/Business pause refuses active original IO after idle DNS retirement"
        );
        returned_driver(&first);
        assert!(old_root.closed().is_none());
        assert!(!old_devices[0].is_closed());
        assert_eq!(root.snapshot().limit, old_limit);
        live(front, remote).await;
        // Rejection drops only the exact token's admission fence. The same
        // current provider/Source can reconnect while that Business stays live.
        let mut work = query(&proxy, "resumed.warm.actual.wg.test");
        let (bytes, answer) = tokio::select! {
            request = tokio::time::timeout_at(end(),tls.requests.recv()) => request.unwrap().unwrap(),
            result = &mut work => panic!("cold rejection did not restore actual DNS admission: {result:?}"),
        };
        let warm = pending(&proxy).await;
        native_pending(warm.original.as_ref().unwrap(), &old_root);
        assert!(
            !warm.task.same_registration(&first.task)
                && warm
                    .task
                    .scope_identity()
                    .same_scope(&first.task.scope_identity())
        );
        let queries = proxy
            .resolver
            .native_query_observations()
            .into_iter()
            .map(|actual| actual.task)
            .collect();
        answer
            .send(zero_dns::udp::build_dns_response(
                &bytes,
                &[IpAddress::V4([127, 0, 0, 1])],
            ))
            .unwrap();
        work.await.unwrap().unwrap();
        idle(&proxy, &warm.task, queries).await;
        live(front, remote).await;
        resumed = Some(warm);
    }
    let warm = resumed.as_ref().unwrap_or(&first);
    drop(business);
    if let Some(entry) = &entry {
        tokio::time::timeout_at(end(), async {
            while !entry.acquisition_idle_for_test() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    handle
        .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
        .await
        .unwrap();
    returned_driver(warm);
    assert!(old_devices[0].is_closed());
    for task in workers {
        let joined = task.try_joined().unwrap();
        assert!(task.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
    }
    let next_root = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    if budget {
        assert_eq!(root.snapshot().limit.get(), 32);
        let closed = old_root.closed().unwrap();
        assert!(closed.matches(&old_root) && closed.had_native_acquisition());
        let driver = old_root.driver().unwrap();
        let joined = driver.try_joined().unwrap();
        assert!(driver.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
        assert!(!old_root.same_original(&next_root));
        if let Some(source) = &old_source {
            let receipt = source.terminal_for_test().unwrap();
            assert_eq!(receipt.identity(), source.identity());
            assert!(
                receipt.failure().is_none()
                    && receipt.task_failures().is_empty()
                    && receipt.native_bound_failures().is_empty()
            );
            assert!(receipt.closed_io_receipts() > 0 && receipt.joined_tasks() >= 3);
            assert!(!Arc::ptr_eq(source, &registry.owner("wg-source").unwrap()));
        }
        let entry = entry.as_ref().unwrap();
        let receipt = entry.terminal_for_test().unwrap();
        assert_eq!(receipt.identity(), entry.identity());
        assert!(receipt.closed_io_receipts() > 0 && receipt.joined_tasks() >= 2);
        assert!(
            receipt.failure().is_none()
                && receipt.task_failures().is_empty()
                && receipt.native_bound_failures().is_empty()
        );
    } else if linked {
        assert!(old_root.same_original(&next_root) && old_root.closed().is_none());
        assert!(old_root
            .driver()
            .unwrap()
            .same_registration(next_root.driver().unwrap()));
        assert!(Arc::ptr_eq(
            old_source.as_ref().unwrap(),
            &registry.owner("wg-source").unwrap()
        ));
    } else {
        let closed = old_root.closed().unwrap();
        assert!(closed.matches(&old_root) && closed.had_native_acquisition());
        assert!(old_root
            .driver()
            .unwrap()
            .try_joined()
            .unwrap()
            .registration_released());
        assert!(!old_root.same_original(&next_root));
    }
    zero_dns::test_support::install_doh_ca(&proxy.resolver, "query", tls.certificate.clone())
        .unwrap();
    let mut work = query(&proxy, "successor.warm.actual.wg.test");
    let (bytes, answer) = tokio::select! {
        request=tokio::time::timeout_at(end(),tls.requests.recv())=>request.unwrap().unwrap(),
        result=&mut work=>panic!("actual new WG scope failed after old retirement: {result:?}"),
    };
    let fresh = pending(&proxy).await;
    let successor = fresh.original.as_ref().unwrap();
    native_pending(successor, &next_root);
    assert!(!fresh
        .task
        .scope_identity()
        .same_scope(&warm.task.scope_identity()));
    answer
        .send(zero_dns::udp::build_dns_response(
            &bytes,
            &[IpAddress::V4([127, 0, 0, 1])],
        ))
        .unwrap();
    work.await.unwrap().unwrap();
    let final_source = linked.then(|| registry.owner("wg-source").unwrap());
    running.shutdown().await.unwrap();
    returned(successor);
    let closed = next_root.closed().unwrap();
    assert!(closed.matches(&next_root) && closed.had_native_acquisition());
    let driver = next_root.driver().unwrap();
    let joined = driver.try_joined().unwrap();
    assert!(driver.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
    assert!(fresh.task.try_joined().unwrap().registration_released());
    assert_eq!(root.snapshot().charged, 0);
    assert!(proxy.engine().active_sessions().is_empty());
    if let Some(source) = final_source {
        let terminal = source.terminal_for_test().unwrap();
        assert_eq!(terminal.identity(), source.identity());
        assert!(
            terminal.failure().is_none()
                && terminal.task_failures().is_empty()
                && terminal.native_bound_failures().is_empty()
        );
        assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() >= 3);
    }
    peer.close().await;
    tls.close().await;
}
