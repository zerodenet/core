//! The ordinary owning Scope must reuse its one real task credit after setup.
use super::*;

#[tokio::test]
async fn ordinary_unobserved_nested_pool_uses_its_actual_declared_pending_graph_and_total_three_carriers(
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        let a = Server::new(true).await;
        let b = Server::new(true).await;
        let c = Server::new(false).await;
        let mut config = config([a.address, b.address, c.address]);
        config.runtime.mieru_max_carriers = Some(3);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        )
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
        // This existing service fixture has no external observer and denies
        // caller I/O. Each real prefix comes from this same installed pool.
        let mut held = loan(&proxy).await;
        echo(&mut held).await;
        let mut originals = vec![];
        let mut current = Some(held.owner.original());
        while let Some(original) = current {
            assert_eq!(original.owner().limit().get(), 3);
            assert!(original.driver().is_some());
            originals.push(original.clone());
            current = original.parent();
        }
        assert_eq!(originals.len(), 3);
        let pool_instance = originals[0].instance();
        let pool_scope = originals[0].driver().unwrap().scope_identity();
        for (index, original) in originals.iter().enumerate() {
            // Each fixed endpoint has its own actual native provider, while
            // all profiles share one protocol ManagedState and owning Scope.
            assert_eq!(original.instance(), pool_instance);
            assert!(original
                .driver()
                .unwrap()
                .scope_identity()
                .same_scope(&pool_scope));
            for other in &originals[..index] {
                assert!(!original.same_original(other));
                assert!(!original.owner().same_owner(other.owner()));
            }
            let services = proxy.tcp_runtime_services();
            let slot = services
                .components
                .as_ref()
                .unwrap()
                .configured(["c", "b", "a"][index])
                .unwrap();
            assert!(slot.dependency_instances().contains(&pool_instance));
        }
        let error = open(&proxy, "unused")
            .await
            .err()
            .expect("three active carriers exhaust the same protocol pool across all profiles");
        assert_eq!(error.reason, Reason::Capacity, "{error}");
        echo(&mut held).await;
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
        }
        assert!(proxy.engine().active_sessions().is_empty());
        returned(held).await;
        running.shutdown().await.unwrap();
        for original in originals {
            let driver = original.driver().unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            assert!(original.closed().unwrap().matches(&original));
            assert!(original.owner().closed().unwrap().matches(original.owner()));
        }
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_one_carrier_preparation_and_driver_share_one_scope_credit_for_both_underlays() {
    tokio::time::timeout(Duration::from_secs(25), async {
        for udp in [false, true] {
            let server = if udp {
                Server::packet(false).await
            } else {
                Server::new(false).await
            };
            let mut config = config([server.address; 3]);
            config.runtime.mieru_max_carriers = Some(1);
            if udp {
                for node in &mut config.outbounds {
                    let zero_config::OutboundProtocolConfig::Mieru { transport, .. } =
                        &mut node.protocol
                    else {
                        unreachable!()
                    };
                    *transport = zero_config::MieruTransport::Udp;
                }
            }
            let proxy = Proxy::new(config.clone()).unwrap();
            let running = proxy.spawn();
            ProxyHandle::new(
                zero_engine::EngineHandle::new(proxy.engine().clone()),
                proxy.clone(),
            )
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
            assert_eq!(server.accepts.load(Ordering::SeqCst), 0);
            let mut first = open(&proxy, "a").await.unwrap();
            echo(&mut first).await;
            let original = first.owner.original().clone();
            assert_eq!(original.owner().limit().get(), 1);
            let driver = original
                .driver()
                .expect("same actual registered driver")
                .clone();
            let mut second = open(&proxy, "a").await.unwrap();
            echo(&mut second).await;
            assert!(second.owner.original().same_original(&original));
            returned(first).await;
            assert!(!driver.is_joined());
            assert!(original.closed().is_none());
            echo(&mut second).await;
            let mut third = open(&proxy, "a").await.unwrap();
            echo(&mut third).await;
            assert!(third.owner.original().same_original(&original));
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
            returned(second).await;
            returned(third).await;
            running.shutdown().await.unwrap();
            let joined = driver.wait_registration_released().await;
            assert!(driver.matches_joined(&joined) && joined.registration_released());
            let native = original.closed().expect("actual original native closure");
            assert!(native.matches(&original) && native.had_native_acquisition());
            assert!(original.owner().closed().unwrap().matches(original.owner()));
            server.close().await;
        }
    })
    .await
    .unwrap();
}
