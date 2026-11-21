use super::*;
#[tokio::test]
async fn system_source_budget_reload_closes_the_original_before_rebuilding_its_actual_accepted_graph(
) {
    timeout(Duration::from_secs(20), async {
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut value = serde_json::to_value(config(peer.local_addr().unwrap())).unwrap();
        value["runtime"]["inbound_max_accepted_sockets"] = serde_json::json!(2);
        let old = zero_config::RuntimeConfig::parse(&value.to_string()).unwrap();
        let proxy = Proxy::new(old.clone()).unwrap();
        let running = proxy.spawn();
        let handle = handle(&proxy);
        handle
            .apply_runtime_config_and_wait(old, Duration::from_secs(5))
            .await
            .unwrap();
        proxy
            .start_system_inbound("127.0.0.1:0".parse().unwrap(), "redirect")
            .await
            .unwrap();
        let source = proxy.inbound_owners.owner(&key("redirect")).unwrap();
        let entry = *source.original_listener_address.get().unwrap();
        exchange(&peer, entry, entry.ip()).await;
        // The same Source explicitly acknowledges idle after real accepted
        // cleanup; a client EOF alone is not its retirement receipt.
        idle(&source).await;
        value["runtime"]["inbound_max_accepted_sockets"] = serde_json::json!(1);
        handle
            .apply_runtime_config_and_wait(
                zero_config::RuntimeConfig::parse(&value.to_string()).unwrap(),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let replacement = proxy.inbound_owners.owner(&key("redirect")).unwrap();
        assert_ne!(source.identity(), replacement.identity());
        let terminal = source.terminal_for_test().unwrap();
        assert!(
            terminal.closed_io_receipts() > 0
                && terminal.joined_tasks() > 0
                && terminal.failure().is_none()
        );
        assert_eq!(*replacement.original_listener_address.get().unwrap(), entry);
        exchange(&peer, entry, entry.ip()).await;
        running.shutdown().await.unwrap();
        assert!(replacement.terminal_for_test().unwrap().failure().is_none());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn system_source_keeps_its_original_on_route_reload_and_uses_the_new_standard_node_generation(
) {
    timeout(Duration::from_secs(20), async {
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let next = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(first.local_addr().unwrap());
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = handle(&proxy);
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        proxy
            .start_system_inbound("127.0.0.1:0".parse().unwrap(), "redirect")
            .await
            .unwrap();
        let source = proxy.inbound_owners.owner(&key("redirect")).unwrap();
        let entry = *source.original_listener_address.get().unwrap();
        exchange(&first, entry, entry.ip()).await;
        idle(&source).await;
        handle
            .apply_runtime_config_and_wait(
                super::config(next.local_addr().unwrap()),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(
            &source,
            &proxy.inbound_owners.owner(&key("redirect")).unwrap()
        ));
        exchange(&next, entry, entry.ip()).await;
        running.shutdown().await.unwrap();
        assert!(source.terminal_for_test().unwrap().failure().is_none());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_public_system_readiness_wait_retains_the_same_original_source_and_native_provider(
) {
    timeout(Duration::from_secs(15), async {
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(peer.local_addr().unwrap());
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        let source = {
            let start = proxy.start_system_inbound("127.0.0.1:0".parse().unwrap(), "redirect");
            tokio::pin!(start);
            assert!(
                futures_util::poll!(&mut start).is_pending(),
                "the actual source readiness await is suspended"
            );
            proxy
                .inbound_owners
                .owner(&key("redirect"))
                .expect("installed before the original readiness wait")
        };
        source.wait_ready().await.unwrap();
        let entry = *source.original_listener_address.get().unwrap();
        exchange(&peer, entry, entry.ip()).await;
        assert!(Arc::ptr_eq(
            &source,
            &proxy.inbound_owners.owner(&key("redirect")).unwrap()
        ));
        running.shutdown().await.unwrap();
        let terminal = source.terminal_for_test().unwrap();
        assert!(
            terminal.closed_io_receipts() > 0
                && terminal.joined_tasks() > 0
                && terminal.failure().is_none()
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn repeated_system_port_zero_calls_keep_distinct_originals_with_the_same_business_tag() {
    timeout(Duration::from_secs(15), async {
        let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(peer.local_addr().unwrap());
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        for _ in 0..2 {
            proxy
                .start_system_inbound("127.0.0.1:0".parse().unwrap(), "redirect")
                .await
                .unwrap();
        }
        let originals = proxy.inbound_owners.system_tcp_owners();
        assert_eq!(originals.len(), 2);
        assert_ne!(originals[0].1.identity(), originals[1].1.identity());
        let endpoints = originals
            .iter()
            .map(|(_, owner)| *owner.original_listener_address.get().unwrap())
            .collect::<Vec<_>>();
        assert_ne!(endpoints[0], endpoints[1]);
        for entry in endpoints {
            exchange(&peer, entry, entry.ip()).await;
        }
        assert!(originals
            .iter()
            .all(|(_, owner)| !owner.task_access().cancellation().is_cancelled()));
        running.shutdown().await.unwrap();
        for (_, owner) in originals {
            assert!(
                owner.task_access().cancellation().is_cancelled(),
                "the shutdown request reaches every API Source absent from configured listeners"
            );
            let terminal = owner.terminal_for_test().unwrap();
            assert!(
                terminal.failure().is_none()
                    && terminal.closed_io_receipts() > 0
                    && terminal.joined_tasks() > 0
            );
        }
    })
    .await
    .unwrap();
}
