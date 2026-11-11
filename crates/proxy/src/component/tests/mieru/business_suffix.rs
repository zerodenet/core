//! A standard business consumer borrows the persistent prefix's real original.
use super::*;
use std::sync::Arc;

#[tokio::test]
async fn persistent_prefix_business_suffix_keeps_original_custody_through_confirmed_replacement() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let a = Server::new(true).await;
        let b = Server::new(true).await;
        let c = Server::new(false).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let suffix_port = listener.local_addr().unwrap().port();
        let stop = zero_platform_tokio::contract::CancellationToken::new();
        let stopped = stop.clone();
        let peer = tokio::spawn(async move {
            let mut callers = tokio::task::JoinSet::new();
            loop {
                let (mut io, _) = tokio::select! {biased;
                    _ = stopped.cancelled() => break,
                    result = listener.accept() => result.unwrap(),
                };
                let stopped = stopped.clone();
                callers.spawn(async move {
                    tokio::select! {biased;
                        _ = stopped.cancelled() => {},
                        _ = async {
                            let target = crate::adapters::socks5::relay_tests::fixtures::request(&mut io).await;
                            assert_eq!(target, (zero_core::Address::Ipv4([203, 0, 113, 1]), 443));
                            crate::adapters::socks5::relay_tests::fixtures::accept(&mut io).await;
                            let (mut read, mut write) = tokio::io::split(io);
                            tokio::io::copy(&mut read, &mut write).await.unwrap();
                        } => {},
                    }
                });
            }
            while let Some(result) = callers.join_next().await {
                result.unwrap();
            }
        });
        let mut config = config([a.address, b.address, c.address]);
        let extra = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "inbounds":[{"tag":"entry","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"socks5"}}],
            "outbounds":[{"tag":"suffix","protocol":{"type":"socks5","server":"127.0.0.1","port":suffix_port}}],
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        config.inbounds = extra.inbounds;
        config.outbounds.extend(extra.outbounds);
        let zero_config::OutboundGroupKind::Relay { proxies } = &mut config.outbound_groups[0].group else {
            unreachable!()
        };
        // Keep c as a retained logical consumer of the same a/b prefix. The
        // accepted Business independently borrows b for its taskless suffix.
        assert_eq!(proxies.pop().as_deref(), Some("c"));
        proxies.push("suffix".into());
        config.route.final_action = zero_config::RouteActionConfig::Route { outbound: "chain".into() };
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5)).await.unwrap();
        let mut held = loan(&proxy).await;
        let original = held.owner.original().clone();
        let plan = proxy.tcp_runtime_services().components.as_ref().unwrap().clone();
        for tag in ["a", "b", "c", "unused"] {
            let slot = plan.configured(tag).expect("same actual native pool, no compatibility claim");
            assert_eq!(slot.recipe.spec().dependencies.len(), 1);
            let snapshot = proxy.engine().runtime_snapshot();
            let (resolved, _) = proxy.engine().resolve_route_decision_in_snapshot(
                &snapshot,
                zero_engine::RouteDecision::Route(tag.into()),
            ).unwrap();
            let zero_engine::ResolvedOutbound::Single(leaf) = resolved else {
                unreachable!()
            };
            assert!(proxy.protocols.claim_outbound_leaf(snapshot.config(), leaf).is_err(),
                "the migrated pool must not recreate a claim/prepared executor");
        }
        // The real Service entrypoint must use the same finite receiver and
        // original Mieru driver as accepted Business, without a bare callback.
        let mut probe = service(&proxy).await;
        probe.write_all(b"service").await.unwrap();
        let mut probe_bytes = [0; 7];
        probe.read_exact(&mut probe_bytes).await.unwrap();
        assert_eq!(&probe_bytes, b"service");
        let suffix_owner = plan.configured("suffix").unwrap().original_carrier_owner(0).unwrap();
        let service_observed = suffix_owner.observe().unwrap();
        assert_eq!(service_observed.slots().len(), 1);
        let service_original = service_observed.slots()[0].original().clone();
        assert!(service_observed.slots()[0].borrowed_origin().unwrap().same_original(original.parent().unwrap()));
        drop(probe);
        let bound = tokio::time::timeout_at(deadline(), async {
            loop {
                if let Some(owner) = proxy.system_tun_source_registry().owner("entry") {
                    owner.wait_ready().await.unwrap();
                    break *owner.original_listener_address.get().unwrap();
                }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let mut client = tokio::net::TcpStream::connect(bound).await.unwrap();
        client.write_all(&[5, 1, 0]).await.unwrap();
        let mut greeting = [0; 2];
        client.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 0]);
        client.write_all(&[5, 1, 0, 1, 203, 0, 113, 1, 1, 187]).await.unwrap();
        let mut reply = [0; 10];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply[..2], &[5, 0]);
        client.write_all(b"real-prefix").await.unwrap();
        let mut response = [0; 11];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"real-prefix");
        let suffix = plan.configured("suffix").unwrap();
        let registration = suffix.original_carrier_owner(0).unwrap();
        let observed = registration.observe().unwrap();
        assert_eq!(observed.slots().len(), 1);
        let last = &observed.slots()[0];
        assert!(service_original.matches_closed(&service_original.closed().expect("Service original settled before the next actual acquisition")));
        let final_original = last.original().clone();
        let donor = last.borrowed_origin().expect("actual persistent donor, never a bare legacy stream");
        assert!(donor.same_original(original.parent().unwrap()));
        assert!(last.driver().is_none(), "no driver invented for the taskless suffix");
        assert!(original.driver().is_some());
        let scope = original.driver().unwrap().scope_identity();
        let mut parent = original.parent();
        while let Some(current) = parent {
            assert_eq!(current.instance(), original.instance());
            assert!(scope.same_scope(&current.driver().unwrap().scope_identity()));
            parent = current.parent();
        }
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1, "one physical pool for background and business callers");
        }
        client.shutdown().await.unwrap();
        let mut tail = Vec::new();
        client.read_to_end(&mut tail).await.unwrap();
        assert!(tail.is_empty());
        drop(client);
        tokio::time::timeout_at(deadline(), async {
            while !proxy.engine().active_sessions().is_empty()
                || !final_original.closed().is_some_and(|closed| final_original.matches_closed(&closed))
            {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let completed = proxy.engine().completed_sessions();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].outbound_tag.as_deref(), Some("suffix"));
        assert_eq!((completed[0].outbound_tx_bytes, completed[0].outbound_rx_bytes), (11, 11));
        assert!(completed[0].failure.is_none());
        let zero_config::OutboundProtocolConfig::Mieru { password, .. } = &mut config.outbounds[3].protocol else {
            unreachable!()
        };
        *password = "unused credential replaced".into();
        let replacing = tokio::spawn(async move {
            handle.apply_runtime_config_and_wait(config, Duration::from_secs(10)).await
        });
        original.driver().unwrap().wait().await;
        assert!(!replacing.is_finished(), "driver join is not logical control return");
        assert!(try_loan(&proxy).await.is_err());
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 1, "no new native I/O before old cleanup");
        }
        drop(held.io);
        assert!(held.owner.close(deadline()).await.unwrap().is_complete());
        replacing.await.unwrap().unwrap();
        let mut current = Some(&original);
        while let Some(prior) = current {
            assert!(prior.closed().unwrap().matches(prior));
            let driver = prior.driver().unwrap();
            assert!(driver.matches_joined(&driver.try_joined().unwrap()));
            current = prior.parent();
        }
        let mut fresh = loan(&proxy).await;
        assert!(!fresh.owner.original().same_original(&original));
        for server in [&a, &b, &c] {
            assert_eq!(server.accepts.load(Ordering::SeqCst), 2);
        }
        // The cached plan is intentionally retained; it must not keep native
        // resources live or let an old role open another physical carrier.
        assert!(!Arc::ptr_eq(&plan, proxy.tcp_runtime_services().components.as_ref().unwrap()));
        drop(fresh.io);
        assert!(fresh.owner.close(deadline()).await.unwrap().is_complete());
        running.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
        c.close().await;
        stop.cancel();
        peer.await.unwrap();
    }).await.unwrap();
}
