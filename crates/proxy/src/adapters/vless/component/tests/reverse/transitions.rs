use super::*;
#[tokio::test]
async fn reverse_metadata_sources_keep_the_real_native_provider_and_driver() {
    timeout(Duration::from_secs(60), async {
        let files = tempfile::tempdir().unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let ca = files.path().join("source.pem");
        let key = files.path().join("source.key");
        std::fs::write(&ca, cert.cert.pem()).unwrap();
        std::fs::write(&key, cert.signing_key.serialize_pem()).unwrap();
        for (incoming, outgoing) in [
            (
                serde_json::json!({"mkcp":{}}),
                serde_json::json!({"mkcp":{}}),
            ),
            (
                serde_json::json!({"quic":{"cert_path":ca,"key_path":key}}),
                serde_json::json!({"quic":{"server_name":"localhost","ca_cert_path":ca}}),
            ),
        ] {
            let (portal, running, socks, _bridge, bridge_running) =
                pair_carriers(incoming, outgoing, None).await;
            let plan = portal.tcp_runtime_services().components.clone().unwrap();
            let provider = plan
                .configured("portal")
                .unwrap()
                .accepted_carrier_provider()
                .unwrap();
            let original = provider.originals().into_iter().next().unwrap();
            assert!(original.listener().is_some() && original.driver().is_some());
            let source = portal.inbound_owners.owner("entry").unwrap();
            assert!(provider.sources().contains(&source.identity()));
            framed::exercise(socks).await;
            empty_and_multiple_targets(socks).await;
            assert!(
                original.closed().is_none(),
                "logical traffic return does not close the native Metadata parent"
            );
            running.shutdown().await.unwrap();
            bridge_running.shutdown().await.unwrap();
            assert!(provider.returned().is_some());
            assert!(original.closed().unwrap().matches(&original));
            let terminal = source.terminal_for_test().unwrap();
            assert!(terminal.failure().is_none() && terminal.task_failures().is_empty());
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn source_budget_replacement_retires_reverse_originals_before_building_successors() {
    timeout(Duration::from_secs(40), async {
        let (portal, running, socks, bridge, bridge_running) =
            pair(serde_json::json!({}), None).await;
        framed::exercise_tcp(socks).await;
        let old_source = portal.inbound_owners.owner("entry").unwrap();
        let plan = portal.tcp_runtime_services().components.clone().unwrap();
        let old = plan.configured("portal").unwrap();
        let provider = old.accepted_carrier_provider().unwrap();
        let originals = provider.originals();
        timeout(Duration::from_secs(5), async {
            while !provider.source_views_returned(old_source.identity()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut document = serde_json::to_value(portal.engine().config().as_ref()).unwrap();
        document["runtime"]["component_total_max_tasks"] = serde_json::json!(
            portal
                .engine()
                .config()
                .runtime
                .effective_component_total_max_tasks()
                + 1
        );
        let candidate = zero_config::RuntimeConfig::parse(&document.to_string()).unwrap();
        crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(portal.engine().clone()),
            portal.clone(),
        )
        .apply_runtime_config_and_wait(candidate, Duration::from_secs(10))
        .await
        .unwrap();
        assert!(old_source.terminal_for_test().unwrap().failure().is_none());
        assert!(provider.returned().is_some());
        for original in originals {
            assert!(original.closed().unwrap().matches(&original));
        }
        let source = portal.inbound_owners.owner("entry").unwrap();
        source.wait_ready().await.unwrap();
        let wire = *source.original_listener_address.get().unwrap();
        let mut config = (*bridge.engine().config()).clone();
        let zero_config::OutboundProtocolConfig::Vless { port, .. } =
            &mut config.outbounds[0].protocol
        else {
            unreachable!()
        };
        *port = wire.port();
        crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(bridge.engine().clone()),
            bridge.clone(),
        )
        .apply_runtime_config_and_wait(config, Duration::from_secs(10))
        .await
        .unwrap();
        let current = portal
            .tcp_runtime_services()
            .components
            .clone()
            .unwrap()
            .configured("portal")
            .unwrap();
        assert_ne!(current.instance_id(), old.instance_id());
        let actual = current.accepted_carrier_provider().unwrap();
        timeout(Duration::from_secs(5), async {
            while actual.originals().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let clients = portal.inbound_owners.owner("clients").unwrap();
        clients.wait_ready().await.unwrap();
        framed::exercise_tcp(*clients.original_listener_address.get().unwrap()).await;
        running.shutdown().await.unwrap();
        bridge_running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
