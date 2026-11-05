use super::*;

#[tokio::test]
async fn actual_confirmed_device_transaction_starts_resolved_graph_and_joins_its_same_workers() {
    let proxy = crate::Proxy::new(proxy_config()).unwrap();
    let upstream = proxy.tcp_runtime_services().upstream();
    let adapter = WireguardAdapter::default();
    let recipe = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let pending = recipe.pending();
    let guard = install(&adapter, &recipe);
    let request = demand(&recipe);
    let catalog = ComponentCatalog::with_factories(crate::register::component_registry());
    let config = Arc::new(proxy_config());
    let mut invocation = None;
    let mut actual_id = None;
    let plan = catalog
        .prepare_with_device_resources(config.clone(), |preparation| {
            let (id, operation) = recipe.into_component(preparation).map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::Io,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
            actual_id = Some(id);
            invocation = Some(operation);
            Ok(std::collections::BTreeSet::from([id]))
        })
        .unwrap();
    let slot = plan.device_component(actual_id.unwrap()).unwrap();
    assert!(catalog
        .start_device_components(config.clone(), deadline())
        .await
        .is_err());
    assert!(pending.peer_stack(0).unwrap().device().is_none());
    catalog.start(config.clone(), deadline()).await.unwrap();
    let resolved = invocation
        .unwrap()
        .resolve(&upstream, request)
        .await
        .unwrap();
    assert!(pending.peer_stack(0).unwrap().device().is_none());
    catalog
        .start_device_components(config.clone(), deadline())
        .await
        .unwrap();
    let link = resolved.install().unwrap();
    let device = link.state.lock().unwrap().devices[0].clone();
    let (sender, mut packets) = tokio::sync::mpsc::channel(4);
    drop(link.sender.send_replace(sender));
    let mut connecting = Box::pin(device.open_tcp_observed(
        "10.0.0.1".parse().unwrap(),
        "192.0.2.2:443".parse().unwrap(),
        None,
    ));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let syn = tokio::time::timeout_at(deadline(), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(zero_stack::packet::parse_tcp(&syn.packet).unwrap().syn);
    let witnesses = pending.peer_stack(0).unwrap().test_original_witnesses();
    catalog.commit(&config, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
    assert_eq!(witnesses.len(), 3);
    assert!(witnesses.iter().all(|witness| witness.is_joined()));
    assert!(pending.peer_stack(0).unwrap().is_drained());
    assert!(device.is_closed());
    assert!(connecting.await.is_err());
    // A virtual stack graph issues no physical-listener close receipt.
    assert!(slot.recipe.spec().resources.is_empty());
    drop(guard);
}
