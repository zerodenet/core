use super::*;

#[tokio::test]
async fn unchanged_candidate_reuses_actual_factory_peer_owners_and_pending_tcp() {
    let proxy = crate::Proxy::new(proxy_config()).unwrap();
    let upstream = proxy.tcp_runtime_services().upstream();
    let adapter = WireguardAdapter::default();
    let catalog = ComponentCatalog::with_factories(crate::register::component_registry());
    let config = Arc::new(proxy_config());
    let first = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    let pending = first.pending();
    let mut guard = install(&adapter, &first);
    let request = demand(&first);
    let mut first_invocation = None;
    let mut id = None;
    let plan = catalog
        .prepare_with_device_resources(config.clone(), |preparation| {
            let (component, invocation) = first.into_component(preparation).unwrap();
            first_invocation = Some(invocation);
            id = Some(component);
            Ok(std::collections::BTreeSet::from([component]))
        })
        .unwrap();
    catalog.start(config.clone(), deadline()).await.unwrap();
    let resolved = first_invocation
        .unwrap()
        .resolve(&upstream, request)
        .await
        .unwrap();
    catalog
        .start_device_components(config.clone(), deadline())
        .await
        .unwrap();
    let link = resolved.install(None).unwrap();
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
    adapter
        .linked_endpoints
        .lock()
        .unwrap()
        .insert("wg-in".into(), link.clone());
    guard.published();
    drop(guard);

    // A discarded transaction only borrowed this already-published graph.
    // Its wrapper can neither revoke the current TCP nor wait for its retirement.
    let abandoned = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    assert!(!Arc::ptr_eq(&pending, &abandoned.pending()));
    assert!(pending.same_resources(&abandoned.pending()));
    let abandoned_guard = install(&adapter, &abandoned);
    assert!(!abandoned.pending().needs_cleanup());
    let abandoned_expected = adapter
        .pending_endpoints
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    PendingEndpoints::release_closed(&adapter.pending_endpoints, &abandoned_expected).unwrap();
    drop(abandoned);
    drop(abandoned_guard);
    adapter.close_discarded_devices().await.unwrap();
    assert!(device.is_usable());
    assert!(pending.peer_stack(0).unwrap().is_open());
    assert!(witnesses.iter().all(|witness| !witness.is_joined()));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;

    let next = recipe(&adapter, "127.0.0.1:51821", upstream.egress_generation());
    assert!(!Arc::ptr_eq(&pending, &next.pending()));
    assert!(pending.same_resources(&next.pending()));
    let guard = install(&adapter, &next);
    let request = demand(&next);
    let mut invocation = None;
    let next_plan = catalog
        .prepare_with_device_resources(config.clone(), |preparation| {
            let (next_id, operation) = next.into_component(preparation).unwrap();
            assert_eq!(Some(next_id), id);
            invocation = Some(operation);
            Ok(std::collections::BTreeSet::from([next_id]))
        })
        .unwrap();
    assert!(Arc::ptr_eq(
        &plan.device_component(id.unwrap()).unwrap(),
        &next_plan.device_component(id.unwrap()).unwrap()
    ));
    catalog.start(config.clone(), deadline()).await.unwrap();
    let resolved = invocation
        .unwrap()
        .resolve(&upstream, request)
        .await
        .unwrap();
    catalog
        .start_device_components(config.clone(), deadline())
        .await
        .unwrap();
    let reused = resolved.install(None).unwrap();
    assert!(Arc::ptr_eq(&link, &reused));
    assert!(Arc::ptr_eq(
        &device,
        &reused.state.lock().unwrap().devices[0]
    ));
    let retained = pending.peer_stack(0).unwrap().test_original_witnesses();
    assert_eq!(witnesses.len(), retained.len());
    assert!(witnesses
        .iter()
        .zip(&retained)
        .all(|(left, right)| left.same_registration(right) && !right.is_joined()));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    catalog.commit(&config, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
    assert!(witnesses.iter().all(|witness| witness.is_joined()));
    assert!(connecting.await.is_err());
    drop(guard);
}
