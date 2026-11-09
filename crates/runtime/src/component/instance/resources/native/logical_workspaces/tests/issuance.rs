use super::*;
#[tokio::test]
async fn complete_registered_peer_set_uses_cold_same_root_memory_and_no_native_provider() {
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(6, 320).unwrap();
    roots.packet.install_transmit(6, 670).unwrap();
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(nz(3)));
    let resources = HostResources::new_hosted(
        instance(721),
        &[declaration(), declaration()],
        CancellationToken::new(),
        scope.registrar().downgrade(),
        roots.clone(),
    )
    .unwrap();
    assert!(!scope.registrar().scope_identity().resource_open());
    assert_eq!(roots.packet.native().occupied.frames, 6);
    assert_eq!(roots.packet.transmit().occupied.bytes, 670);
    assert!(resources.packet_calls.get().is_none());
    let access = resources.activate(end()).unwrap();
    scope.enable_registration().unwrap();
    let (stop_a, a) = pending(&scope);
    let (stop_b, b) = pending(&scope);
    let (stop_other, other) = pending(&scope);
    assert!(resources.take_logical_packet_workspaces(0, &a).is_ok());
    assert!(resources.take_logical_packet_workspaces(0, &a).is_err());
    assert!(resources.take_logical_packet_workspaces(1, &a).is_err());
    let second = resources.take_logical_packet_workspaces(1, &b).unwrap();
    assert_eq!(second.receive.len(), 3);
    assert!(second.reads[0]
        .memory_view()
        .unwrap()
        .same_original(&second.receive[0].memory_view().unwrap()));
    assert!(second
        .control
        .as_ref()
        .unwrap()
        .memory_view()
        .unwrap()
        .same_kernel_root(&second.reads[0].memory_view().unwrap()));
    assert!(resources
        .register_software_packet_source_driver(&a)
        .is_err());
    assert!(resources
        .register_software_packet_source_drivers(&[a.clone(), other.clone()])
        .is_err());
    assert!(resources
        .register_software_packet_source_drivers(&[a.clone(), a.clone()])
        .is_err());
    let logical = resources
        .register_software_packet_source_drivers(&[b.clone(), a.clone()])
        .unwrap();
    let repeat = resources
        .register_software_packet_source_drivers(&[a.clone(), b.clone()])
        .unwrap();
    assert!(logical.same_owner(&repeat));
    assert_eq!(logical.drivers().len(), 2);
    assert!(
        logical.source_admission().is_none(),
        "software memory cannot impersonate native UDP"
    );
    assert!(resources.packet_calls.get().unwrap().matches_original(
        &zero_platform_tokio::controlled_carrier::CarrierOrigin::from_logical_packet(
            logical.clone()
        )
    ));
    assert_eq!(roots.carrier.snapshot().charged, 1);
    assert!(resources.io_evidence().native.is_empty());
    assert!(resources.io_evidence().bounds.is_empty());
    drop(access);
    finish(&mut scope, stop_a, &a).await;
    finish(&mut scope, stop_b, &b).await;
    finish(&mut scope, stop_other, &other).await;
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none() && scope.is_drained());
    assert!(resources.shutdown_io(end()).await.unwrap().is_complete());
    assert_eq!(roots.carrier.snapshot().charged, 0);
    drop(second);
    drop(resources);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(roots.packet.transmit().occupied.frames, 0);
}

#[tokio::test]
async fn foreign_scope_cannot_take_and_held_host_ack_keeps_software_n_independent_of_memory() {
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(3, 160).unwrap();
    roots.packet.install_transmit(3, 335).unwrap();
    roots.carrier.install_after_retirement(nz(1)).unwrap();
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(nz(1)));
    let resources = HostResources::new_hosted(
        instance(722),
        &[declaration()],
        CancellationToken::new(),
        scope.registrar().downgrade(),
        roots.clone(),
    )
    .unwrap();
    let access = resources.activate(end()).unwrap();
    scope.enable_registration().unwrap();
    let mut foreign = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(nz(1)));
    foreign.enable_registration().unwrap();
    let (foreign_stop, foreign_driver) = pending(&foreign);
    assert!(resources
        .take_logical_packet_workspaces(0, &foreign_driver)
        .is_err());
    let (stop, driver) = pending(&scope);
    let views = resources
        .take_logical_packet_workspaces(0, &driver)
        .unwrap();
    let held = views.reads[0].try_buffer().unwrap();
    let logical = resources
        .register_software_packet_source_driver(&driver)
        .unwrap();
    assert_eq!(logical.drivers().len(), 1);
    drop(views);
    drop(access);
    stop.send(()).unwrap();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(driver.matches_joined(receipt.joined()));
    receipt.result().unwrap();
    scope.begin_shutdown();
    assert!(!receipt.joined().registration_released());
    assert_eq!(roots.carrier.snapshot().charged, 1);
    assert!(resources.shutdown_io(end()).await.unwrap().is_complete());
    assert_eq!(
        roots.carrier.snapshot().charged,
        1,
        "memory/native resource report cannot ACK actual driver registration"
    );
    scope.acknowledge(&receipt).unwrap();
    assert!(driver
        .wait_registration_released()
        .await
        .registration_released());
    assert!(scope.next_receipt().await.is_none() && scope.is_drained());
    assert_eq!(roots.carrier.snapshot().charged, 0);
    drop(resources);
    assert_eq!(
        roots.packet.native().occupied.frames,
        1,
        "actual held PacketBuffer retains its own memory credit after Host teardown"
    );
    drop(held);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(roots.packet.transmit().occupied.frames, 0);
    finish(&mut foreign, foreign_stop, &foreign_driver).await;
    foreign.begin_shutdown();
    assert!(foreign.next_receipt().await.is_none() && foreign.is_drained());
}
