use super::*;
#[tokio::test]
async fn cold_same_root_peer_pressure_rejects_before_driver_or_native_and_does_not_refund_live_payload(
) {
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(3, 160).unwrap();
    roots.packet.install_transmit(3, 335).unwrap();
    let baseline = roots.packet.driver_metadata().occupied;
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(nz(1)));
    let resources = HostResources::new_hosted(
        instance(723),
        &[declaration()],
        CancellationToken::new(),
        scope.registrar().downgrade(),
        roots.clone(),
    )
    .unwrap();
    let other = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(nz(1)));
    assert!(HostResources::new_hosted(
        instance(724),
        &[declaration()],
        CancellationToken::new(),
        other.registrar().downgrade(),
        roots.clone()
    )
    .is_err());
    assert!(!other.registrar().scope_identity().resource_open());
    assert_eq!(roots.packet.native().occupied.frames, 3);
    assert_eq!(roots.packet.transmit().occupied.frames, 3);
    let access = resources.activate(end()).unwrap();
    scope.enable_registration().unwrap();
    let (stop, driver) = pending(&scope);
    let grants = resources
        .take_logical_packet_workspaces(0, &driver)
        .unwrap();
    let held = grants.reads[0].try_buffer().unwrap();
    drop(grants);
    drop(access);
    finish(&mut scope, stop, &driver).await;
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none() && scope.is_drained());
    assert!(resources.shutdown_io(end()).await.unwrap().is_complete());
    drop(resources);
    assert_eq!(roots.packet.native().occupied.frames, 1);
    assert!(roots.packet.driver_metadata().occupied.bytes() > baseline.bytes());
    // A real buffer still consumes the same Root; dropping the controller is
    // not enough to admit another complete peer set on this finite limit.
    assert!(HostResources::new_hosted(
        instance(724),
        &[declaration()],
        CancellationToken::new(),
        other.registrar().downgrade(),
        roots.clone()
    )
    .is_err());
    drop(held);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(roots.packet.driver_metadata().occupied, baseline);
    let recovered = HostResources::new_hosted(
        instance(724),
        &[declaration()],
        CancellationToken::new(),
        other.registrar().downgrade(),
        roots.clone(),
    )
    .unwrap();
    assert_eq!(roots.packet.native().occupied.frames, 3);
    assert!(recovered.shutdown_io(end()).await.unwrap().is_complete());
    drop(recovered);
    assert_eq!(roots.packet.native().occupied.frames, 0);
}
