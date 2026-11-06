use super::*;
#[tokio::test]
async fn actual_packet_listener_capacity_is_checked_before_first_native_bind_and_pair_is_unique() {
    use zero_platform_tokio::contract::scope::TaskScope;
    let proxy = crate::runtime::inbound_owners::bind::tests::proxy();
    let identity = proxy.protocols.allocate_resource_identity().unwrap();
    let mut scope = TaskScope::suspended(tokio::runtime::Handle::current(), NonZeroUsize::new(2));
    scope.enable_registration().unwrap();
    let declaration = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    let descriptors = [
        declaration.clone(),
        ResourceDeclaration::PacketDatagramReads {
            datagram: 0,
            workspaces: [NonZeroUsize::new(65_536).unwrap(); 2],
        },
    ];
    let roots = Arc::new(zero_runtime::HostOriginalBudgets::default());
    roots.packet.install_native(1, 131_072).unwrap();
    let grant = Arc::new(
        NativeDatagramGrant::new_scoped(
            identity,
            &declaration,
            Some("127.0.0.1:0".parse().unwrap()),
            NonZeroUsize::new(1).unwrap(),
            CancellationToken::new(),
            scope.registrar(),
        )
        .unwrap()
        .with_original_budgets(roots.clone())
        .with_packet_reads(&descriptors)
        .unwrap(),
    );
    assert!(grant.bind().is_err());
    assert!(
        grant
            .native
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .known_no_acquisition()
    );
    assert_eq!(roots.packet.native().occupied.frames, 0);
    roots.packet.install_native(2, 131_072).unwrap();
    let view = grant.bind().unwrap();
    assert!(
        !grant
            .native
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .known_no_acquisition()
    );
    let [scratch, decode] = grant.take_packet_reads(0).unwrap();
    assert!(grant.take_packet_reads(0).is_err());
    let mut actual = scratch.try_buffer().unwrap();
    actual.as_mut_bytes()[..4].copy_from_slice(b"held");
    drop(view);
    let mut resource = DatagramResource(grant);
    let report = resource
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert_eq!(
        roots.packet.native().occupied.frames,
        1,
        "real retained scratch survives matching native close"
    );
    drop(actual);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert!(decode.try_workspace().is_err());
    assert!(
        scope
            .shutdown(Instant::now() + std::time::Duration::from_secs(2))
            .await
            .is_complete()
    );
}
