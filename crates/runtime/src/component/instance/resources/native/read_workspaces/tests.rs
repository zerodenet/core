use super::*;
fn declaration(datagram: usize) -> ResourceDeclaration {
    ResourceDeclaration::PacketDatagramReads {
        datagram,
        workspaces: [NonZeroUsize::new(65_536).unwrap(); 2],
        control_workspace: None,
        supplemental: Default::default(),
    }
}
#[test]
fn cold_read_descriptions_reject_wrong_descriptor_and_duplicate_same_original() {
    assert!(packet_datagram_read_spec(&[declaration(1)], 0).is_err());
    let listener = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    assert!(
        packet_datagram_read_spec(&[listener.clone(), declaration(0), declaration(0)], 0).is_err()
    );
    assert!(packet_datagram_read_spec(&[listener.clone(), declaration(1)], 0).is_err());
    assert_eq!(
        packet_datagram_read_spec(&[listener, declaration(0)], 0).unwrap(),
        Some([NonZeroUsize::new(65_536).unwrap(); 2])
    );
}

#[cfg(feature = "raw-ip-runtime")]
#[tokio::test]
async fn revoked_controller_detaches_only_its_memory_handles_and_last_actual_buffer_frees_the_header(
) {
    use zero_core::component::ResourceGeneration;
    use zero_platform_tokio::contract::scope::TaskScope;
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let roots = Arc::new(crate::HostOriginalBudgets::default());
    roots.packet.install_native(2, 128).unwrap();
    let baseline = roots.packet.driver_metadata().occupied;
    let original = HostPacketReadReservation::new(
        ComponentInstanceId {
            component: ComponentId(713),
            generation: ResourceGeneration(1),
        },
        scope.registrar().scope_identity(),
        roots.clone(),
        [NonZeroUsize::new(64).unwrap(); 2],
    )
    .unwrap();
    original
        .prepare_before_listener_bind(&scope.registrar())
        .unwrap();
    let grants = original.take().unwrap();
    let held = grants[0].try_buffer().unwrap();
    drop(grants);
    original.revoke();
    assert!(original.state.lock().unwrap().grants.is_none());
    assert!(original.state.lock().unwrap().supplemental.is_none());
    assert!(
        original.released(),
        "memory admission is sealed, never an IO/Scope receipt"
    );
    assert_eq!(roots.packet.native().occupied.frames, 1);
    assert!(roots.packet.driver_metadata().occupied.bytes() > baseline.bytes());
    assert!(original.take().is_err());
    drop(held);
    assert_eq!(roots.packet.native().occupied.frames, 0);
    assert_eq!(roots.packet.driver_metadata().occupied, baseline,
        "the still-live controller cannot pin removed memory headers after their actual users return");
    assert!(original.state.lock().unwrap().stopped);
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}
