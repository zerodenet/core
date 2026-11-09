use super::*;
use zero_traits::packet_metadata::PacketMetadataLayout;

#[tokio::test]
async fn hosted_source_rows_and_sealed_weak_header_hold_exact_prepaid_root_credit() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let view = root.driver_metadata_view();
    let before = view.snapshot().occupied;
    let mut prepaid = Prepaid::reserve(limits(), Some(&view)).unwrap();
    assert!(view.snapshot().occupied.bytes() > before.bytes());
    let sources = MetadataOriginalSources::new(limits(), &mut prepaid);
    let weak = sources.0.downgrade();
    assert!(weak.upgrade().is_some());
    drop(prepaid);
    drop(sources);
    assert!(weak.upgrade().is_none());
    assert!(
        view.snapshot().occupied.bytes() > before.bytes(),
        "sealed weak still holds the actual header and attachment"
    );
    drop(weak);
    assert_eq!(view.snapshot().occupied, before);
}

#[test]
fn source_layout_refusal_precedes_any_rows_or_hosted_header() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let view = root.driver_metadata_view();
    let before = view.snapshot().occupied;
    let occupied = view.snapshot().occupied;
    // Reserve all available actual units, without creating a substitute Root.
    let limit = view.snapshot().limit;
    let hold = view
        .reserve(PacketMetadataLayout::new(
            limit.allocations() - occupied.allocations(),
            limit.bytes() - occupied.bytes(),
        ))
        .unwrap();
    assert!(Prepaid::reserve(limits(), Some(&view)).is_err());
    drop(hold);
    assert_eq!(view.snapshot().occupied, before);
}

#[tokio::test]
async fn actual_metadata_binding_rejects_a_different_root_with_the_same_numeric_limits() {
    let one = NonZeroUsize::new(1).unwrap();
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(one));
    scope.enable_registration().unwrap();
    let tasks = scope.registrar();
    let host = Arc::new(CancellationToken::new());
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let owner = MetadataPoolOwner::new(instance(), one, limits(), &host).unwrap();
    let declaration = ResourceDeclaration::ControlledMetadataDatagram {
        endpoint: peer.local_addr().unwrap(),
        dial: Default::default(),
        limits: limits(),
        max_origins: one,
    };
    let originals = CarrierOriginalTotals::new(one);
    let first = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let second = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    owner
        .registration()
        .bind_declared_with_accounting(
            &declaration,
            &tasks,
            &host,
            &originals,
            &first.driver_metadata_view(),
        )
        .unwrap();
    assert!(owner
        .registration()
        .bind_declared_with_accounting(
            &declaration,
            &tasks,
            &host,
            &originals,
            &second.driver_metadata_view()
        )
        .is_err());
    assert!(
        owner
            .registration()
            .bind_declared_with_totals(&declaration, &tasks, &host, &originals)
            .is_err(),
        "an explicit library entry cannot downgrade an actual Host binding"
    );
    owner
        .registration()
        .bind_declared_with_accounting(
            &declaration,
            &tasks,
            &host,
            &originals,
            &first.driver_metadata_view(),
        )
        .unwrap();
    assert!(owner.shutdown().unwrap().is_complete());
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}

#[tokio::test]
async fn paid_native_canonical_and_its_sealed_weak_keep_the_actual_header_until_deallocation() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let view = root.driver_metadata_view();
    let before = view.snapshot().occupied;
    let mut prepaid = Prepaid::reserve(limits(), Some(&view)).unwrap();
    let sources = MetadataOriginalSources::new(limits(), &mut prepaid);
    let mut native =
        crate::controlled_datagram::ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let socket = native
        .bind_native_listener("127.0.0.1:0".parse().unwrap())
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let actual = os_source(
        &directory
            .path()
            .join("real OS failure at native source capture"),
    );
    sources.acquisition(
        MetadataOriginalNamespace::Main,
        socket.reservation(),
        &actual,
    );
    sources.mark_failed();
    let canonical = sources.canonical_error().unwrap();
    assert!(canonical.is_prepaid());
    assert!(canonical.ptr_eq(&sources.canonical_error().unwrap()));
    let weak = canonical.downgrade();
    socket.close().unwrap();
    assert_eq!(native.take_report().confirmed_closed, 1);
    drop(native);
    drop(prepaid);
    drop(sources);
    assert!(weak.upgrade().is_some());
    drop(canonical);
    assert!(weak.upgrade().is_none());
    assert!(view.snapshot().occupied.bytes() > before.bytes());
    drop(weak);
    assert_eq!(view.snapshot().occupied, before);
}
