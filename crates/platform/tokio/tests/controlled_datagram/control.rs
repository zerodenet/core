use super::*;
use crate::EgressInterfaceControl;
use zero_traits::DialPolicy;

#[tokio::test]
async fn exact_original_control_is_unique_foreign_isolated_and_drop_never_closes() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let selected = EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let mut other = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let socket = owner
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap();
    let foreign = other
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap();
    assert_eq!(socket.reservation(), foreign.reservation());
    assert!(owner.take_control(&foreign).is_none());
    let control = owner.take_control(&socket).unwrap();
    assert!(owner.take_control(&socket).is_none());
    let foreign_control = other.take_control(&foreign).unwrap();
    let identity = control.identity();
    assert!(identity.matches_socket(&socket));
    assert!(identity.matches_control(&control));
    assert!(!identity.matches_socket(&foreign));
    assert!(!identity.matches_control(&foreign_control));
    let local = socket.local_addr().unwrap();
    socket.send_to_addr(b"same", peer).await.unwrap();
    let mut bytes = [0; 4];
    let (size, source) = target.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"same");
    assert_eq!(source.port(), local.port());
    assert!(source.ip().is_loopback());
    control.revoke();
    assert!(socket.send_to_addr(b"revoked", peer).await.is_err());
    assert!(!control.observe().native_closed);
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    let receipt = control.close().unwrap();
    assert!(receipt.matches(&control));
    assert!(!receipt.matches(&foreign_control));
    assert!(control.observe().native_closed);
    assert_eq!(owner.occupied(), 0);
    let _rebound = tokio::net::UdpSocket::bind(local).await.unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    let local = foreign.local_addr().unwrap();
    drop(foreign_control);
    assert_eq!(other.occupied(), 1);
    assert!(foreign.send_to_addr(b"dropped", peer).await.is_err());
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    assert_eq!(other.close_all().confirmed_closed, 1);
    let _rebound_foreign = tokio::net::UdpSocket::bind(local).await.unwrap();
}

#[tokio::test]
async fn original_controls_reuse_one_fd_slot_for_sixty_five_actual_acquisitions() {
    let peer = "127.0.0.1:9".parse().unwrap();
    let selected = EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    for _ in 0..65 {
        let socket = owner
            .acquire(peer, None, &DialPolicy::default(), &selected)
            .unwrap();
        let control = owner.take_control(&socket).unwrap();
        let local = socket.local_addr().unwrap();
        assert!(control.try_close().unwrap().unwrap().matches(&control));
        assert_eq!(owner.occupied(), 0);
        assert!(socket.send_to_addr(b"closed", peer).await.is_err());
        let _replacement = tokio::net::UdpSocket::bind(local).await.unwrap();
    }
    assert_eq!(owner.take_report().confirmed_closed, 65);
}

#[tokio::test]
async fn native_fault_received_after_actual_close_still_requires_root_handoff_and_scope_ack() {
    use super::super::totals::BoundDatagramTotalsBinding;
    use crate::contract::{
        scope::{TaskClass, TaskScope},
        CancellationToken,
    };
    use crate::controlled_carrier::CarrierOriginalTotals;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
        let mut scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        scope.enable_registration().unwrap();
        let stop = CancellationToken::new();
        let actual_stop = stop.clone();
        let (_, caller) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                actual_stop.cancelled().await;
                Ok(())
            })
            .unwrap();
        let instance = zero_core::component::ComponentInstanceId {
            component: zero_core::component::ComponentId(972),
            generation: zero_core::component::ResourceGeneration(4),
        };
        let mut owner = ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
        owner
            .bind_bound_totals(
                BoundDatagramTotalsBinding::prepare(&root, instance, &caller, &stop).unwrap(),
            )
            .unwrap();
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = peer.local_addr().unwrap();
        let policy = DialPolicy::default();
        let selected = EgressInterfaceControl::default()
            .select_for_peer_with_policy(address, &policy)
            .unwrap();
        let io = owner.acquire(address, None, &policy, &selected).unwrap();
        let control = owner.take_control(&io).unwrap();
        io.send_to_addr(b"same original", address).await.unwrap();
        let mut wire = [0; 32];
        let (read, _) = peer.recv_from(&mut wire).await.unwrap();
        assert_eq!(&wire[..read], b"same original");
        let missing = tempfile::tempdir()
            .unwrap()
            .path()
            .join("missing-native-error-fixture");
        let source = Arc::new(std::fs::File::open(missing).unwrap_err());
        // Inject the native error return at its persistent storage boundary.
        // This checks late Root delivery, not Windows closesocket behavior.
        assert!(control
            .entry
            .first_close_failure
            .set(source.clone())
            .is_ok());
        assert!(control.close().unwrap().matches(&control));
        assert!(root.failures().len() == 1);
        drop(io);
        stop.cancel();
        let joined = scope.next_receipt().await.unwrap();
        assert!(caller.matches_joined(joined.joined()));
        joined.result().unwrap();
        assert_eq!(root.snapshot().charged, 1);
        scope.acknowledge(&joined).unwrap();
        assert!(joined.joined().registration_released());
        assert_eq!(
            root.snapshot().charged,
            1,
            "closed native and real Scope ACK do not acknowledge a fault"
        );
        let retained = root.resource_failures();
        assert_eq!(retained.len(), 1);
        let again = root.resource_failures();
        assert!(Arc::ptr_eq(&retained[0], &again[0]));
        let mut found_original = false;
        let mut current: Option<&(dyn std::error::Error + 'static)> =
            Some(retained[0].source.as_ref());
        while let Some(error) = current {
            if let Some(actual) = error.downcast_ref::<std::io::Error>() {
                found_original |= std::ptr::eq(actual, source.as_ref());
            }
            current = error.source();
        }
        assert!(
            found_original,
            "same actual OS error Arc must survive a positive native receipt"
        );
        assert!(retained[0].acknowledge_retained(&retained[0]));
        assert_eq!(root.snapshot().charged, 0);
        scope.begin_shutdown();
        assert!(scope.is_drained());
        assert_eq!(owner.take_report().confirmed_closed, 1);
    })
    .await
    .unwrap();
}
