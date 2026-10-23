use super::*;
use crate::EgressInterfaceControl;
use std::{net::SocketAddr, time::Duration};
use zero_traits::DialPolicy;
fn one() -> ControlledDatagramOwner {
    ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap())
}
fn selection(peer: SocketAddr) -> crate::EgressSelection {
    EgressInterfaceControl::default()
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap()
}
#[tokio::test]
async fn actual_original_stays_owned_after_handle_drop_and_recycles_only_after_explicit_close() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let peer = target.local_addr().unwrap();
    let selected = selection(peer);
    let mut owner = one();
    let socket = owner
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap();
    let local = socket.local_addr().unwrap();
    socket.send_to_addr(b"original", peer).await.unwrap();
    let mut bytes = [0; 32];
    let (size, source) = target.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"original");
    target.send_to(b"", source).await.unwrap();
    assert_eq!(socket.recv_from_addr(&mut bytes).await.unwrap().0, 0);
    drop(socket);
    assert_eq!(owner.occupied(), 1);
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    assert!(owner
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .is_err());
    let report = owner.close_all();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
    tokio::net::UdpSocket::bind(local).await.unwrap();
    // A bound of one is reusable capacity, not one lifetime attempt.
    for _ in 0..128 {
        let socket = owner
            .acquire(peer, None, &DialPolicy::default(), &selected)
            .unwrap();
        let local = socket.local_addr().unwrap();
        socket.close().unwrap();
        assert_eq!(owner.occupied(), 0);
        tokio::net::UdpSocket::bind(local).await.unwrap();
    }
    assert_eq!(owner.take_report().confirmed_closed, 128);
    assert_eq!(owner.take_report().confirmed_closed, 0);
}
#[tokio::test]
async fn prepare_failure_has_known_no_native_proof_and_releases_its_reservation() {
    let peer = "127.0.0.1:9".parse().unwrap();
    let mut owner = one();
    let invalid = DialPolicy {
        source_ip: Some("192.0.2.197".parse().unwrap()),
        ..Default::default()
    };
    let error = owner
        .acquire(peer, None, &invalid, &selection(peer))
        .unwrap_err();
    assert_eq!(error.outcome, AcquisitionOutcome::KnownNoNative);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(owner.occupied(), 0);
    let report = owner.take_report();
    assert_eq!(report.known_no_native, 1);
    assert_eq!(report.confirmed_closed, 0);
    let socket = owner
        .acquire(peer, None, &DialPolicy::default(), &selection(peer))
        .unwrap();
    socket.close().unwrap();
    assert!(owner.close_all().is_complete());
}
#[cfg(target_os = "linux")]
#[tokio::test]
async fn actual_interface_bind_failure_explicitly_closes_original_before_reusing_capacity() {
    let peer = "192.0.2.197:9".parse().unwrap();
    let egress = EgressInterfaceControl::default();
    egress.replace(Some(
        crate::EgressInterface::new(
            "controlled-datagram-interface-longer-than-ifnamsiz",
            u32::MAX,
        )
        .unwrap(),
    ));
    let selected = egress
        .select_for_peer_with_policy(peer, &DialPolicy::default())
        .unwrap();
    let mut owner = one();
    for _ in 0..16 {
        let failure = owner
            .acquire(peer, None, &DialPolicy::default(), &selected)
            .unwrap_err();
        assert!(matches!(failure.outcome, AcquisitionOutcome::Closed(_)));
        assert!(failure.to_string().contains("bind_interface"));
        assert_eq!(owner.occupied(), 0);
    }
    let report = owner.take_report();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 16);
}
#[test]
fn consuming_registration_panic_keeps_unknown_original_and_original_reservation() {
    let peer = "127.0.0.1:9".parse().unwrap();
    let selected = selection(peer);
    let mut owner = one();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.acquire(peer, None, &DialPolicy::default(), &selected);
    }));
    assert!(
        panic.is_err(),
        "actual Tokio registration without reactor must panic"
    );
    assert_eq!(owner.occupied(), 1);
    let first = owner.close_all();
    assert!(!first.is_complete());
    assert_eq!(first.confirmed_closed, 0);
    assert_eq!(first.unconfirmed.len(), 1);
    let again = owner.close_all();
    assert!(Arc::ptr_eq(
        &first.unconfirmed[0].source,
        &again.unconfirmed[0].source
    ));
    let denied = owner
        .acquire(peer, None, &DialPolicy::default(), &selected)
        .unwrap_err();
    assert!(matches!(denied.outcome, AcquisitionOutcome::Unknown(_)));
}
#[tokio::test]
async fn cancelled_actual_receive_retains_same_owner_and_native_close_is_not_a_task_join() {
    let peer = "127.0.0.1:9".parse().unwrap();
    let mut owner = one();
    let socket = owner
        .acquire(peer, None, &DialPolicy::default(), &selection(peer))
        .unwrap();
    let local = socket.local_addr().unwrap();
    let mut bytes = [0; 1];
    let read = socket.recv_from_addr(&mut bytes);
    tokio::pin!(read);
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(read.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut read)
        .await
        .is_err());
    let report = owner.close_all();
    assert_eq!(report.confirmed_closed, 1);
    assert!(report.is_complete());
    tokio::net::UdpSocket::bind(local).await.unwrap();
}
