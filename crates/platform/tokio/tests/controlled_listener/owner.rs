//! Actual acquisition/registration/port tests, not injected syscall outcomes.
use super::*;
use crate::contract::{component::ResourceDeclaration, DeliveryState, Reason};
use std::{error::Error, num::NonZeroUsize};
use zero_core::{request::Destination, Address};

fn declaration(address: SocketAddr, attempts: usize) -> ResourceDeclaration {
    let ip = match address.ip() {
        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
    };
    ResourceDeclaration::TcpListener {
        accepted: None,
        destination: Destination::new(ip, address.port()),
        max_bind_attempts: std::num::NonZeroUsize::new(attempts).unwrap(),
    }
}
fn owner(address: SocketAddr, attempts: usize) -> ControlledTcpListenerOwner {
    ControlledTcpListenerOwner::new(
        &declaration(address, attempts),
        NonZeroUsize::new(attempts).unwrap(),
        CancellationToken::new(),
    )
    .unwrap()
}
fn io_source<'a>(error: &'a (dyn Error + 'static)) -> &'a io::Error {
    if let Some(source) = error.downcast_ref::<io::Error>() {
        return source;
    }
    io_source(error.source().expect("original IO error in chain"))
}

#[tokio::test]
async fn native_bind_collision_closes_original_before_bounded_retry_and_retains_exact_os_source() {
    timeout(Duration::from_secs(3), async {
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = occupied.local_addr().unwrap();
        // The actual owner exists before either native creation attempt.
        let mut native = owner(address, 2);
        let error = match native.bind_listener(address) {
            Err(error) => error,
            Ok(_) => panic!("collision succeeded"),
        };
        assert_eq!(error.reason, Reason::Io);
        assert_eq!(error.delivery, DeliveryState::NotSubmitted);
        assert!(error.retry_permitted());
        let actual = io_source(&error);
        assert_eq!(actual.kind(), io::ErrorKind::AddrInUse);
        assert!(actual.raw_os_error().is_some());
        let retained = native.acquisition_source().unwrap();
        assert!(std::ptr::eq(actual, retained.as_ref()));
        assert_eq!(native.remaining_bind_attempts(), 1);
        assert_eq!(native.confirmed_close_count(), 1);
        assert!(!native.native_unconfirmed());
        // This is a different, test-owned listener; its Drop is not the failed
        // attempt's receipt. That original was explicitly closed before Err.
        drop(occupied);
        let mut view = native.bind_listener(address).unwrap();
        assert_eq!(view.local_addr().unwrap(), address);
        assert_eq!(native.remaining_bind_attempts(), 0);
        let (peer, accepted) = tokio::join!(TcpStream::connect(address), view.accept());
        let mut peer = peer.unwrap();
        let (mut accepted, _) = accepted.unwrap();
        peer.write_all(b"bound").await.unwrap();
        let mut bytes = [0; 5];
        accepted.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"bound");
        let report = native.wait_closed(deadline()).await.unwrap();
        assert!(report.is_complete());
        assert_eq!(
            report.closed_io.len(),
            1,
            "constant-size aggregate, not unbounded per-attempt history"
        );
        assert_eq!(native.confirmed_close_count(), 2);
        assert!(report.closed_io[0].contains("2 original native closes"));
        let rebound = std::net::TcpListener::bind(address).unwrap();
        assert_eq!(rebound.local_addr().unwrap(), address);
        assert!(native
            .wait_closed(deadline())
            .await
            .unwrap()
            .closed_io
            .is_empty());
        assert!(Arc::ptr_eq(
            &retained,
            &native.acquisition_source().unwrap()
        ));
        drop((peer, accepted)); // These caller-owned children have no claimed receipt.
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn exhausted_creation_budget_cannot_replace_confirmed_failed_attempt() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let mut native = owner(address, 1);
    assert!(native.bind_listener(address).is_err());
    assert_eq!(native.confirmed_close_count(), 1);
    drop(occupied);
    let error = match native.bind_listener(address) {
        Err(error) => error,
        Ok(_) => panic!("budget bypass"),
    };
    assert_eq!(error.reason, Reason::Capacity);
    assert_eq!(native.confirmed_close_count(), 1);
    assert!(native
        .wait_closed(Instant::now())
        .await
        .unwrap()
        .is_complete());
    std::net::TcpListener::bind(address).unwrap();
}

#[tokio::test]
async fn exact_declaration_and_revoke_deny_before_native_attempt_and_empty_owner_has_no_receipt() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut native = owner(address, 1);
    let error = match native.bind_listener("127.0.0.2:0".parse().unwrap()) {
        Err(e) => e,
        Ok(_) => panic!("wrong address"),
    };
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert_eq!(native.remaining_bind_attempts(), 1);
    native.revoke();
    let error = match native.bind_listener(address) {
        Err(e) => e,
        Ok(_) => panic!("revoked"),
    };
    assert_eq!(error.reason, Reason::Revoked);
    assert_eq!(native.remaining_bind_attempts(), 1);
    let report = native.wait_closed(Instant::now()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert_eq!(native.confirmed_close_count(), 0);
}

#[tokio::test]
async fn owner_retains_success_after_view_drop_and_abandoned_cleanup_until_actual_close() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut native = owner(address, 3);
    let view = native.bind_listener(address).unwrap();
    let bound = view.local_addr().unwrap();
    drop(view);
    drop(native.wait_closed(deadline())); // Not polled, no side effect.
    assert_bound(bound);
    let report = native.wait_closed(Instant::now()).await.unwrap();
    assert!(!report.is_complete());
    assert!(report.closed_io.is_empty());
    assert_eq!(native.confirmed_close_count(), 0);
    assert_bound(bound);
    assert!(native.wait_closed(deadline()).await.unwrap().is_complete());
    assert_eq!(native.confirmed_close_count(), 1);
    std::net::TcpListener::bind(bound).unwrap();
    assert!(native.bind_listener(address).is_err());
}

#[test]
fn domain_and_non_tcp_declarations_are_rejected_without_native_work() {
    let parent = CancellationToken::new();
    let cap = NonZeroUsize::new(1).unwrap();
    let domain = ResourceDeclaration::TcpListener {
        accepted: None,
        destination: Destination::new(Address::Domain("localhost".into()), 0),
        max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
    };
    let error = match ControlledTcpListenerOwner::new(&domain, cap, parent.clone()) {
        Err(e) => e,
        Ok(_) => panic!("implicit DNS"),
    };
    assert_eq!(error.reason, Reason::Unsupported);
    let wrong = ResourceDeclaration::BackgroundCarrier;
    assert!(ControlledTcpListenerOwner::new(&wrong, cap, parent).is_err());
}

#[test]
fn actual_attempt_budget_cannot_exceed_the_same_canonical_listener_declaration() {
    let address = "127.0.0.1:0".parse().unwrap();
    let error = match ControlledTcpListenerOwner::new(
        &declaration(address, 1),
        NonZeroUsize::new(2).unwrap(),
        CancellationToken::new(),
    ) {
        Err(error) => error,
        Ok(_) => panic!("actual caller exceeded declared attempt ceiling"),
    };
    assert_eq!(error.reason, Reason::InvalidConfig);
}

#[test]
fn actual_shutdown_reactor_registration_error_stays_unknown_with_original_source_and_no_retry() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    drop(runtime); // Actual Tokio IO registration set has shut down.
    let _entered = handle.enter();
    let address = "127.0.0.1:0".parse().unwrap();
    let mut native = owner(address, 2);
    let error = match native.bind_listener(address) {
        Err(e) => e,
        Ok(_) => panic!("shutdown reactor registered"),
    };
    let retained = native.acquisition_source().unwrap();
    assert!(std::ptr::eq(io_source(&error), retained.as_ref()));
    assert_eq!(error.delivery, DeliveryState::Unknown);
    assert!(!error.retry_permitted());
    assert!(native.native_unconfirmed());
    assert_eq!(native.remaining_bind_attempts(), 1);
    assert_eq!(native.confirmed_close_count(), 0);
    assert!(native.bind_listener(address).is_err());
    assert_eq!(native.remaining_bind_attempts(), 1);
    let healthy = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    healthy.block_on(async {
        for _ in 0..2 {
            let failure = native.wait_closed(deadline()).await.unwrap_err();
            let source = failure
                .source()
                .unwrap()
                .downcast_ref::<Arc<io::Error>>()
                .unwrap();
            assert!(Arc::ptr_eq(source, &retained));
            assert_eq!(native.confirmed_close_count(), 0);
        }
    });
}

#[test]
fn actual_missing_io_driver_panic_keeps_preinstalled_unknown_despite_poison_and_handle_consumption()
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let address = "127.0.0.1:0".parse().unwrap();
    let mut native = owner(address, 2);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        native.bind_listener(address)
    }));
    assert!(
        panic.is_err(),
        "actual Tokio registration without IO driver must panic"
    );
    assert!(native.native_unconfirmed());
    assert_eq!(native.remaining_bind_attempts(), 1);
    assert_eq!(native.confirmed_close_count(), 0);
    assert!(native.bind_listener(address).is_err());
    let healthy = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    healthy.block_on(async {
        let failure = native.wait_closed(deadline()).await.unwrap_err();
        assert_eq!(failure.delivery, DeliveryState::Unknown);
        assert!(native.native_unconfirmed());
    });
}
