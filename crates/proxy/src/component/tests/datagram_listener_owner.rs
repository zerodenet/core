use super::*;
use std::{error::Error, time::Duration};
use zero_core::Address;
fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn declaration(address: SocketAddr, attempts: usize) -> ResourceDeclaration {
    ResourceDeclaration::DatagramListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), address.port()),
        max_bind_attempts: nz(attempts),
        max_pending_sends: nz(1),
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn original_io<'a>(mut error: &'a (dyn Error + 'static)) -> &'a std::io::Error {
    loop {
        if let Some(source) = error.downcast_ref::<std::io::Error>() {
            return source;
        }
        error = error.source().expect("actual original IO source");
    }
}

#[tokio::test]
async fn shared_owner_validates_canonical_budget_and_retries_only_actual_closed_failures() {
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let declared = declaration(address, 3);
    assert!(HostDatagramListenerOwner::new(ComponentId(531), &declared, nz(4)).is_err());
    let mut owner = HostDatagramListenerOwner::new(ComponentId(531), &declared, nz(3)).unwrap();
    assert_eq!(
        owner
            .bind_listener("127.0.0.1:0".parse().unwrap())
            .err()
            .unwrap()
            .reason,
        Reason::InvalidConfig
    );
    for _ in 0..2 {
        let failure = owner.bind_listener(address).err().unwrap();
        let source = original_io(&failure);
        assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse);
        assert!(source.raw_os_error().is_some());
    }
    let first = owner.fault().expect("first actual bind error retained");
    drop(occupied);
    let view = owner.bind_listener(address).unwrap();
    let mut rx = view.rx;
    let tx = view.tx;
    let clone = tx.clone();
    let sender = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender.send_to(b"same", address).await.unwrap();
    let mut bytes = [0; 4];
    assert_eq!(rx.recv_from_addr(&mut bytes).await.unwrap().0, 4);
    assert_eq!(&bytes, b"same");
    let mut pending = Box::pin(rx.recv_from_addr(&mut bytes));
    assert!(std::future::Future::poll(
        pending.as_mut(),
        &mut std::task::Context::from_waker(std::task::Waker::noop())
    )
    .is_pending());
    owner.revoke();
    assert!(
        pending.await.is_err(),
        "same original pending receive is cancelled"
    );
    assert!(
        std::net::UdpSocket::bind(address).is_err(),
        "revoke is not a native close ACK"
    );
    let report = owner.shutdown(deadline()).unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(report.closed_io[0].contains("3 originals explicitly closed"));
    assert!(Arc::ptr_eq(&first, owner.fault().as_ref().unwrap()));
    assert_eq!(
        original_io(first.as_ref()).kind(),
        std::io::ErrorKind::AddrInUse
    );
    assert!(rx.recv_from_addr(&mut bytes).await.is_err());
    assert!(clone
        .send_to_addr(b"closed", sender.local_addr().unwrap())
        .await
        .is_err());
    std::net::UdpSocket::bind(address).unwrap();
    assert!(owner.shutdown(deadline()).unwrap().closed_io.is_empty());
    assert_eq!(
        owner.bind_listener(address).err().unwrap().reason,
        Reason::Revoked
    );
}

#[tokio::test]
async fn shared_owner_actual_startup_budget_one_is_not_the_declaration_ceiling() {
    let occupied = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let mut owner =
        HostDatagramListenerOwner::new(ComponentId(532), &declaration(address, 50), nz(1)).unwrap();
    assert_eq!(
        original_io(&owner.bind_listener(address).err().unwrap()).kind(),
        std::io::ErrorKind::AddrInUse
    );
    drop(occupied);
    assert_eq!(
        owner.bind_listener(address).err().unwrap().reason,
        Reason::Capacity
    );
    let report = owner.shutdown(deadline()).unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    assert!(report.closed_io[0].contains("1 originals explicitly closed"));
}

#[test]
fn shared_owner_rejects_domain_and_non_listener_declarations_without_native_creation() {
    let domain = ResourceDeclaration::DatagramListener {
        destination: Destination::new(Address::Domain("localhost".into()), 9),
        max_bind_attempts: nz(1),
        max_pending_sends: nz(1),
    };
    assert!(
        matches!(HostDatagramListenerOwner::new(ComponentId(533), &domain, nz(1)), Err(error) if error.reason == Reason::Unsupported)
    );
    assert!(HostDatagramListenerOwner::new(
        ComponentId(533),
        &ResourceDeclaration::BackgroundCarrier,
        nz(1)
    )
    .is_err());
}

#[test]
fn consumed_registration_unknown_retains_the_same_original_source_and_owner() {
    let address = "127.0.0.1:0".parse().unwrap();
    let mut owner =
        HostDatagramListenerOwner::new(ComponentId(534), &declaration(address, 2), nz(2)).unwrap();
    let dead = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = dead.handle().clone();
    drop(dead);
    {
        let _entered = handle.enter();
        let failure = owner.bind_listener(address).err().unwrap();
        assert_eq!(
            failure.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
    }
    let first = owner.fault().unwrap();
    for _ in 0..2 {
        let failure = owner.shutdown(deadline()).unwrap_err();
        assert_eq!(
            failure.delivery,
            zero_platform_tokio::contract::DeliveryState::Unknown
        );
        assert!(std::ptr::eq(
            original_io(first.as_ref()),
            original_io(&failure)
        ));
        assert!(Arc::ptr_eq(&first, owner.fault().as_ref().unwrap()));
        assert_eq!(
            owner.bind_listener(address).err().unwrap().reason,
            Reason::Revoked
        );
    }
}
