use super::*;
use zero_platform_tokio::contract::{lifecycle::ResourceLifecycle, CloseReason};
use std::sync::{mpsc, Barrier};
use zero_core::{request::Destination, Address};

#[tokio::test]
async fn concurrent_revoke_after_original_bind_fences_escaped_views_without_dropping_the_fd() {
    let proxy = crate::runtime::inbound_owners::bind::tests::proxy();
    let identity = proxy.protocols.allocate_resource_identity().unwrap();
    let declaration = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    let cancel = CancellationToken::new();
    let grant = Arc::new(
        NativeDatagramGrant::new(
            identity,
            &declaration,
            Some("127.0.0.1:0".parse().unwrap()),
            NonZeroUsize::new(1).unwrap(),
            cancel.clone(),
        )
        .unwrap(),
    );
    let release = Arc::new(Barrier::new(2));
    let (bound, allocated) = mpsc::channel();
    let binding = grant.clone();
    let barrier = release.clone();
    let runtime = tokio::runtime::Handle::current();
    let bind = std::thread::spawn(move || {
        let _entered = runtime.enter();
        binding
            .bind_after(|| {
                bound.send(()).unwrap(); // Real native bind is complete under the same lock.
                barrier.wait();
            })
            .unwrap()
    });
    allocated.recv().unwrap();
    let revoking = grant.clone();
    let revoke = std::thread::spawn(move || {
        DatagramResource(revoking).revoke(CloseReason::Shutdown);
    });
    cancel.cancelled().await; // The actual revoker has reached the locked native owner.
    release.wait();
    let mut view = bind.join().unwrap();
    revoke.join().unwrap();
    let address = view.rx.local_addr().unwrap();
    let mut bytes = [0; 8];
    assert!(view.rx.recv_from_addr(&mut bytes).await.is_err());
    assert!(grant.bind().is_err());
    assert!(tokio::net::UdpSocket::bind(address).await.is_err());
    let mut resource = DatagramResource(grant);
    let report = resource
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.closed_io.len(), 1);
    let rebound = tokio::net::UdpSocket::bind(address).await.unwrap();
    assert_eq!(rebound.local_addr().unwrap(), address);
}

#[tokio::test]
async fn resolving_udp_grants_bounded_address_candidates_without_broadening_literal_startup() {
    let proxy = crate::runtime::inbound_owners::bind::tests::proxy();
    let identity = proxy.protocols.allocate_resource_identity().unwrap();
    let literal = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(50).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    let single = NonZeroUsize::new(1).unwrap();
    let native = NativeDatagramGrant::new(
        identity,
        &literal,
        Some("127.0.0.1:0".parse().unwrap()),
        single,
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(native.attempts, single);
    let mut domain = literal;
    if let ResourceDeclaration::DatagramListener { destination, .. } = &mut domain {
        destination.address = Address::Domain("localhost".into());
    }
    let resolving =
        NativeDatagramGrant::new(identity, &domain, None, single, CancellationToken::new())
            .unwrap();
    assert_eq!(
        resolving.attempts.get(),
        50,
        "same bounded host DNS-candidate grant as TCP"
    );
    assert!(NativeDatagramGrant::new(
        identity,
        &domain,
        None,
        NonZeroUsize::new(51).unwrap(),
        CancellationToken::new()
    )
    .is_err());
}

#[tokio::test]
async fn resolved_udp_candidates_retry_only_after_original_failed_bind_is_closed() {
    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }
    fn original_io<'a>(mut error: &'a (dyn std::error::Error + 'static)) -> &'a std::io::Error {
        let mut original = None;
        loop {
            if let Some(io) = error.downcast_ref::<std::io::Error>() {
                original = Some(io);
            }
            match error.source() {
                Some(source) => error = source,
                None => return original.expect("retained actual native source"),
            }
        }
    }
    let component = zero_core::component::ComponentId(534);
    let declaration = ResourceDeclaration::DatagramListener {
        accepted: None,
        destination: Destination::new(Address::Domain("listener.example".into()), 0),
        max_bind_attempts: nz(2),
        max_pending_sends: nz(1),
    };
    // This tests consumption of the host's resolved plan, not a fabricated DNS
    // query. The Proxy hostname regression exercises the real supervised lookup.
    let unavailable: std::net::SocketAddr = "192.0.2.1:0".parse().unwrap();
    let local: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut owner = HostDatagramListenerOwner::new_resolved(
        component,
        &declaration,
        nz(2),
        Arc::from([unavailable, local]),
    )
    .unwrap();
    let failure = owner.bind_listener(unavailable).err().unwrap();
    assert_eq!(
        original_io(&failure).kind(),
        std::io::ErrorKind::AddrNotAvailable
    );
    assert!(
        failure.retry_permitted(),
        "real original failure permits retry only after confirmed close"
    );
    let view = owner.bind_listener(local).unwrap();
    let endpoint = view.rx.local_addr().unwrap();
    assert!(endpoint.ip().is_loopback());
    assert_eq!(
        owner.bind_listener(local).err().unwrap().reason,
        Reason::Capacity
    );
    drop(view);
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .unwrap();
    assert!(report.is_complete());
    assert!(report
        .closed_io
        .iter()
        .any(|io| io.contains("2 originals explicitly closed")));
}

#[cfg(feature = "raw-ip-runtime")]
#[path = "tests/read_budget.rs"]
mod read_budget;
