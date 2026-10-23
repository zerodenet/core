use super::*;
use std::sync::{mpsc, Barrier};
use zero_core::{request::Destination, Address};

#[tokio::test]
async fn concurrent_revoke_after_original_bind_fences_escaped_views_without_dropping_the_fd() {
    let proxy = crate::runtime::inbound_owners::bind::tests::proxy();
    let identity = proxy.protocols.allocate_resource_identity().unwrap();
    let declaration = ResourceDeclaration::DatagramListener {
        destination: Destination::new(Address::Ipv4([127, 0, 0, 1]), 0),
        max_bind_attempts: NonZeroUsize::new(1).unwrap(),
        max_pending_sends: NonZeroUsize::new(1).unwrap(),
    };
    let cancel = CancellationToken::new();
    let grant = Arc::new(
        NativeDatagramGrant::new(
            identity,
            &declaration,
            "127.0.0.1:0".parse().unwrap(),
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
