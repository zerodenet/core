use super::*;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::component::{ResourceAccess, ResourceDeclaration};

#[tokio::test]
async fn real_native_close_does_not_delete_an_already_published_original_fault() {
    let destination = Destination::new(Address::Ipv4([127, 0, 0, 1]), 0);
    let grants = HostResources::new(
        ComponentId(244),
        &[ResourceDeclaration::TcpListener {
            accepted: None,
            destination: destination.clone(),
            max_bind_attempts: std::num::NonZeroUsize::new(1).unwrap(),
        }],
        CancellationToken::new(),
    )
    .unwrap();
    let deadline = || Instant::now() + std::time::Duration::from_secs(3);
    let _access = grants.activate(deadline()).unwrap();
    let listener = grants.listen_tcp(destination).await.unwrap();
    let address = listener.local_addr().unwrap();
    // Obtain an actual OS source, not a copied errno or matching error string.
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let source = std::net::TcpListener::bind(occupied.local_addr().unwrap()).unwrap_err();
    assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse);
    assert!(source.raw_os_error().is_some());
    let key = (true, "127.0.0.1:0".parse().unwrap());
    grants.record_native(
        key,
        Err(error(ComponentId(244), Reason::Io, "actual native fault").with_source(source)),
    );
    let first = grants.state.lock().unwrap().io.failures[&key].clone();
    // This regression targets aggregate error retention, not a portable claim
    // that a Windows known-open close retry can be reproduced on Unix.
    for _ in 0..2 {
        assert!(grants.shutdown_io(deadline()).await.is_err());
        let state = grants.state.lock().unwrap();
        assert!(state.io.confirmed.contains(&key));
        assert!(Arc::ptr_eq(&first, &state.io.failures[&key]));
        assert_eq!(state.io.receipts.closed_io.len(), 1);
    }
    let evidence = grants.io_evidence();
    assert!(evidence.physical_confirmed());
    assert!(Arc::ptr_eq(
        &first,
        evidence.native[0].first_fault.as_ref().unwrap()
    ));
    std::net::TcpListener::bind(address).unwrap();
}
