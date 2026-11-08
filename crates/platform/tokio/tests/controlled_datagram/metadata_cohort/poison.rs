use super::*;

#[tokio::test]
async fn same_original_zero_id_sources_survive_poison_and_real_positive_udp_close() {
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut owner =
        crate::controlled_datagram::ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap());
    let socket = owner
        .bind_native_listener("127.0.0.1:0".parse().unwrap())
        .unwrap();
    let actual = socket.entry.clone();
    assert_eq!(socket.reservation().get(), 0);
    socket
        .send_to_addr(b"before poison", target.local_addr().unwrap())
        .await
        .unwrap();
    let mut bytes = [0; 32];
    let (size, _) = target.recv_from(&mut bytes).await.unwrap();
    assert_eq!(&bytes[..size], b"before poison");
    let directory = tempfile::tempdir().unwrap();
    let source = os_source(
        &directory
            .path()
            .join("actual OS error at original source boundary"),
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut acquisition = actual.acquisition_failure.lock().unwrap();
        *acquisition = Some(source.clone());
        panic!("original acquisition source mutex poisoned");
    }))
    .is_err());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _native = actual.native.lock().unwrap();
        panic!("original native mutex poisoned");
    }))
    .is_err());
    let sources = MetadataOriginalSources::new(limits());
    sources.try_capture(MetadataOriginalNamespace::Main, &owner);
    let found = sources.originals().next().unwrap();
    assert_eq!(found.namespace, MetadataOriginalNamespace::Main);
    assert_eq!(found.reservation, socket.reservation());
    assert!(Arc::ptr_eq(found.acquisition.unwrap(), &source));
    let native = socket.close().unwrap();
    assert_eq!(native.reservation(), socket.reservation());
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert!(Arc::ptr_eq(
        sources.originals().next().unwrap().acquisition.unwrap(),
        &source
    ));
}
