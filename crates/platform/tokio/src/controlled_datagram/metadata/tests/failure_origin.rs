use super::*;

#[tokio::test]
async fn actual_native_failure_keeps_os_source_and_local_rejections_do_not_gain_native_origin() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        rx: _rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    let local = tx.local_addr().unwrap();
    let mut invalid = transmit(local, b"invalid segment");
    invalid.segment_size = Some(0);
    let failure = tx.try_send_detailed(&invalid).unwrap_err();
    assert_eq!(failure.origin, MetadataSendErrorOrigin::Local);
    assert_eq!(failure.error.kind(), std_io::ErrorKind::InvalidInput);
    let permit = tx.shared.sends.try_acquire().unwrap();
    let failure = tx
        .try_send_detailed(&transmit(local, b"capacity"))
        .unwrap_err();
    assert_eq!(failure.origin, MetadataSendErrorOrigin::Local);
    assert_eq!(failure.error.kind(), std_io::ErrorKind::WouldBlock);
    drop(permit);
    let mut poller = tx.create_write_poller().unwrap();
    poll_fn(|cx| poller.poll_writable(cx)).await.unwrap();
    let oversized = vec![0; 65536];
    let failure = tx
        .try_send_detailed(&transmit(local, &oversized))
        .unwrap_err();
    assert_eq!(failure.origin, MetadataSendErrorOrigin::Native);
    assert!(failure.error.raw_os_error().is_some());
    assert_eq!(owner.take_report().main.confirmed_closed, 0);
    control.close().unwrap();
    let failure = tx
        .try_send_detailed(&transmit(local, b"retired"))
        .unwrap_err();
    assert_eq!(failure.origin, MetadataSendErrorOrigin::Local);
    assert_eq!(failure.error.kind(), std_io::ErrorKind::NotConnected);
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
