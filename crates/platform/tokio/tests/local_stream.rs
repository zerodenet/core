use super::*;
#[test]
fn same_original_local_stream_closes_explicitly_after_real_request_and_response() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hook.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let mut original = OriginalLocalStream::new(&path).unwrap();
    original.connect().unwrap();
    original.set_timeout(Some(Duration::from_secs(1))).unwrap();
    let (mut peer, _) = listener.accept().unwrap();
    original.write_all(b"request").unwrap();
    let mut bytes = [0; 7];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"request");
    peer.write_all(b"response").unwrap();
    let mut bytes = [0; 8];
    original.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"response");
    let receipt = original.close().unwrap();
    assert!(receipt.has_native_original());
    assert!(original.matches_closed(&receipt));
    assert!(original.read(&mut [0]).is_err());
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
}
#[test]
fn failed_local_connect_retains_native_until_same_original_close_and_cold_is_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let mut failed = OriginalLocalStream::new(directory.path().join("missing.sock")).unwrap();
    assert!(failed.connect().is_err());
    assert!(
        failed.native.is_some(),
        "failed connect retains its actual created socket"
    );
    assert!(failed.receipt.is_none());
    let receipt = failed.close().unwrap();
    assert!(receipt.has_native_original());
    let mut cold = OriginalLocalStream::new(directory.path().join("unused.sock")).unwrap();
    let unused = cold.close().unwrap();
    assert!(!unused.has_native_original());
    assert!(!cold.matches_closed(&receipt));
    assert!(!failed.matches_closed(&unused));
    assert!(failed.connect().is_err());
}
