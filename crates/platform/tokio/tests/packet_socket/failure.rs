use super::*;
use std::error::Error;

#[test]
fn restore_error_keeps_submission_and_both_original_sources_without_wouldblock_retry() {
    // Classification only: these constructed errors are not OS close evidence.
    let restore = io::Error::from(io::ErrorKind::WouldBlock);
    let error = io::Error::other(PacketScopeRestoreFailure::new(Ok(0), restore));
    assert_eq!(error.kind(), io::ErrorKind::Other);
    let failure = error
        .get_ref()
        .unwrap()
        .downcast_ref::<PacketScopeRestoreFailure>()
        .unwrap();
    assert_eq!(failure.submitted_bytes(), Some(0));
    assert!(failure.send_error().is_none());
    assert_eq!(
        failure.restoration_error().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(std::ptr::eq(
        failure
            .source()
            .unwrap()
            .downcast_ref::<io::Error>()
            .unwrap(),
        failure.restoration_error()
    ));

    let failure = PacketScopeRestoreFailure::new(
        Err(io::Error::from(io::ErrorKind::InvalidInput)),
        io::Error::from(io::ErrorKind::PermissionDenied),
    );
    assert_eq!(failure.submitted_bytes(), None);
    assert_eq!(
        failure.send_error().unwrap().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        failure.restoration_error().kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[test]
fn retained_control_error_preserves_typed_submission_and_same_restore_source() {
    let original = std::sync::Arc::new(io::Error::other(PacketScopeRestoreFailure::new(
        Ok(7),
        io::Error::from(io::ErrorKind::PermissionDenied),
    )));
    let first = io::Error::other(original.clone());
    let later = io::Error::other(original.clone());
    let expected = PacketScopeRestoreFailure::from_io(original.as_ref()).unwrap();
    for error in [&first, &later] {
        let failure = PacketScopeRestoreFailure::from_io(error).unwrap();
        assert!(std::ptr::eq(failure, expected));
        assert_eq!(failure.submitted_bytes(), Some(7));
        assert!(std::ptr::eq(
            failure.restoration_error(),
            expected.restoration_error()
        ));
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }
}
