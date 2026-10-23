use super::*;

#[test]
fn ambiguous_native_policy_preserves_os_source_and_never_retries_a_possible_reused_fd() {
    // Policy input only, not a claim of inducing a real close(EINTR). Closing an
    // already reused descriptor to force failure would make the test unsafe.
    let source = io::Error::from_raw_os_error(libc::EINTR);
    let error = io::Error::new(
        source.kind(),
        CloseFailure {
            source,
            retained: Mutex::new(None),
        },
    );
    assert!(!retryable(&error));
    for _ in 0..2 {
        assert!(!retry(&error).unwrap());
    }
    let original = std::error::Error::source(error.get_ref().unwrap())
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(original.raw_os_error(), Some(libc::EINTR));
}
