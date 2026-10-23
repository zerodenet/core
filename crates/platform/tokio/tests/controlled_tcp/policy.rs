use super::*;

#[test]
fn ambiguous_close_error_policy_retains_source_and_never_retries_a_possible_reused_fd() {
    // A policy input, not evidence that a real OS close returned EINTR.
    // Safely inducing close(EINTR) portably would itself risk closing a reused fd.
    let source = io::Error::from_raw_os_error(libc::EINTR);
    let error = io::Error::new(
        source.kind(),
        NativeCloseFailure {
            source,
            retained: Mutex::new(None),
        },
    );
    assert!(!TokioSocket::close_is_retryable(&error));
    assert!(!TokioSocket::retry_close(&error).unwrap());
    let source = std::error::Error::source(error.get_ref().unwrap())
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(source.raw_os_error(), Some(libc::EINTR));
    assert!(!TokioSocket::retry_close(&error).unwrap());
}
