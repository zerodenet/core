use super::*;

#[test]
fn failed_open_error_kinds_without_original_close_cannot_authorize_fallback() {
    for kind in [
        io::ErrorKind::ConnectionRefused,
        io::ErrorKind::NotConnected,
        io::ErrorKind::BrokenPipe,
        io::ErrorKind::Interrupted,
        io::ErrorKind::TimedOut,
        io::ErrorKind::Unsupported,
    ] {
        let attempt = Attempt::open_failed(io::Error::new(kind, "unconfirmed original"));
        assert!(!attempt.can_retry());
        let (result, settled) = attempt.into_parts();
        assert!(!settled);
        assert_eq!(result.unwrap_err().kind(), kind);
    }
}
