//! Exact malformed input is separate from unexpected native IO failures.
use super::*;
#[derive(Debug)]
struct InvalidInputFrame(std::str::Utf8Error);
impl std::fmt::Display for InvalidInputFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("stream did not contain valid UTF-8")
    }
}
impl std::error::Error for InvalidInputFrame {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
/// A malformed frame terminates only its original client, as before. This
/// exact decoder error is not permission to hide arbitrary InvalidData IO.
pub(crate) fn is_invalid_input_frame(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.downcast_ref::<InvalidInputFrame>().is_some())
}
pub(super) fn text(bytes: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(bytes)
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, InvalidInputFrame(source)))
}
