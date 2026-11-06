//! Only this actual pre-send registration path can attest NotSubmitted.
use std::{error::Error, fmt, io};
#[derive(Debug)]
struct Rejected(io::Error);
impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}
impl Error for Rejected {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
pub(in crate::transport::udp_socket) fn tag(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), Rejected(error))
}
pub(crate) fn rejected(error: &zero_transport::RuntimeError) -> Option<io::ErrorKind> {
    let zero_transport::RuntimeError::Io(error) = error else {
        return None;
    };
    error
        .get_ref()?
        .downcast_ref::<Rejected>()
        .map(|source| source.0.kind())
}
