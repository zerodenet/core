//! Exact malformed input is separate from unexpected native IO failures.
use super::*;
#[derive(Debug)]
enum InvalidInputFrame {
    Utf8(std::str::Utf8Error),
    TooLarge,
    Capacity(io::Error),
}
impl std::fmt::Display for InvalidInputFrame {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Utf8(_) => "stream did not contain valid UTF-8",
            Self::TooLarge => "IPC frame exceeds configured byte limit",
            Self::Capacity(_) => "control frame capacity exhausted",
        })
    }
}
impl std::error::Error for InvalidInputFrame {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8(source) => Some(source),
            Self::TooLarge => None,
            Self::Capacity(source) => Some(source),
        }
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
    std::str::from_utf8(bytes).map_err(|source| {
        io::Error::new(io::ErrorKind::InvalidData, InvalidInputFrame::Utf8(source))
    })
}

pub(super) fn too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, InvalidInputFrame::TooLarge)
}

/// Only the concrete control book's known capacity refusal is local to this
/// client. Unexpected native IO still reaches the retained server fault owner.
pub(super) fn budget(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::WouldBlock {
        io::Error::new(
            io::ErrorKind::InvalidData,
            InvalidInputFrame::Capacity(error),
        )
    } else {
        error
    }
}
