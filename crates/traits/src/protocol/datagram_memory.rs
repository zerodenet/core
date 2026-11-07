//! Pure workspace dimensions/results of the existing DatagramCodec SPI.
//! No allocator, native handle, Scope, IO driver or cleanup permission lives here.
use crate::PacketDeriveError;
use core::{fmt, ops::Range};
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatagramCodecOutput {
    pub buffer: usize,
    pub range: Range<usize>,
}
#[derive(Debug)]
pub enum DatagramCodecBufferError<E> {
    Storage(PacketDeriveError),
    Protocol(E),
}
impl<E: fmt::Display> fmt::Display for DatagramCodecBufferError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(source) => source.fmt(f),
            Self::Protocol(source) => source.fmt(f),
        }
    }
}
impl<E: core::error::Error + 'static> core::error::Error for DatagramCodecBufferError<E> {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(match self {
            Self::Storage(source) => source,
            Self::Protocol(source) => source,
        })
    }
}
