//! Pure workspace dimensions/results of the existing DatagramCodec SPI.
//! No allocator, native handle, Scope, IO driver or cleanup permission lives here.
use crate::PacketDeriveError;
use core::{fmt, ops::Range};
/// The concrete built-in codec workspace count, without a temporary heap table.
/// These dimensions describe storage only; they grant no allocation or IO.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatagramCodecBufferLayout {
    One([usize; 1]),
    Two([usize; 2]),
}
impl core::ops::Deref for DatagramCodecBufferLayout {
    type Target = [usize];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::One(lengths) => lengths,
            Self::Two(lengths) => lengths,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatagramCodecOutput {
    pub buffer: usize,
    pub range: Range<usize>,
}
/// Existing buffered decode outcome, including malformed-packet rejection.
pub type DatagramCodecDecodeResult<Target, E> =
    Result<Option<(Target, u16, DatagramCodecOutput)>, DatagramCodecBufferError<E>>;
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

impl AsRef<[usize]> for DatagramCodecBufferLayout {
    fn as_ref(&self) -> &[usize] {
        self
    }
}
