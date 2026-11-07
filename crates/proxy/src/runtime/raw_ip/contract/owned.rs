//! Owned protocol operations preserve actual rejected buffers in their typed
//! source. A missing implementation never enters a copying protocol fallback.
use super::*;
use zero_traits::{PacketBuffer, PacketDeriveError};

pub(crate) struct OwnedWireResult<T> {
    pub result: Result<T, OwnedWireFailure>,
    pub unused_control: Option<PacketBuffer>,
}
pub(crate) struct OwnedWireFailure {
    pub source: EngineError,
    pub storage: Option<PacketDeriveError>,
}
impl<T> OwnedWireResult<T> {
    pub(in crate::runtime) fn unsupported(
        original: Option<PacketBuffer>,
        control: PacketBuffer,
    ) -> Self {
        Self {
            result: Err(OwnedWireFailure {
                source: std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    UnsupportedOwnedPacket { original },
                )
                .into(),
                storage: None,
            }),
            unused_control: Some(control),
        }
    }
}
#[derive(Debug)]
struct UnsupportedOwnedPacket {
    original: Option<PacketBuffer>,
}
impl std::fmt::Display for UnsupportedOwnedPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "protocol has no owned packet operation (original bytes: {})",
            self.original.as_ref().map_or(0, |packet| packet.len())
        )
    }
}
impl std::error::Error for UnsupportedOwnedPacket {}
