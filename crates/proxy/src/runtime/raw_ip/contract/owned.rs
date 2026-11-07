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

/// Actual prepared bytes and an unforgeable-by-equality operation identity.
/// No native/Scope authority is issued by this DTO. The same owning codec must
/// match its pending cell before it can commit that operation.
pub(crate) struct PreparedWireData {
    pub identity: std::sync::Arc<()>,
    pub packet: PacketBuffer,
    pub peer: Option<usize>,
    pub allowed: bool,
}
pub(crate) enum PreparedWireReceive<T> {
    Complete(T),
    Data(PreparedWireData),
}
impl<T> OwnedWireResult<T> {
    pub(crate) fn map<R>(self, map: impl FnOnce(T) -> R) -> OwnedWireResult<R> {
        OwnedWireResult {
            result: self.result.map(map),
            unused_control: self.unused_control,
        }
    }
}

#[derive(Debug)]
struct RejectedPrepared {
    source: PacketDeriveError,
    original: PacketBuffer,
}
impl std::fmt::Display for RejectedPrepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "prepared original Data admission: {} ({} bytes retained)",
            self.source,
            self.original.len()
        )
    }
}
impl std::error::Error for RejectedPrepared {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl PreparedWireData {
    pub(crate) fn admit_forward(&mut self) -> Result<(), PacketDeriveError> {
        self.packet.prepare_transmit(0, 0)
    }
    pub(crate) fn rejected(self, source: PacketDeriveError) -> OwnedWireFailure {
        OwnedWireFailure {
            storage: Some(source),
            source: std::io::Error::other(RejectedPrepared {
                source,
                original: self.packet,
            })
            .into(),
        }
    }
}
