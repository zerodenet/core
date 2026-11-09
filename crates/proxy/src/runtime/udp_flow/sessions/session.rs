//! A Source session is one actual immutable value shared by its real snapshots.
use crate::runtime::pipe::UdpDomainMemory;
use std::ops::{Deref, DerefMut};
use zero_core::{Address, Session};
use zero_engine::EngineError;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit};

#[derive(Clone)]
// Preserve the existing unannotated Session's inline storage; the Source
// variant has a prepaid actual Arc and needs no additional wrapper Box.
#[allow(clippy::large_enum_variant)]
pub(crate) enum UdpFlowSession {
    Existing(Session),
    Source(PacketMetadataArc<SourceSession>),
}
impl std::fmt::Debug for UdpFlowSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}
#[derive(Debug)]
pub(crate) struct SourceSession {
    value: Session,
    // The actual Session fields are freed before their address credits.
    _memory: UdpDomainMemory,
}
impl SourceSession {
    pub(super) fn target(&self) -> &Address {
        &self.value.target
    }
}
impl From<Session> for UdpFlowSession {
    fn from(value: Session) -> Self {
        Self::Existing(value)
    }
}
impl Deref for UdpFlowSession {
    type Target = Session;
    fn deref(&self) -> &Session {
        match self {
            Self::Existing(value) => value,
            Self::Source(value) => &value.value,
        }
    }
}
pub(crate) struct PreparedUdpSession {
    value: Session,
    memory: Option<UdpDomainMemory>,
    header: Option<PacketMetadataCredit>,
}
impl PreparedUdpSession {
    pub(crate) fn new(
        value: Session,
        memory: Option<UdpDomainMemory>,
    ) -> Result<Self, EngineError> {
        let header = memory
            .as_ref()
            .map(|memory| {
                memory
                    .source
                    .reserve(PacketMetadataArc::<SourceSession>::layout().map_err(failure)?)
                    .map_err(failure)
            })
            .transpose()?;
        Ok(Self {
            value,
            memory,
            header,
        })
    }
    pub(crate) fn sniffed_sni(&mut self, use_route: bool) -> Result<(), EngineError> {
        let target = if use_route {
            self.value
                .route_target
                .as_ref()
                .unwrap_or(&self.value.target)
        } else {
            &self.value.target
        };
        if let Address::Domain(name) = target {
            if let Some(memory) = &mut self.memory {
                memory.pay_string(name.len())?;
            }
            self.value.sni = Some(name.clone());
        }
        Ok(())
    }
    pub(crate) fn transparent_original(&mut self, original: Address) -> Result<(), EngineError> {
        if let Some(memory) = &mut self.memory {
            memory.pay_clone(&original)?;
        }
        self.value.original_target = Some(original.clone());
        self.value.direct_target = Some(original);
        Ok(())
    }
    pub(crate) fn into_stored(self) -> UdpFlowSession {
        match (self.memory, self.header) {
            (Some(memory), Some(header)) => UdpFlowSession::Source(
                PacketMetadataArc::from_prepaid(
                    SourceSession {
                        value: self.value,
                        _memory: memory,
                    },
                    header,
                )
                .unwrap_or_else(|_| unreachable!("same prepaid actual Source session")),
            ),
            (None, None) => UdpFlowSession::Existing(self.value),
            _ => unreachable!("same prepared Source metadata state"),
        }
    }
}
impl Deref for PreparedUdpSession {
    type Target = Session;
    fn deref(&self) -> &Session {
        &self.value
    }
}
impl DerefMut for PreparedUdpSession {
    fn deref_mut(&mut self) -> &mut Session {
        &mut self.value
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> EngineError {
    std::io::Error::other(source).into()
}
#[cfg(test)]
#[path = "session/tests.rs"]
mod tests;
