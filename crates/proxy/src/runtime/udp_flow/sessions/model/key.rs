//! Exact key bytes share one actual paid owner; lookups borrow their input.
use super::*;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use zero_engine::EngineError;
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::{PacketMetadataArc, PacketMetadataCredit, PacketMetadataLayout};

#[derive(Debug, Clone)]
pub(super) enum KeyTarget {
    Existing(Address),
    Source(PacketMetadataArc<Target>),
    Session(PacketMetadataArc<super::super::session::SourceSession>),
}
pub(super) struct Target {
    address: Address,
    _memory: PacketMetadataCredit,
}
impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.address.fmt(f)
    }
}
impl KeyTarget {
    fn address(&self) -> &Address {
        match self {
            Self::Existing(value) => value,
            Self::Source(value) => &value.address,
            Self::Session(value) => value.target(),
        }
    }
    pub(super) fn from_session(session: &super::super::UdpFlowSession) -> Self {
        match session {
            super::super::UdpFlowSession::Source(value) => Self::Session(value.clone()),
            super::super::UdpFlowSession::Existing(value) => Self::Existing(value.target.clone()),
        }
    }
}
mod response;
pub(super) use response::{ResponseIdentity, ResponseLookup};
impl PartialEq for KeyTarget {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}
impl Eq for KeyTarget {}
impl Hash for KeyTarget {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.address().hash(state);
    }
}
impl UdpFlowKey {
    pub(crate) fn new_with_metadata(
        target: &Address,
        port: u16,
        client_session_id: Option<u64>,
        metadata: Option<&PacketDriverMetadataView>,
    ) -> Result<Self, EngineError> {
        let key = if let (Some(memory), Address::Domain(name)) = (metadata, target) {
            let bytes = memory
                .reserve(PacketMetadataLayout::array::<u8>(name.len()).map_err(failure)?)
                .map_err(failure)?;
            let header = memory
                .reserve(PacketMetadataArc::<Target>::layout().map_err(failure)?)
                .map_err(failure)?;
            let actual = PacketMetadataArc::from_prepaid(
                Target {
                    address: target.clone(),
                    _memory: bytes,
                },
                header,
            )
            .unwrap_or_else(|_| unreachable!("same paid target owner"));
            Self {
                target: KeyTarget::Source(actual),
                port,
                client_session_id,
                peer_identity: None,
            }
        } else {
            Self::new(target, port, client_session_id)
        };
        Ok(key)
    }
}
pub(in crate::runtime::udp_flow::sessions) trait Identity {
    fn target(&self) -> &Address;
    fn port(&self) -> u16;
    fn client(&self) -> Option<u64>;
    fn peer(&self) -> Option<&str>;
}
pub(in crate::runtime::udp_flow::sessions) struct Lookup<'a> {
    pub target: &'a Address,
    pub port: u16,
    pub client: Option<u64>,
    pub peer: Option<&'a str>,
}
impl Identity for Lookup<'_> {
    fn target(&self) -> &Address {
        self.target
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn client(&self) -> Option<u64> {
        self.client
    }
    fn peer(&self) -> Option<&str> {
        self.peer
    }
}
impl Identity for UdpFlowKey {
    fn target(&self) -> &Address {
        self.target.address()
    }
    fn port(&self) -> u16 {
        self.port
    }
    fn client(&self) -> Option<u64> {
        self.client_session_id
    }
    fn peer(&self) -> Option<&str> {
        self.peer_identity.as_deref()
    }
}
impl Hash for dyn Identity + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.target().hash(state);
        self.port().hash(state);
        self.client().hash(state);
        self.peer().hash(state);
    }
}
impl PartialEq for dyn Identity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.target() == other.target()
            && self.port() == other.port()
            && self.client() == other.client()
            && self.peer() == other.peer()
    }
}
impl Eq for dyn Identity + '_ {}
impl<'a> Borrow<dyn Identity + 'a> for UdpFlowKey {
    fn borrow(&self) -> &(dyn Identity + 'a) {
        self
    }
}
fn failure(source: impl std::error::Error + Send + Sync + 'static) -> EngineError {
    std::io::Error::other(source).into()
}
