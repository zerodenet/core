//! Immutable request facts, independent of configuration and API projections.
use alloc::string::String;
use core::net::SocketAddr;

use crate::{component::*, Address, ProtocolType, TargetHostSource};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Destination {
    pub address: Address,
    pub port: u16,
}

impl Destination {
    pub fn new(address: Address, port: u16) -> Self {
        Self { address, port }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessFacts {
    pub id: Option<u32>,
    pub name: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestFacts {
    pub session: SessionId,
    pub inbound: RoleId,
    pub original_source: Option<SocketAddr>,
    pub effective_source: Option<SocketAddr>,
    pub local_endpoint: Option<SocketAddr>,
    pub original_target: Option<Destination>,
    /// Absent for target-free associations; never synthesized for control traffic.
    pub target: Option<Destination>,
    pub target_source: Option<TargetHostSource>,
    pub subject: Option<AuthSubject>,
    pub peer: Option<PeerId>,
    pub protocol: ProtocolType,
    pub sni: Option<String>,
    pub process: Option<ProcessFacts>,
}

/// No target is fabricated for an unbound UDP association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatagramTargets {
    Fixed(Destination),
    Multiple,
}
