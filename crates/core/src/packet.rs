//! Datagram and raw-IP ownership are deliberately separate.
use zero_traits::PacketBuffer;

use crate::{component::*, request::Destination};

/// Raw-IP ingress is an endpoint fact, not a fabricated TCP/UDP business flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PacketFacts {
    pub endpoint: EndpointId,
    pub inbound: RoleInstanceId,
    pub carrier_source: Option<core::net::SocketAddr>,
    pub subject: Option<AuthSubject>,
    pub peer: Option<PeerId>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Datagram {
    /// Destination on send, source on receive.
    pub address: Destination,
    pub association: AssociationId,
    pub payload: PacketBuffer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpFamily {
    Ipv4,
    Ipv6,
}

/// Validated return association, opaque to generic forwarding code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnAssociation {
    pub endpoint: EndpointId,
    pub generation: ResourceGeneration,
    pub peer: Option<PeerId>,
    pub correlation: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct IpPacket {
    payload: PacketBuffer,
    family: IpFamily,
    pub return_association: ReturnAssociation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidIpPacket {
    Header,
    Length,
}

impl IpPacket {
    /// Check the complete IP envelope without parsing transport protocols.
    /// Fragmentation, extension headers, checksums and policy stay with their owners.
    pub fn new(
        payload: PacketBuffer,
        return_association: ReturnAssociation,
    ) -> Result<Self, InvalidIpPacket> {
        let family = match payload.first().map(|byte| byte >> 4) {
            Some(4) if payload.len() >= 20 => {
                let header = usize::from(payload[0] & 15) * 4;
                if header < 20 || header > payload.len() {
                    return Err(InvalidIpPacket::Header);
                }
                let length = usize::from(u16::from_be_bytes([payload[2], payload[3]]));
                if length != payload.len() {
                    return Err(InvalidIpPacket::Length);
                }
                IpFamily::Ipv4
            }
            Some(6) if payload.len() >= 40 => {
                let length = usize::from(u16::from_be_bytes([payload[4], payload[5]]));
                if length + 40 != payload.len() {
                    return Err(InvalidIpPacket::Length);
                }
                IpFamily::Ipv6
            }
            _ => return Err(InvalidIpPacket::Header),
        };
        Ok(Self {
            payload,
            family,
            return_association,
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    pub fn family(&self) -> IpFamily {
        self.family
    }
    pub fn into_payload(self) -> PacketBuffer {
        self.payload
    }
}
