//! The original zero.endpoint.wireguard.v1 shape, without temporary JSON Values.
use super::*;
use crate::adapters::wireguard::{EndpointObservationRows, EndpointPeerRow};
pub(super) struct Details<'a> {
    pub(super) facts: &'a EndpointObservationRows,
    pub(super) config: &'a zero_config::RuntimeConfig,
}
struct Rows<'a>(&'a Details<'a>);
enum Config<'a> {
    Outbound(&'a zero_config::WireguardPeerConfig),
    Inbound(&'a zero_config::WireguardInboundPeerConfig),
}
impl Config<'_> {
    fn key(&self) -> &str {
        match self {
            Self::Outbound(peer) => &peer.public_key,
            Self::Inbound(peer) => &peer.public_key,
        }
    }
    fn allowed(&self) -> &[String] {
        match self {
            Self::Outbound(peer) => &peer.allowed_ips,
            Self::Inbound(peer) => &peer.allowed_ips,
        }
    }
    fn endpoint(&self) -> Option<&str> {
        match self {
            Self::Outbound(peer) => (!peer.endpoint.is_empty()).then_some(peer.endpoint.as_str()),
            Self::Inbound(_) => None,
        }
    }
}
impl Serialize for Details<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_struct("WireguardEndpointDetails", 1)?;
        out.serialize_field("peers", &Rows(self))?;
        out.end()
    }
}
impl Serialize for Rows<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let view = self.0;
        let mut out = s.serialize_seq(Some(view.facts.peers.len()))?;
        for (index, facts) in view.facts.peers.iter().enumerate() {
            let (config, tag) = if let Some(outbound) = view.facts.outbound {
                let outbound = &view.config.outbounds[outbound];
                let zero_config::OutboundProtocolConfig::Wireguard { peers, .. } =
                    &outbound.protocol
                else {
                    unreachable!("same actual WG projection")
                };
                (Config::Outbound(&peers[index]), Some(outbound.tag.as_str()))
            } else {
                let inbound =
                    &view.config.inbounds[view.facts.inbound.expect("same configured peer role")];
                let zero_config::InboundProtocolConfig::Wireguard { peers, .. } = &inbound.protocol
                else {
                    unreachable!("same actual WG projection")
                };
                (Config::Inbound(&peers[index]), None)
            };
            out.serialize_element(&Peer {
                index,
                facts,
                config,
                tag,
            })?;
        }
        out.end()
    }
}
struct Peer<'a> {
    index: usize,
    facts: &'a EndpointPeerRow,
    config: Config<'a>,
    tag: Option<&'a str>,
}
struct PublicId<'a>(&'a [u8; 54]);
impl Serialize for PublicId<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(
            std::str::from_utf8(self.0).expect("fixed canonical peer identity is ASCII"),
        )
    }
}
struct Displayed<'a>(&'a dyn std::fmt::Display);
impl Serialize for Displayed<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self.0)
    }
}
impl Serialize for Peer<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_struct("WireguardEndpointPeer", 9)?;
        out.serialize_field("peer_id", &self.facts.public_id.as_ref().map(PublicId))?;
        out.serialize_field("peer_index", &self.index)?;
        out.serialize_field("public_key", self.config.key())?;
        out.serialize_field("allowed_ips", self.config.allowed())?;
        out.serialize_field("configured_endpoint", &self.config.endpoint())?;
        let endpoint = self
            .facts
            .source
            .and_then(|source| source.authenticated_endpoint);
        out.serialize_field(
            "authenticated_endpoint",
            &endpoint.as_ref().map(|address| Displayed(address)),
        )?;
        out.serialize_field(
            "source_known",
            &self.facts.source.and_then(|source| source.source_known),
        )?;
        out.serialize_field(
            "last_authenticated_packet_age_ms",
            &self
                .facts
                .source
                .and_then(|source| source.last_authenticated_packet_age)
                .map(|age| age.as_millis() as u64),
        )?;
        out.serialize_field(
            "health",
            &self.facts.health.as_ref().map(|facts| Health {
                facts,
                tag: self.tag.expect("same actual outbound peer"),
            }),
        )?;
        out.end()
    }
}
struct Health<'a> {
    facts: &'a zero_api::OutboundDeviceHealthSnapshot,
    tag: &'a str,
}
impl Serialize for Health<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(None)?;
        out.serialize_entry("tag", self.tag)?;
        out.serialize_entry("peer_index", &self.facts.peer_index)?;
        out.serialize_entry("state", &self.facts.state)?;
        if let Some(age) = self.facts.last_handshake_age_ms {
            out.serialize_entry("last_handshake_age_ms", &age)?;
        }
        if let Some(age) = self.facts.last_authenticated_packet_age_ms {
            out.serialize_entry("last_authenticated_packet_age_ms", &age)?;
        }
        out.serialize_entry(
            "endpoint_resolution_failed",
            &self.facts.endpoint_resolution_failed,
        )?;
        out.end()
    }
}
