//! Data from the same accepted recipe; no role/IO/caller privilege is issued.
use super::*;
pub(crate) struct OwnedQuicParts {
    pub plan: OwnedVlessOutboundTransportPlan,
    pub profile: VlessQuicClientProfile,
    pub endpoint: zero_core::request::Destination,
    pub protocol: ([u8; 16], Option<&'static str>, [u32; 4]),
    pub identity: [u8; 32],
}
impl VlessOutboundLeaf {
    pub(crate) fn plain_owned_quic_parts(&self) -> Option<OwnedQuicParts> {
        // Preserve the exact old carrier selection order. A QUIC option next
        // to an earlier real browser/H3/HY/ mKCP selection is not a QUIC call.
        let profile = self.transport.plain_owned_quic_profile()?;
        if self.preconnect.is_some() || self.protocol.has_mux() {
            return None;
        }
        let address = zero_core::address::parse_ip_literal(&self.server)
            .map(|ip| match ip {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            })
            .unwrap_or_else(|| zero_core::Address::Domain(self.server.clone()));
        Some(OwnedQuicParts {
            plan: self.transport.clone(),
            profile,
            endpoint: zero_core::request::Destination::new(address, self.port),
            protocol: self.protocol.ordinary_parts(),
            identity: self.owned_identity,
        })
    }
}
