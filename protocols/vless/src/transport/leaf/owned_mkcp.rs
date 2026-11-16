//! The already-accepted reliable UDP carrier, before any driver or socket.
use super::*;
pub(crate) struct OwnedMkcpParts {
    pub plan: OwnedVlessOutboundTransportPlan,
    pub settings: zero_transport::mkcp::Settings,
    pub endpoint: zero_core::request::Destination,
    pub protocol: ([u8; 16], Option<&'static str>, [u32; 4]),
    pub identity: [u8; 32],
    pub tls: Option<zero_transport::profile::OwnedClientTlsProfile>,
}
impl VlessOutboundLeaf {
    pub(crate) fn plain_owned_mkcp_parts(&self) -> Option<OwnedMkcpParts> {
        if self.preconnect.is_some() || self.protocol.has_mux() {
            return None;
        }
        let (settings, tls) = self.transport.plain_owned_mkcp_parts()?;
        let address = zero_core::address::parse_ip_literal(&self.server)
            .map(|ip| match ip {
                std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
            })
            .unwrap_or_else(|| zero_core::Address::Domain(self.server.clone()));
        Some(OwnedMkcpParts {
            plan: self.transport.clone(),
            settings,
            tls,
            endpoint: zero_core::request::Destination::new(address, self.port),
            protocol: self.protocol.ordinary_parts(),
            identity: self.owned_identity,
        })
    }
}
