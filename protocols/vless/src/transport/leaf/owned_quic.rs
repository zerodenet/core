//! Data from the same accepted recipe; no role/IO/caller privilege is issued.
use super::*;
pub(crate) struct OwnedQuicParts {
    pub plan: OwnedVlessOutboundTransportPlan,
    pub profile: VlessQuicClientProfile,
    pub endpoint: zero_core::request::Destination,
    pub protocol: ([u8; 16], Option<&'static str>, [u32; 4]),
    pub identity: [u8; 32],
    pub hysteria: Option<(
        zero_transport::hysteria::Profile,
        zero_transport::hysteria::Pool,
    )>,
    pub h3: Option<(
        zero_transport::split_http::XhttpClientPool,
        zero_transport::profile::OwnedSplitHttpProfile,
    )>,
}
impl VlessOutboundLeaf {
    pub fn native_hysteria_eligible(&self) -> bool {
        self.transport
            .plain_owned_hysteria_parts()
            .is_none_or(|(_, pool, _)| pool.native_eligible())
    }

    pub(crate) fn plain_owned_quic_parts(&self) -> Option<OwnedQuicParts> {
        // Preserve the exact old carrier selection order. A QUIC option next
        // to an earlier real browser/H3/HY/ mKCP selection is not a QUIC call.
        let hysteria = self.transport.plain_owned_hysteria_parts();
        let (profile, h3) = if let Some((_, _, profile)) = &hysteria {
            (profile.clone(), None)
        } else {
            match self.transport.plain_owned_quic_profile() {
                Some(profile) => (profile, None),
                None => {
                    let (profile, pool, http) = self.transport.owned_h3_parts()?;
                    (profile, Some((pool, http)))
                }
            }
        };
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
            h3,
            hysteria: hysteria.map(|(profile, pool, _)| (profile, pool)),
        })
    }
}
