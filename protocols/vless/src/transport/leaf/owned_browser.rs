//! Same accepted browser recipe, without a second parse, session or pool.
use super::*;
use std::sync::Arc;
pub(crate) struct OwnedBrowserParts {
    pub plan: OwnedVlessOutboundTransportPlan,
    pub provider: Arc<crate::transport::BrowserProviderOptions>,
    pub endpoint: zero_core::request::Destination,
    pub protocol: ([u8; 16], Option<&'static str>, [u32; 4]),
    pub identity: [u8; 32],
}
impl VlessOutboundLeaf {
    pub(crate) fn owned_browser_parts(
        &self,
    ) -> Result<Option<OwnedBrowserParts>, zero_transport::RuntimeError> {
        if self.preconnect.is_some() || self.protocol.has_mux() {
            return Ok(None);
        }
        let Some(provider) = self.transport.owned_browser_provider()? else {
            return Ok(None);
        };
        let address = zero_core::address::parse_ip_literal(&self.server)
            .map(|address| match address {
                std::net::IpAddr::V4(address) => zero_core::Address::Ipv4(address.octets()),
                std::net::IpAddr::V6(address) => zero_core::Address::Ipv6(address.octets()),
            })
            .unwrap_or_else(|| zero_core::Address::Domain(self.server.clone()));
        Ok(Some(OwnedBrowserParts {
            plan: self.transport.clone(),
            provider,
            endpoint: zero_core::request::Destination::new(address, self.port),
            protocol: self.protocol.ordinary_parts(),
            identity: self.owned_identity,
        }))
    }
}
