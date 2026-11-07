//! Finite production Proxy views over actual original state, not a query SPI.
use super::model::ProxyHandle;
use serde::{ser::SerializeMap, Serialize, Serializer};
mod health;
mod tun;
pub use health::ProxyHealthQuery;
mod capabilities;
pub use capabilities::ProxyCapabilitiesQuery;
/// Borrow only synchronously while measuring/reserving/encoding. Destroy this
/// and every original guard before any await or response-credit refund.
pub struct ProxyControlQuery<'a> {
    tun: tun::Tun<'a>,
}
impl ProxyHandle {
    pub fn control_proxy_query<'a>(
        &'a self,
        request: &'a zero_api::QueryRequest,
    ) -> Option<ProxyControlQuery<'a>> {
        if !matches!(request, zero_api::QueryRequest::TunStatus(_)) {
            return None;
        }
        Some(ProxyControlQuery {
            tun: tun::Tun::new(self),
        })
    }
}
impl ProxyControlQuery<'_> {
    pub fn body(&self) -> impl Serialize + '_ {
        &self.tun
    }
}
impl Serialize for ProxyControlQuery<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry("tun_status", &self.tun)?;
        out.end()
    }
}
impl std::fmt::Debug for ProxyControlQuery<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyControlQuery").finish_non_exhaustive()
    }
}
#[cfg(test)]
mod tests;
