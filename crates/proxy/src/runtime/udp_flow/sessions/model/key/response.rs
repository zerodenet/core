//! Borrow the original response index key; no per-response address copy.
use super::*;
pub(in crate::runtime::udp_flow::sessions) trait ResponseIdentity {
    fn tag(&self) -> &str;
    fn target(&self) -> &Address;
    fn port(&self) -> u16;
}
pub(in crate::runtime::udp_flow::sessions) struct ResponseLookup<'a> {
    pub tag: &'a str,
    pub target: &'a Address,
    pub port: u16,
}
impl ResponseIdentity for ResponseLookup<'_> {
    fn tag(&self) -> &str {
        self.tag
    }
    fn target(&self) -> &Address {
        self.target
    }
    fn port(&self) -> u16 {
        self.port
    }
}
impl ResponseIdentity for UdpUpstreamResponseKey {
    fn tag(&self) -> &str {
        &self.outbound_tag
    }
    fn target(&self) -> &Address {
        self.target.address()
    }
    fn port(&self) -> u16 {
        self.port
    }
}
impl Hash for dyn ResponseIdentity + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag().hash(state);
        self.target().hash(state);
        self.port().hash(state);
    }
}
impl PartialEq for dyn ResponseIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.tag() == other.tag() && self.target() == other.target() && self.port() == other.port()
    }
}
impl Eq for dyn ResponseIdentity + '_ {}
impl<'a> Borrow<dyn ResponseIdentity + 'a> for UdpUpstreamResponseKey {
    fn borrow(&self) -> &(dyn ResponseIdentity + 'a) {
        self
    }
}
