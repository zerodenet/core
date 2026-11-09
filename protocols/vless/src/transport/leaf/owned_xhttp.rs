//! One accepted typed recipe supplies both existing material/pool families.
use super::*;
pub(crate) struct OwnedXhttpRecipe {
    pub halves: crate::transport::OwnedXhttpParts,
    pub plan: OwnedVlessOutboundTransportPlan,
    pub protocol: ([u8; 16], Option<&'static str>, [u32; 4]),
    pub identity: [u8; 32],
}
impl VlessOutboundLeaf {
    pub(crate) fn owned_xhttp_recipe(&self) -> Option<OwnedXhttpRecipe> {
        if self.preconnect.is_some() || self.protocol.has_mux() {
            return None;
        }
        Some(OwnedXhttpRecipe {
            halves: self.transport.owned_xhttp_parts()?,
            plan: self.transport.clone(),
            protocol: self.protocol.ordinary_parts(),
            identity: self.owned_identity,
        })
    }
}
