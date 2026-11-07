//! Immutable cold semantic data on this exact already-validated recipe.
use super::*;
use zero_core::request::Destination;
/// Protocol-owned existing packet-path grammar. This is not a role/provider,
/// Scope/instance, native grant, transfer proof or StopOld/closure receipt.
#[derive(Clone, PartialEq, Eq)]
pub struct PacketPathFacts {
    pub endpoint: Destination,
    pub associated: bool,
    identity: Arc<[u8]>,
}
impl PacketPathFacts {
    pub fn new(endpoint: Destination, associated: bool) -> Self {
        Self {
            endpoint,
            associated,
            identity: Arc::from([]),
        }
    }
    /// Opaque actual recipe equality/material stamp, not permissions. Callers
    /// may hash it into a descriptor key but cannot parse it as a provider.
    pub fn recipe_identity(&self) -> &[u8] {
        &self.identity
    }
}
impl ComponentRecipe {
    pub fn packet_path_facts(&self) -> Option<&PacketPathFacts> {
        self.packet_path.as_ref()
    }
    pub(in crate::component) fn with_packet_path_facts(
        mut self,
        mut facts: PacketPathFacts,
    ) -> Self {
        facts.identity = self.spec.static_identity.clone();
        self.packet_path = Some(facts);
        self
    }
}
