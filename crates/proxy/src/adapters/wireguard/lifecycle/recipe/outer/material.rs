//! Actual accepted material stamps and their original formatter family.
use super::*;
#[derive(Clone)]
pub(crate) struct OuterMaterial {
    pub(super) identities: Arc<[Arc<[u8]>]>,
    pub(super) projections: Arc<[Option<crate::protocol_registry::ColdPacketPathProjection>]>,
}
pub(crate) type OuterMaterials = std::collections::HashMap<String, OuterMaterial>;
