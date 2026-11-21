//! The same candidate map's original identity check, without its Future graph.
use super::*;

pub(super) trait PendingOwnerLookup: Send + Sync {
    fn check_owner(
        &self,
        tag: &str,
        pending: &Arc<PendingLinkedEndpoint>,
    ) -> Result<(), EngineError>;
}

impl PendingOwnerLookup for super::super::super::PendingEndpointMapState {
    fn check_owner(
        &self,
        tag: &str,
        pending: &Arc<PendingLinkedEndpoint>,
    ) -> Result<(), EngineError> {
        LinkedEndpointRecipe::check_owner(self, tag, pending)
    }
}
