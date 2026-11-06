//! Strong custody of the exact same endpoint, with a sealed type boundary.
//! No resource factory, permission, cleanup executor or replacement endpoint.
use super::ProcessEndpointHandoff;
use std::sync::Arc;

#[derive(Clone)]
pub(in crate::owned_process) struct RetainedEndpoint(Arc<dyn OriginalEndpoint>);
trait OriginalEndpoint: Send + Sync {
    fn original(&self) -> &Arc<ProcessEndpointHandoff>;
}
impl OriginalEndpoint for Arc<ProcessEndpointHandoff> {
    fn original(&self) -> &Arc<ProcessEndpointHandoff> {
        self
    }
}
impl RetainedEndpoint {
    pub(in crate::owned_process) fn new(original: Arc<ProcessEndpointHandoff>) -> Self {
        Self(Arc::new(original))
    }
    pub(in crate::owned_process) fn original(&self) -> Arc<ProcessEndpointHandoff> {
        self.0.original().clone()
    }
    pub(in crate::owned_process) fn matches(&self, original: &Arc<ProcessEndpointHandoff>) -> bool {
        Arc::ptr_eq(self.0.original(), original)
    }
}
