//! Read-only retention of the actual Host's existing pure declared cause set.
use std::{error::Error, fmt, sync::Arc};
use zero_core::component::ComponentInstanceId;

/// Diagnostic data only. This holds neither an owner, Scope, controller nor IO.
/// Host assembly constructs it from its existing cause capsule; a protocol must
/// not return this capsule to that same declaration receiver (which would form
/// a source cycle). Keeping it alive does not acknowledge any contained fault.
#[derive(Clone)]
pub struct DeclaredResourceSources {
    instance: ComponentInstanceId,
    sources: Arc<dyn Error + Send + Sync>,
}
impl DeclaredResourceSources {
    /// Host projection only; no authority is created by supplying error data.
    pub fn from_host(instance: ComponentInstanceId, sources: Arc<dyn Error + Send + Sync>) -> Self {
        Self { instance, sources }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
}
impl fmt::Debug for DeclaredResourceSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeclaredResourceSources")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for DeclaredResourceSources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actual declared resource sources")
    }
}
impl Error for DeclaredResourceSources {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.sources.as_ref())
    }
}
