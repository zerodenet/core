//! One retired dispatcher's actual fault receiver; no native or join authority.
use super::*;
use std::{error::Error, fmt};

/// Keeps all original independent failures after the application's physically
/// retired slot is cleared. Native closure is observed separately on the handle.
/// The children/resources are the existing finite owner inventories, not copies
/// of handles or a new registry. A source-thread panic payload stays in children.
pub struct DispatcherRetirementFailure {
    parent: Option<Arc<tokio::task::JoinError>>,
    child: Option<Arc<tokio::task::JoinError>>,
    preparation: Option<Arc<ConnectorError>>,
    files: Option<Arc<std::io::Error>>,
    _children: Arc<DispatcherChildren>,
    _resources: DispatcherResources,
}
impl DispatcherRetirementFailure {
    /// Diagnostic equality with a readiness error already returned by this
    /// actual handle. This is not evidence of native or task retirement.
    pub fn is_only_preparation(&self, original: &Arc<ConnectorError>) -> bool {
        self.parent.is_none()
            && self.child.is_none()
            && self.files.is_none()
            && self._children.failure_count() == 0
            && self
                .preparation
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, original))
    }
}
impl fmt::Debug for DispatcherRetirementFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DispatcherRetirementFailure")
            .field("parent", &self.parent)
            .field("child", &self.child)
            .field("preparation", &self.preparation)
            .field("files", &self.files)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for DispatcherRetirementFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original dispatcher failures retained after cleanup was driven")
    }
}
impl Error for DispatcherRetirementFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.parent
            .as_ref()
            .map(|error| error as _)
            .or_else(|| self.child.as_ref().map(|error| error as _))
            .or_else(|| self.preparation.as_ref().map(|error| error as _))
            .or_else(|| self.files.as_ref().map(|error| error.as_ref() as _))
    }
}
impl EventDispatcherHandle {
    pub(super) fn confirm_retirement(&self, files: std::io::Result<()>) -> ConnectorResult<()> {
        let parent = self.original_shutdown_failure();
        let preparation = self.original_preparation_failure();
        let files = files.err().map(Arc::new);
        if parent.is_some()
            || preparation.is_some()
            || files.is_some()
            || self.children.failure_count() != 0
        {
            return Err(DispatcherRetirementFailure {
                parent,
                child: self.children.first_failure(),
                preparation,
                files,
                _children: self.children.clone(),
                _resources: self.resources.clone(),
            }
            .into());
        }
        if !self.retirement_complete() {
            return Err(ConnectorError::Config(
                "original dispatcher native handles or joins remain unsettled".into(),
            ));
        }
        Ok(())
    }
}
