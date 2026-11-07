//! Shared original Tokio outcome, distinct from its real Scope receipt.
use std::sync::Arc;
#[derive(Debug)]
pub struct TaskJoinError(Arc<tokio::task::JoinError>);
impl TaskJoinError {
    pub(in crate::contract::scope) fn new(original: tokio::task::JoinError) -> Self {
        Self(Arc::new(original))
    }
    pub fn original(&self) -> &Arc<tokio::task::JoinError> {
        &self.0
    }
}
impl std::fmt::Display for TaskJoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for TaskJoinError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
