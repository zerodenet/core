//! Same actual fact/error/recovery strings remain borrowed during encoding.
use super::*;
pub(in crate::runtime::endpoint) struct EndpointFactQuery<'a>(std::sync::MutexGuard<'a, State>);
impl EndpointFacts {
    pub(in crate::runtime::endpoint) fn query(&self) -> EndpointFactQuery<'_> {
        EndpointFactQuery(
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}
impl EndpointFactQuery<'_> {
    pub(in crate::runtime::endpoint) fn get(&self, id: &str) -> Option<&Fact> {
        self.0.entries.get(id)
    }
}
