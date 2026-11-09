//! Fault delivery belongs to each existing charged original row.
use super::*;
use crate::contract::lifecycle::{ResourceFailure, ResourceFailurePhase};
pub(super) struct Retained<T: ?Sized> {
    pub original: Arc<T>,
    pub(super) instance: Option<zero_core::component::ComponentInstanceId>,
    fault: Mutex<Option<Arc<ResourceFailure>>>,
}
impl<T: ?Sized> Retained<T> {
    pub fn new(
        original: Arc<T>,
        instance: Option<zero_core::component::ComponentInstanceId>,
    ) -> Arc<Self> {
        Arc::new(Self {
            original,
            instance,
            fault: Mutex::default(),
        })
    }
    pub fn observe(
        &self,
        source: Option<impl Into<crate::contract::RetainedError>>,
        original: Option<Arc<ResourceFailure>>,
    ) -> Option<Arc<ResourceFailure>> {
        // Foreign sources and discarded candidates drop with no Root row lock.
        if let Some(actual) = self
            .fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Some(actual);
        }
        let candidate = match original {
            Some(original) => Some(original),
            None => Some(Arc::new(ResourceFailure::new(
                self.instance?,
                ResourceFailurePhase::DeclaredResources,
                source?.into(),
            ))),
        };
        let actual = {
            let mut fault = self
                .fault
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if fault.is_none() {
                *fault = candidate.clone();
            }
            fault.clone()
        };
        drop(candidate);
        actual
    }

    pub fn received(
        &self,
        source: Option<impl Into<crate::contract::RetainedError>>,
        original: Option<Arc<ResourceFailure>>,
    ) -> bool {
        if source.is_some() && self.instance.is_none() {
            return false;
        }
        self.observe(source, original)
            .is_none_or(|fault| fault.ownership_received())
    }
}
impl<T: ?Sized> std::ops::Deref for Retained<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.original
    }
}
