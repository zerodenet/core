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
        source: Option<Arc<ChannelError>>,
        original: Option<Arc<ResourceFailure>>,
    ) -> Option<Arc<ResourceFailure>> {
        let mut fault = self
            .fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if fault.is_none() {
            *fault = original.or_else(|| {
                Some(Arc::new(ResourceFailure::new(
                    self.instance?,
                    ResourceFailurePhase::DeclaredResources,
                    source?,
                )))
            });
        }
        fault.clone()
    }
    pub fn received(
        &self,
        source: Option<Arc<ChannelError>>,
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
