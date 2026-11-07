//! One actual Host controller's pure fault receiver.
//! Concrete providers must supply stable per-original/phase source identities;
//! sealing admission alone does not bound independent cleanup errors.
use super::*;
use std::{error::Error, fmt};
use zero_platform_tokio::contract::lifecycle::{
    CloseReport, ResourceFailure, ResourceFailurePhase,
};
pub(in crate::component) struct DeclaredReceiver {
    controller: ResourceFailurePhase,
    canonical: std::sync::OnceLock<Arc<ResourceFailure>>,
    causes: Arc<Causes>,
}
#[derive(Default, Debug)]
struct Causes {
    first: std::sync::OnceLock<Arc<ChannelError>>,
    sources: Mutex<Vec<Cause>>,
}
#[derive(Debug)]
enum Cause {
    Original(Arc<ChannelError>),
    Child(Arc<ResourceFailure>),
}
impl fmt::Display for Causes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("actual component resource controller failure")
    }
}
impl Error for Causes {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.first.get().map(|source| source.as_ref() as _)
    }
}
impl Default for DeclaredReceiver {
    fn default() -> Self {
        Self {
            controller: ResourceFailurePhase::DeclaredResources,
            canonical: Default::default(),
            causes: Default::default(),
        }
    }
}
impl DeclaredReceiver {
    pub(in crate::component) fn protocol() -> Self {
        Self {
            controller: ResourceFailurePhase::ProtocolLifecycle,
            ..Self::default()
        }
    }
    fn canonical(
        &self,
        instance: zero_core::component::ComponentInstanceId,
    ) -> Arc<ResourceFailure> {
        self.canonical
            .get_or_init(|| {
                let receipt = zero_platform_tokio::contract::CleanupFailureReceipt::new();
                assert!(receipt.bind_payload(self.causes.clone()));
                let mut source = ChannelError::new(
                    zero_platform_tokio::contract::Stage::Close,
                    Reason::Io,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(self.causes.clone())
                .with_cleanup_failure_receipt(receipt);
                source.component = Some(instance.component);
                Arc::new(ResourceFailure::new(
                    instance,
                    self.controller,
                    Arc::new(source),
                ))
            })
            .clone()
    }
    #[cfg(test)]
    pub(in crate::component) fn source_count(&self) -> usize {
        self.causes.sources.lock().unwrap().len()
    }
    pub(in crate::component) fn get(&self) -> Option<Arc<ResourceFailure>> {
        self.canonical.get().cloned()
    }
    pub(in crate::component) fn original(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        source: Arc<ChannelError>,
    ) {
        if self
            .canonical
            .get()
            .is_some_and(|same| same_source(&same.source, &source))
        {
            return;
        }
        let mut causes = self
            .causes
            .sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !causes.iter().any(|cause| match cause {
            Cause::Original(same) => same_source(same, &source),
            Cause::Child(same) => same_source(&same.source, &source),
        }) {
            causes.push(Cause::Original(source.clone()));
        }
        drop(causes);
        self.causes.first.get_or_init(|| source.clone());
        self.canonical(instance);
        // The receiver owns this source or the same stable payload before its
        // producer receives local acceptance. Never wait for the parent archive:
        // the producer may need this handoff to finish actual cleanup. This is
        // not NativeClosed, task completion, Host ACK or external publication.
        if let Some(receipt) = source.cleanup_failure_receipt() {
            let _ = receipt.acknowledge_retained(&source);
        }
    }
    pub(in crate::component) fn child(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        child: Arc<ResourceFailure>,
    ) {
        if self
            .canonical
            .get()
            .is_some_and(|same| Arc::ptr_eq(same, &child))
        {
            return;
        }
        if self
            .canonical
            .get()
            .is_some_and(|same| same_source(&same.source, &child.source))
        {
            let _ = child.acknowledge_retained(&child);
            return;
        }
        let mut causes = self
            .causes
            .sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !causes
            .iter()
            .any(|cause| matches!(cause, Cause::Child(same) if Arc::ptr_eq(same, &child)))
        {
            causes.push(Cause::Child(child.clone()));
        }
        drop(causes);
        self.causes.first.get_or_init(|| child.source.clone());
        self.canonical(instance);
        // The canonical receiver now strongly owns the actual child and source.
        // This local handoff is never publication, NativeClosed or Host ACK.
        let _ = child.acknowledge_retained(&child);
    }
}
impl HostResources {
    pub(in crate::component) fn resource_fault(&self) -> Option<Arc<ResourceFailure>> {
        let instance = self.instance?;
        // All existing acquisition sites already share this same cancellation.
        // Source callbacks/native close never execute under the receiver lock.
        for failure in self.originals.carrier.resource_failures() {
            if failure.instance == instance {
                self.cancel.cancel();
                self.declared_failures.child(instance, failure);
            }
        }
        let evidence = self.io_evidence();
        for source in evidence.sticky_faults {
            self.cancel.cancel();
            self.declared_failures.original(instance, source);
        }
        self.declared_failures.get()
    }
    pub(in crate::component) fn retain_resource_report(&self, report: &mut CloseReport) {
        let Some(instance) = self.instance else {
            return;
        };
        let incoming = std::mem::take(&mut report.resource_failures);
        for fault in incoming {
            if fault.instance == instance
                && fault.controller == ResourceFailurePhase::DeclaredResources
            {
                self.cancel.cancel();
                self.declared_failures.child(instance, fault);
            } else {
                report.resource_failures.push(fault);
            }
        }
        if let Some(fault) = self.resource_fault() {
            report.resource_failures.push(fault);
        }
    }
    pub(in crate::component) fn retain_resource_error(&self, source: Arc<ChannelError>) {
        if let Some(instance) = self.instance {
            self.cancel.cancel();
            self.declared_failures.original(instance, source);
        }
    }
}

#[cfg(test)]
mod tests;

// Error identity only; no source traversal is used as native/ACK evidence.
fn same_source<'a>(mut first: &'a Arc<ChannelError>, mut later: &'a Arc<ChannelError>) -> bool {
    if first.cleanup_failure_receipt().is_some_and(|receipt| {
        later
            .cleanup_failure_receipt()
            .is_some_and(|actual| receipt.same_receipt(actual))
    }) {
        return true;
    }
    while let Some(source) = first.retained_source() {
        first = source;
    }
    while let Some(source) = later.retained_source() {
        later = source;
    }
    Arc::ptr_eq(first, later)
}
