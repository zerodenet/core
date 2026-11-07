//! One actual Host declaration graph's bounded, pure fault receiver.
//! Its first fault stops new admissions. Later causes are bounded by the already
//! admitted original graph and declared providers, not total historical queries.
use super::*;
use std::{error::Error, fmt};
use zero_platform_tokio::contract::lifecycle::{
    CloseReport, ResourceFailure, ResourceFailurePhase,
};
#[derive(Default)]
pub(in crate::component) struct DeclaredReceiver {
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
        f.write_str("actual declared resource graph failure")
    }
}
impl Error for Causes {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.first.get().map(|source| source.as_ref() as _)
    }
}
impl DeclaredReceiver {
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
                    ResourceFailurePhase::DeclaredResources,
                    Arc::new(source),
                ))
            })
            .clone()
    }
    pub(in crate::component) fn get(&self) -> Option<Arc<ResourceFailure>> {
        self.canonical.get().cloned()
    }
    fn original(
        &self,
        instance: zero_core::component::ComponentInstanceId,
        source: Arc<ChannelError>,
    ) {
        if self
            .canonical
            .get()
            .is_some_and(|same| Arc::ptr_eq(&same.source, &source))
        {
            return;
        }
        let mut causes = self
            .causes
            .sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !causes
            .iter()
            .any(|cause| matches!(cause, Cause::Original(same) if Arc::ptr_eq(same, &source)))
        {
            causes.push(Cause::Original(source.clone()));
        }
        drop(causes);
        self.causes.first.get_or_init(|| source);
        self.canonical(instance);
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
            .is_some_and(|same| Arc::ptr_eq(&same.source, &child.source))
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
    pub(super) fn retain_resource_error(&self, source: Arc<ChannelError>) {
        if let Some(instance) = self.instance {
            self.cancel.cancel();
            self.declared_failures.original(instance, source);
        }
    }
}

#[cfg(test)]
mod tests;
