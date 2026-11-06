//! The single assembly owner for component lifetimes and accepted configuration work.
//! Protocol lookup and forwarding retain handles; they never create another host.
use crate::apply_owner::ApplyOwner;
use crate::component::{catalog::ComponentCatalog, ComponentRegistry};
use std::sync::{Arc, Mutex};
use zero_config::OutboundConfig;

pub struct RuntimeHost<A = ()> {
    components: ComponentCatalog,
    probes: Arc<crate::probe_owner::ProbeOwner>,
    apply: Arc<ApplyOwner>,
    apply_lock: Arc<tokio::sync::Mutex<()>>,
    reload_ack: Mutex<Option<A>>,
}
impl<A> std::fmt::Debug for RuntimeHost<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHost")
            .field("components", &self.components)
            .field("apply", &self.apply)
            .finish_non_exhaustive()
    }
}
impl<A> RuntimeHost<A> {
    pub fn new(factories: ComponentRegistry<OutboundConfig>) -> Self {
        let probes = Arc::new(crate::probe_owner::ProbeOwner::default());
        let components = ComponentCatalog::with_factories(factories);
        components.attach_probe_owner(Arc::downgrade(&probes));
        Self {
            components,
            probes,
            apply: Arc::default(),
            apply_lock: Arc::default(),
            reload_ack: Mutex::default(),
        }
    }
}

impl<A> RuntimeHost<A> {
    pub fn components(&self) -> &ComponentCatalog {
        &self.components
    }
    pub fn apply(&self) -> &Arc<ApplyOwner> {
        &self.apply
    }
    pub fn apply_lock(&self) -> &Arc<tokio::sync::Mutex<()>> {
        &self.apply_lock
    }
    pub fn reload_ack(&self) -> &Mutex<Option<A>> {
        &self.reload_ack
    }
}

#[cfg(test)]
#[path = "tests/host.rs"]
mod tests;

impl<A> RuntimeHost<A> {
    /// Select the original started Direct registrar from this very accepted plan.
    /// The invoked task is a new real probe, never an existing driver's witness.
    pub async fn run_probe<T, F, Fut>(
        &self,
        config: &Arc<zero_config::RuntimeConfig>,
        direct_tag: Option<&str>,
        credit: crate::ProbeAdmission,
        purpose: zero_core::request::ServicePurpose,
        deadline: tokio::time::Instant,
        work: F,
    ) -> Result<T, zero_platform_tokio::contract::ChannelError>
    where
        T: Send + 'static,
        F: FnOnce(crate::ProbeCall) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + Send + 'static,
    {
        let registrar = self.components.probe_registrar(config, direct_tag)?;
        self.probes
            .run(
                registrar,
                credit,
                std::num::NonZeroUsize::new(config.runtime.effective_diagnostic_max_operations())
                    .ok_or_else(|| {
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::InvalidConfig,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                })?,
                purpose,
                deadline,
                work,
            )
            .await
    }
    pub fn validate_probe_candidate(
        &self,
        config: &zero_config::RuntimeConfig,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.probes
            .validate_limit(config.runtime.effective_diagnostic_max_operations())
    }
    pub fn cancel_probes(&self) {
        self.probes.cancel_all();
    }
    pub fn stop_probes(&self) {
        self.probes.begin_shutdown();
    }
    pub async fn drain_probes(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        self.probes.drain(deadline).await
    }
}
