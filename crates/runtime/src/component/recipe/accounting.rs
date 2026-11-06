//! Typed Host accounting facts, separate from the protocol's secret-free identity.
use super::*;
use zero_platform_tokio::contract::component::{ComponentSpec, ResourceDeclaration as R};
/// Only providers whose actual originals are bound to this Host custody root.
/// This is a cold replacement fact, never an I/O or lifecycle readiness grant.
pub(in crate::component) fn owns_carrier_custody(spec: &ComponentSpec) -> bool {
    spec.resources.iter().any(|resource| {
        matches!(
            resource,
            R::TcpListener { .. }
                | R::ListenerHandoff { .. }
                | R::DatagramListener { .. }
                | R::MetadataDatagram { .. }
                | R::NativeIcmp { .. }
                | R::ControlledCarrierPool { .. }
                | R::ControlledServiceStream { .. }
                | R::ControlledAssociationPool { .. }
                | R::ControlledDatagramCarrier { .. }
                | R::ControlledOutboundStream { .. }
                | R::ControlledDirectStream { .. }
                | R::ControlledNodeStream { .. }
                | R::ControlledNodeCarrier { .. }
                | R::ControlledMetadataDatagram { .. }
                | R::ControlledMetadataCarrier { .. }
                | R::BoundOutboundDatagram { .. }
        )
    })
}
impl ComponentRecipe {
    pub(in crate::component) fn with_carrier_total_limit(mut self, limit: Option<usize>) -> Self {
        self.carrier_total = limit;
        self
    }
    pub(in crate::component) fn carrier_total_limit(&self) -> Option<usize> {
        self.carrier_total
    }
}

/// The same Host Process budget's actual job and per-instance dimensions.
/// Native jobs/actors/derived reservations are never reduced to a fit counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::component) struct ProcessLimits {
    pub(super) max_jobs: usize,
    pub(super) max_jobs_per_instance: usize,
}
impl ProcessLimits {
    pub(in crate::component) fn from_runtime(
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<Self, BuildError> {
        let max_jobs = runtime.effective_process_max_jobs();
        let max_jobs_per_instance = runtime.effective_process_max_jobs_per_instance();
        if !(1..=4096).contains(&max_jobs) || !(1..=max_jobs).contains(&max_jobs_per_instance) {
            return Err(super::super::instance::invalid(
                zero_platform_tokio::contract::Reason::InvalidConfig,
                "invalid actual Process job capacities",
            ));
        }
        Ok(Self {
            max_jobs,
            max_jobs_per_instance,
        })
    }
}
pub(in crate::component) fn owns_process_custody(spec: &ComponentSpec) -> bool {
    spec.resources
        .iter()
        .any(|resource| matches!(resource, R::Process { .. } | R::ProcessEndpoint { .. }))
}
impl ComponentRecipe {
    pub(in crate::component) fn with_process_limits(
        mut self,
        limits: Option<ProcessLimits>,
    ) -> Self {
        self.process_limits = limits;
        self
    }
    pub(in crate::component) fn process_limits(&self) -> Option<ProcessLimits> {
        self.process_limits
    }
}
