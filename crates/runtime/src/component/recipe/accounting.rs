//! Typed Host accounting facts, separate from the protocol's secret-free identity.
use super::*;
use zero_platform_tokio::contract::component::{ComponentSpec, ResourceDeclaration as R};
/// Only providers whose actual originals are bound to this Host custody root.
/// This is a cold replacement fact, never an I/O or lifecycle readiness grant.
pub(in crate::component) fn owns_carrier_custody(spec: &ComponentSpec) -> bool {
    spec.resources.iter().any(|resource| {
        matches!(
            resource,
            R::ControlledJournal { .. }
                | R::InboundTargets { .. }
                | R::TcpListener { .. }
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
    pub(in crate::component) fn with_component_task_total(mut self, limit: Option<usize>) -> Self {
        self.component_task_total = limit;
        self
    }
    pub(in crate::component) fn component_task_total(&self) -> Option<usize> {
        self.component_task_total
    }
    pub(in crate::component) fn with_buffer_limits(
        mut self,
        limits: Option<zero_platform_tokio::controlled_carrier::CarrierBufferLimits>,
    ) -> Self {
        self.buffer_limits = limits;
        self
    }
    pub(in crate::component) fn buffer_limits(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierBufferLimits> {
        self.buffer_limits
    }
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

/// Same Root's finite noncached job/request policy; data only, not permission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::component) struct PreparationLimits {
    max_jobs: usize,
    max_requests: usize,
}
impl PreparationLimits {
    pub(in crate::component) fn from_runtime(
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<Self, BuildError> {
        let max_jobs = runtime.effective_navigation_max_jobs();
        let max_requests = runtime.effective_navigation_max_requests();
        if !(1..=64).contains(&max_jobs) || !(1..=32).contains(&max_requests) {
            return Err(super::super::instance::invalid(
                zero_platform_tokio::contract::Reason::InvalidConfig,
                "invalid actual preparation job/request capacities",
            ));
        }
        Ok(Self {
            max_jobs,
            max_requests,
        })
    }
}
impl ComponentRecipe {
    pub(in crate::component) fn with_preparation_limits(
        mut self,
        limits: Option<PreparationLimits>,
    ) -> Self {
        self.preparation_limits = limits;
        self
    }
    pub(in crate::component) fn preparation_limits(&self) -> Option<PreparationLimits> {
        self.preparation_limits
    }
}

/// Concrete metadata policy on actual packet producers, never IO authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::component) struct PacketDriverMetadataLimits {
    allocations: usize,
    bytes: usize,
}
impl PacketDriverMetadataLimits {
    pub(in crate::component) fn from_runtime(
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<Self, BuildError> {
        let allocations = runtime.effective_packet_driver_metadata_total_max_allocations();
        let bytes = runtime.effective_packet_driver_metadata_total_max_bytes();
        if !(1..=1048576).contains(&allocations) || !(1..=268435456).contains(&bytes) {
            return Err(super::super::instance::invalid(
                zero_platform_tokio::contract::Reason::InvalidConfig,
                "invalid actual packet-driver metadata limits",
            ));
        }
        Ok(Self { allocations, bytes })
    }
}
pub(in crate::component) fn owns_packet_driver_metadata(spec: &ComponentSpec) -> bool {
    spec.roles
        .contains(&zero_platform_tokio::contract::component::RoleKind::Packet)
        // Actual native/resource owners may construct masked carrier workers,
        // headers and peer tables before acquiring IO. Their cold accounting
        // identity must match the same policy in non-packet feature builds too.
        // This describes accounting only; it adds no resource or role authority.
        || !spec.resources.is_empty()
}
impl ComponentRecipe {
    pub(in crate::component) fn with_packet_metadata_limits(
        mut self,
        limits: Option<PacketDriverMetadataLimits>,
    ) -> Self {
        self.packet_metadata_limits = limits;
        self
    }
    pub(in crate::component) fn packet_metadata_limits(
        &self,
    ) -> Option<PacketDriverMetadataLimits> {
        self.packet_metadata_limits
    }
}

/// Original SDK request and shared requested bytes; not mapped memory or RSS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::component) struct PacketWindowsRingLimits {
    pub capacity: usize,
    pub total_requested_bytes: usize,
}
impl ComponentRecipe {
    pub(in crate::component) fn with_packet_windows_ring_limits(
        mut self,
        limits: Option<PacketWindowsRingLimits>,
    ) -> Self {
        self.packet_windows_ring_limits = limits;
        self
    }
    pub(in crate::component) fn packet_windows_ring_limits(
        &self,
    ) -> Option<PacketWindowsRingLimits> {
        self.packet_windows_ring_limits
    }
}
