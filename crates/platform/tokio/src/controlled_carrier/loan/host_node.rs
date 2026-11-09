//! The same catalog-installed Node service, bound with its actual private fence.
use crate::contract::{dns::DnsService, CancellationToken};
use std::sync::Arc;
/// Read-only Node source for one actual HostResources. Protocol BuildContext
/// has no way to install its own source or obtain the private fence Arc.
#[derive(Clone)]
pub struct HostNodeDnsSource {
    pub(in crate::controlled_carrier) source: Arc<dyn DnsService>,
    pub(in crate::controlled_carrier) host_fence: Arc<CancellationToken>,
    memory: Option<Memory>,
    issuer: Option<zero_traits::packet_metadata::PacketMetadataCandidateIssuer>,
}
impl HostNodeDnsSource {
    /// Actual Host construction only. The original private fence is checked
    /// against the Host-issued target issuer before any resolver can be called.
    /// A new Arc around a cloned cancellation token is a foreign identity.
    pub fn for_host(source: Arc<dyn DnsService>, host_fence: &Arc<CancellationToken>) -> Self {
        Self {
            source,
            host_fence: host_fence.clone(),
            memory: None,
            issuer: None,
        }
    }
}

#[derive(Clone)]
struct Memory {
    counter: zero_traits::packet_metadata::PacketMetadataBudget,
    metadata: crate::contract::component::PacketDriverMetadataView,
    instance: zero_core::component::ComponentInstanceId,
    scope: crate::contract::scope::TaskScopeIdentity,
}
impl HostNodeDnsSource {
    /// Actual Host assembly adds only its original scope and memory facts;
    /// the existing private fence continues to seal every IO issuer.
    pub fn with_memory_root(
        mut self,
        counter: zero_traits::packet_metadata::PacketMetadataBudget,
        metadata: crate::contract::component::PacketDriverMetadataView,
        instance: zero_core::component::ComponentInstanceId,
        scope: crate::contract::scope::TaskScopeIdentity,
    ) -> Self {
        self.memory = Some(Memory {
            counter,
            metadata,
            instance,
            scope,
        });
        self
    }
    /// Only the actual target ComponentPlan adds this candidate's pure issuer.
    /// Existing Source/Slot values and old plans remain under the old ceiling.
    pub fn with_wire_issuer(
        mut self,
        issuer: zero_traits::packet_metadata::PacketMetadataCandidateIssuer,
    ) -> Result<Self, crate::contract::ChannelError> {
        if !self
            .memory
            .as_ref()
            .is_some_and(|memory| issuer.same_budget(&memory.counter))
        {
            return Err(crate::contract::ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::StalePlan,
                crate::contract::Impact::Component,
            ));
        }
        self.issuer = Some(issuer);
        Ok(self)
    }
    /// Pure memory reservation only; callers still use the exact existing
    /// controlled issuer/host fence before invoking the actual Node resolver.
    pub fn prepare_wire_allocation(
        &self,
    ) -> Result<Option<crate::contract::dns::DnsWireAllocation>, crate::contract::ChannelError>
    {
        let Some(issuer) = &self.issuer else {
            return Ok(None);
        };
        if issuer.is_published() {
            return Ok(None);
        }
        let memory = self
            .memory
            .as_ref()
            .expect("candidate matched actual Host memory");
        let fail = |source| {
            crate::contract::ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::Capacity,
                crate::contract::Impact::Component,
            )
            .with_source(source)
        };
        if !memory.scope.resource_open() {
            return Err(crate::contract::ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::Revoked,
                crate::contract::Impact::Component,
            ));
        }
        let header = memory
            .metadata
            .reserve(
                zero_traits::packet_metadata::PacketMetadataCandidateIssuer::source_layout()
                    .map_err(fail)?,
            )
            .map_err(fail)?;
        let source = issuer.prepare_source(header).map_err(|(error, header)| {
            drop(header);
            fail(error)
        })?;
        Ok(Some(crate::contract::dns::DnsWireAllocation::new(
            memory.instance,
            memory.scope.clone(),
            source,
        )))
    }
}
