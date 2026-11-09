//! Only candidate memory custody, issued by the actual Host Node source.
//! This context creates no task, IO grant, resolver, native or ACK authority.
use super::*;
use crate::contract::scope::TaskScopeIdentity;
use zero_core::component::ComponentInstanceId;
use zero_traits::packet_metadata::PacketMetadataCandidateSource;
#[derive(Clone)]
pub struct DnsWireAllocation {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    source: PacketMetadataCandidateSource,
}
tokio::task_local! {static CURRENT:DnsWireAllocation;}
pub fn current_wire_allocation() -> Option<DnsWireAllocation> {
    CURRENT.try_with(Clone::clone).ok()
}
impl DnsWireAllocation {
    /// Pure candidate-memory metadata, not an IO/Scope/task capability. The
    /// actual Host caller must supply its verified installed Source identity.
    pub fn new(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        source: PacketMetadataCandidateSource,
    ) -> Self {
        Self {
            instance,
            scope,
            source,
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn scope_identity(&self) -> &TaskScopeIdentity {
        &self.scope
    }
    pub fn source(&self) -> &PacketMetadataCandidateSource {
        &self.source
    }
    /// Poll the caller's same future with metadata only. No execution/cleanup
    /// task is created, and the context grants no DNS or network operation.
    pub async fn with_memory<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        CURRENT.scope(self.clone(), work).await
    }
}
