//! Actual wire allocation custody. Pure accounting, never DNS/IO authority.
use std::{io, sync::Arc, task::Waker};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::PacketDriverMetadataView, scope::TaskScopeIdentity,
};
use zero_traits::packet_metadata::{
    PacketMemoryError, PacketMetadataBudget, PacketMetadataCandidateCredit,
    PacketMetadataCandidateSource, PacketMetadataCredit, PacketMetadataLayout,
};
mod buffer;
mod lock;
pub(crate) use lock::MetadataMutex;
mod response;
mod synthetic;
pub(crate) use buffer::WireBuffer;
pub use response::DnsWireResponse;
#[derive(Clone)]
pub struct DnsWireMemory {
    counter: PacketMetadataBudget,
    pub(crate) metadata: PacketDriverMetadataView,
    source: Option<Source>,
}
#[derive(Clone)]
struct Source {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    provisional: Option<PacketMetadataCandidateSource>,
}
/// No Source, registrar or IO is retained by a wire payload's backing credit.
enum Credit {
    Current(PacketMetadataCredit),
    Provisional(PacketMetadataCandidateCredit),
}
impl std::fmt::Debug for DnsWireMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsWireMemory")
            .field("source_bound", &self.source.is_some())
            .finish_non_exhaustive()
    }
}
impl DnsWireMemory {
    /// The actual Proxy passes its existing Host roots before local/cached
    /// answers can allocate. This does not create a counter or authorize IO.
    pub fn from_host(counter: PacketMetadataBudget, metadata: PacketDriverMetadataView) -> Self {
        Self {
            counter,
            metadata,
            source: None,
        }
    }
    pub fn same_root(&self, other: &Self) -> bool {
        self.counter.same_budget(&other.counter) && self.metadata.same_root(&other.metadata)
    }
    /// Only the actual Factory binds the real target Source instance/Scope.
    /// A provisional permit comes from the same accepted ConfigAttempt and
    /// exact target plan, not a caller-supplied request purpose or generation.
    pub fn for_source(
        &self,
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        provisional: Option<PacketMetadataCandidateSource>,
    ) -> io::Result<Self> {
        if provisional
            .as_ref()
            .is_some_and(|source| !source.same_budget(&self.counter))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS provisional memory belongs to another Root",
            ));
        }
        Ok(Self {
            counter: self.counter.clone(),
            metadata: self.metadata.clone(),
            source: Some(Source {
                instance,
                scope,
                provisional,
            }),
        })
    }
    pub(crate) fn prepare_candidate_allocation(
        &self,
        issuer: &zero_traits::packet_metadata::PacketMetadataCandidateIssuer,
    ) -> io::Result<Option<zero_platform_tokio::contract::dns::DnsWireAllocation>> {
        if !issuer.same_budget(&self.counter) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS warm candidate belongs to another Host",
            ));
        }
        let source = self.source.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS warm memory has no actual Source binding",
            )
        })?;
        if !source.scope.resource_open() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS warm Source Scope is closed",
            ));
        }
        if issuer.is_published() {
            return Ok(None);
        }
        let header = self
            .metadata
            .reserve(
                zero_traits::packet_metadata::PacketMetadataCandidateIssuer::source_layout()
                    .map_err(memory_error)?,
            )
            .map_err(memory_error)?;
        let provisional = issuer.prepare_source(header).map_err(|(error, header)| {
            drop(header);
            memory_error(error)
        })?;
        Ok(Some(
            zero_platform_tokio::contract::dns::DnsWireAllocation::new(
                source.instance,
                source.scope.clone(),
                provisional,
            ),
        ))
    }
    pub(crate) fn matches_scope(&self, scope: &TaskScopeIdentity) -> bool {
        self.source
            .as_ref()
            .is_some_and(|actual| actual.scope.same_scope(scope))
    }
    pub fn matches_source(&self, instance: ComponentInstanceId, scope: &TaskScopeIdentity) -> bool {
        self.source
            .as_ref()
            .is_some_and(|actual| actual.instance == instance && actual.scope.same_scope(scope))
    }
    pub fn revoke_source(&self) {
        if let Some(source) = &self.source {
            if let Some(provisional) = &source.provisional {
                provisional.revoke();
            }
        }
    }
    fn reserve(&self, layout: PacketMetadataLayout) -> io::Result<Credit> {
        if self
            .source
            .as_ref()
            .is_some_and(|source| !source.scope.resource_open())
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS wire Source admission closed",
            ));
        }
        // This concrete byte admission rejects a new allocation; it never
        // creates a waiter/cleanup task or self-waits while holding backing.
        let wake = Waker::noop().clone();
        match self
            .source
            .as_ref()
            .and_then(|source| source.provisional.as_ref())
        {
            Some(provisional) => provisional.reserve(layout, wake).map(Credit::Provisional),
            None => self.counter.reserve(layout, wake).map(Credit::Current),
        }
        .map_err(memory_error)
    }
    fn header<T>(&self) -> io::Result<PacketMetadataCredit> {
        self.metadata
            .reserve(
                zero_traits::packet_metadata::PacketMetadataArc::<T>::layout()
                    .map_err(memory_error)?,
            )
            .map_err(memory_error)
    }
}
fn memory_error(error: PacketMemoryError) -> io::Error {
    io::Error::new(
        match error {
            PacketMemoryError::Busy | PacketMemoryError::Paused => io::ErrorKind::WouldBlock,
            PacketMemoryError::Stopped => io::ErrorKind::NotConnected,
            PacketMemoryError::Capacity | PacketMemoryError::Overflow => io::ErrorKind::OutOfMemory,
        },
        error,
    )
}

pub(crate) async fn read_framed<R: tokio::io::AsyncRead + Unpin>(
    io: &mut R,
) -> io::Result<DnsWireResponse> {
    use tokio::io::AsyncReadExt;
    let size = io.read_u16().await? as usize;
    let memory = crate::OwnedDnsQueryTasks::current_wire_memory();
    let mut bytes = WireBuffer::zeroed(size, memory.as_ref())?;
    io.read_exact(bytes.as_mut()).await?;
    bytes.freeze()
}

/// Mutable input storage for the caller's already owned TCP framing. The
/// buffer grants no read/stream/Scope capability; it only preserves backing.
pub struct DnsWireInput {
    pub(crate) buffer: WireBuffer,
}
impl DnsWireInput {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}
impl std::ops::Deref for DnsWireInput {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buffer
    }
}

impl DnsWireMemory {
    pub(crate) fn for_new_query(&self) -> io::Result<Self> {
        match zero_platform_tokio::contract::dns::current_wire_allocation() {
            Some(allocation) => self.for_source(
                allocation.instance(),
                allocation.scope_identity().clone(),
                Some(allocation.source().clone()),
            ),
            None => Ok(self.clone()),
        }
    }
    pub(crate) fn at_registered_query(&self, scope: TaskScopeIdentity) -> Self {
        let mut memory = self.clone();
        if let Some(source) = &mut memory.source {
            source.scope = scope;
        }
        memory
    }
}
