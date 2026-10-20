//! Exact service-root authority, separate from public purpose labels/business IO.
use super::ResourceAccess;
use crate::{
    contract::{scope::TaskJoinWitness, ChannelError, Impact, Reason, Stage},
    controlled_datagram::{
        ControlledDatagramControl, ControlledDatagramOwner, ControlledDatagramSocket,
        DatagramAcquisitionError,
    },
    EgressSelection,
};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
use zero_core::component::{ComponentInstanceId, NetworkGeneration};
mod io;
mod receipt;
pub use io::{RootDatagramIo, RootDatagramUse};
pub use receipt::{RootDatagramAttemptFailure, RootDatagramClosed, RootDatagramRetryReceipt};

/// Host-held issuer identity. Creating a different issuer cannot match the
/// original private issuer installed in an actual HostResources provider.
#[derive(Clone)]
pub struct RootDatagramAuthority(Arc<()>);
#[derive(Clone)]
pub struct RootDatagramRequest {
    access: Weak<dyn ResourceAccess>,
    instance: ComponentInstanceId,
    authority: RootDatagramAuthority,
    ordinal: usize,
    peer: SocketAddr,
    generation: NetworkGeneration,
}
#[derive(Clone)]
pub struct RootDatagramGrant {
    access: Weak<dyn ResourceAccess>,
    request: RootDatagramRequest,
    instance: ComponentInstanceId,
}
pub struct GrantedRootDatagram {
    pub io: RootDatagramIo,
    pub selection: EgressSelection,
    pub instance: ComponentInstanceId,
}
impl RootDatagramAuthority {
    pub fn issue() -> Self {
        Self(Arc::new(()))
    }
    pub fn matches_request(&self, request: &RootDatagramRequest) -> bool {
        Arc::ptr_eq(&self.0, &request.authority.0)
    }
    pub fn grant(
        &self,
        access: Weak<dyn ResourceAccess>,
        instance: ComponentInstanceId,
        ordinal: usize,
        peer: SocketAddr,
        generation: NetworkGeneration,
    ) -> RootDatagramGrant {
        RootDatagramGrant {
            access: access.clone(),
            instance,
            request: RootDatagramRequest {
                access,
                instance,
                authority: self.clone(),
                ordinal,
                peer,
                generation,
            },
        }
    }
    /// Invoke after the same control is persistently installed, before handing
    /// out I/O or running any external observer. No native ownership moves here.
    pub fn install_io(
        &self,
        request: &RootDatagramRequest,
        query: &TaskJoinWitness,
        socket: ControlledDatagramSocket,
        control: &ControlledDatagramControl,
    ) -> Result<(RootDatagramIo, RootDatagramUse), ChannelError> {
        if !self.matches_request(request) || !control.identity().matches_socket(&socket) {
            return Err(denied());
        }
        let usage = RootDatagramUse::new(request.clone(), query.clone(), control.identity());
        Ok((RootDatagramIo::new(socket, usage.clone()), usage))
    }
    pub fn acquisition_failure(
        &self,
        request: &RootDatagramRequest,
        query: &TaskJoinWitness,
        error: ChannelError,
        acquisition: DatagramAcquisitionError,
        owner: &ControlledDatagramOwner,
    ) -> RootDatagramAttemptFailure {
        let settled =
            self.matches_request(request) && owner.confirms_settled_acquisition(&acquisition);
        RootDatagramAttemptFailure::acquired(
            error.with_source(acquisition),
            settled.then(|| RootDatagramRetryReceipt::new(request.clone(), query.clone())),
        )
    }
}
impl RootDatagramRequest {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn network_generation(&self) -> NetworkGeneration {
        self.generation
    }
}
impl RootDatagramGrant {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn ordinal(&self) -> usize {
        self.request.ordinal
    }
    pub fn peer(&self) -> SocketAddr {
        self.request.peer
    }
    pub fn network_generation(&self) -> NetworkGeneration {
        self.request.generation
    }
    pub fn same_root(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.request.authority.0, &other.request.authority.0)
            && self.access.ptr_eq(&other.access)
            && self.instance == other.instance
    }
    pub fn open(
        &self,
        peer: SocketAddr,
        expected: NetworkGeneration,
        query: &TaskJoinWitness,
    ) -> Result<GrantedRootDatagram, RootDatagramAttemptFailure> {
        if peer != self.peer() || expected != self.network_generation() {
            return Err(RootDatagramAttemptFailure::denied(denied()));
        }
        self.access
            .upgrade()
            .ok_or_else(|| RootDatagramAttemptFailure::denied(denied()))?
            .open_root_datagram(&self.request, query)
    }
    /// Native-only closure. Query join/users accounting remains with the same
    /// actual root scope and provider; this never signs a task-exit receipt.
    pub fn close(&self, io: RootDatagramIo) -> Result<RootDatagramClosed, ChannelError> {
        if !io.usage().matches_request(&self.request) {
            return Err(denied());
        }
        let usage = io.into_returned();
        self.access
            .upgrade()
            .ok_or_else(denied)?
            .close_root_datagram(usage)
    }
}
fn denied() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
}

#[cfg(test)]
mod tests;
