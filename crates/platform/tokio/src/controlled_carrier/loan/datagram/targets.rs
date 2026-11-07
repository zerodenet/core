//! Selected numeric target for this actual noncached Service original only.
//! No native acquisition or cached-driver privilege is carried by this proof.
use crate::{
    contract::{
        context::RequestIdentity,
        dns::{DnsGeneration, DnsQueryContext, DnsQueryRole, DnsRequest, DnsService},
        network::{CarrierContext, UpstreamBinding},
        scope::TaskJoinWitness,
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{binding::CarrierOwnerBinding, slot::Slot, CarrierGrant},
};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
use zero_core::{component::NetworkGeneration, request::Destination, Address};
mod issuer;
mod resolve;

/// Issued by the same Host's registered receiver/private fence before lookup.
/// One issuer belongs to one exact original Slot and its actual live setup Aux.
pub struct ServiceDatagramTargetIssuer {
    original: Weak<Slot>,
    bound: CarrierOwnerBinding,
    actor: TaskJoinWitness,
    endpoint: Destination,
    binding: UpstreamBinding,
    identity: RequestIdentity,
    call: CarrierContext,
}
/// One whole real Service demand's actual Node answer. A failed attempt's
/// joined/cancelled setup never authorizes another Slot or replaces this query.
/// Actual subsequent attempts need their own fresh Host issuer and real Aux.
pub struct BorrowedDatagramCandidates {
    bound: CarrierOwnerBinding,
    endpoint: Destination,
    binding: UpstreamBinding,
    identity: RequestIdentity,
    call: CarrierContext,
    peers: CandidatePeers,
    resolver: Option<(Arc<dyn DnsService>, DnsGeneration)>,
}
// Move the original DNS answer allocation. A literal target uses no vector.
enum CandidatePeers {
    Literal(SocketAddr),
    Answer(Vec<zero_traits::IpAddress>),
}
impl CandidatePeers {
    fn len(&self) -> usize {
        match self {
            Self::Literal(_) => 1,
            Self::Answer(answer) => answer.len(),
        }
    }
    fn get(&self, index: usize, port: u16) -> Option<SocketAddr> {
        match self {
            Self::Literal(peer) => (index == 0).then_some(*peer),
            Self::Answer(answer) => answer.get(index).map(|ip| {
                SocketAddr::new(
                    match ip {
                        zero_traits::IpAddress::V4(ip) => std::net::IpAddr::from(*ip),
                        zero_traits::IpAddress::V6(ip) => std::net::IpAddr::from(*ip),
                    },
                    port,
                )
            }),
        }
    }
}
fn failure(instance: zero_core::component::ComponentInstanceId, reason: Reason) -> ChannelError {
    crate::controlled_carrier::error(
        instance,
        Stage::Prepare,
        reason,
        DeliveryState::NotSubmitted,
    )
}
fn same_service(a: &RequestIdentity, b: &RequestIdentity) -> bool {
    matches!((a,b),(RequestIdentity::Service(a),RequestIdentity::Service(b)) if crate::contract::context::ServiceFactsOwner::same_owner(a,b))
}
fn literal(peer: SocketAddr) -> Destination {
    Destination::new(
        match peer.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        peer.port(),
    )
}
impl BorrowedDatagramCandidates {
    pub fn len(&self) -> usize {
        self.peers.len()
    }
    pub fn is_empty(&self) -> bool {
        self.peers.len() == 0
    }
    pub fn peer(&self, index: usize) -> Result<SocketAddr, ChannelError> {
        self.check()?;
        let instance = self.bound.registration().instance();
        let peer = self
            .peers
            .get(index, self.endpoint.port)
            .ok_or_else(|| failure(instance, Reason::InvalidConfig))?;
        if peer.port() == 0 || peer.ip().is_unspecified() {
            return Err(failure(instance, Reason::Io));
        }
        Ok(peer)
    }
    fn check(&self) -> Result<(), ChannelError> {
        let instance = self.bound.registration().instance();
        let current = self.bound.registration().bound();
        if !self.bound.authority.available()
            || current.is_none_or(|current| !current.matches_authority(&self.bound.authority))
            || self
                .resolver
                .as_ref()
                .is_some_and(|(resolver, generation)| resolver.generation() != *generation)
        {
            return Err(failure(instance, Reason::StalePlan));
        }
        if self.call.cancel.is_cancelled() {
            return Err(failure(instance, Reason::Cancelled));
        }
        if self
            .call
            .deadline
            .is_some_and(|end| tokio::time::Instant::now() >= end)
        {
            return Err(failure(instance, Reason::Timeout));
        }
        Ok(())
    }
    /// One selected borrow per actual original. Later address attempts need
    /// their own fresh grant/Aux after the previous original really retires.
    pub fn select(
        &self,
        index: usize,
        issuer: ServiceDatagramTargetIssuer,
    ) -> Result<ServiceDatagramTarget, ChannelError> {
        self.check()?;
        issuer.check()?;
        let instance = self.bound.registration().instance();
        if !self.bound.matches_authority(&issuer.bound.authority)
            || self.endpoint != issuer.endpoint
            || self.binding != issuer.binding
            || !same_service(&self.identity, &issuer.identity)
            || self.call.deadline != issuer.call.deadline
        {
            return Err(failure(instance, Reason::StalePlan));
        }
        let peer = self.peer(index)?;
        issuer.select_once()?;
        Ok(ServiceDatagramTarget {
            issuer,
            peer,
            resolver: self.resolver.clone(),
        })
    }
}

mod selected;
pub use selected::{ServiceDatagramTarget, ServiceDatagramTargetLoan};
