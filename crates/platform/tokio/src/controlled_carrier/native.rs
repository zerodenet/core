//! Explicit library provider for one literal empty-prefix root and one grantee.
use super::{connect_error, error, grant::CarrierGrant, prepared::PreparedCarrier};
use crate::{
    contract::{
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        stream::BoxStream,
        BoxFuture, ChannelError, DeliveryState, Reason, Stage,
    },
    EgressInterfaceControl, PendingTcpConnect,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};
use zero_core::{component::ComponentInstanceId, request::Destination, Address};
use zero_traits::IoObserver;
mod packet;
mod stream;
/// No public defaults and no production registration. The host must explicitly
/// grant this exact component generation, selected peer, binding and finite owner.
pub struct NativeCarrierOpener {
    grantee: ComponentInstanceId,
    binding: UpstreamBinding,
    peer: SocketAddr,
    pub(super) endpoint: Destination,
    egress: Arc<EgressInterfaceControl>,
    observer: Option<Arc<dyn IoObserver>>,
    business: Option<super::CarrierBusinessGrant>,
    service: Option<super::CarrierServiceGrant>,
    dns: Option<(
        Arc<dyn crate::contract::dns::DnsService>,
        crate::contract::dns::DnsGeneration,
    )>,
}
mod relay;
impl NativeCarrierOpener {
    pub fn new(
        grantee: ComponentInstanceId,
        binding: UpstreamBinding,
        peer: SocketAddr,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        if !binding.prefix.is_empty()
            || peer.port() == 0
            || binding.network_generation.0 != egress.generation()
        {
            return Err(error(
                grantee,
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(Self {
            grantee,
            binding,
            peer,
            endpoint: super::resolution::destination(peer),
            egress,
            observer,
            business: None,
            service: None,
            dns: None,
        })
    }
    /// A distinct host capability, pinned to the original registered slot. The
    /// ordinary constructor remains Service-only for every existing caller.
    pub fn business(
        grant: super::CarrierBusinessGrant,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        grant.check_liveness()?;
        let instance = grant.binding().registration().instance();
        let endpoint = grant.endpoint();
        let ip = match endpoint.address {
            zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(ip).into(),
            zero_core::Address::Ipv6(ip) => std::net::Ipv6Addr::from(ip).into(),
            zero_core::Address::Domain(_) => {
                return Err(error(
                    instance,
                    Stage::Prepare,
                    Reason::Unsupported,
                    DeliveryState::NotSubmitted,
                ))
            }
        };
        let mut native = Self::new(
            instance,
            grant.upstream().clone(),
            SocketAddr::new(ip, endpoint.port),
            egress,
            observer,
        )?;
        native.business = Some(grant);
        Ok(native)
    }
    /// Host-only selection from its actual Node DNS answer. The sealed business
    /// ticket keeps the original domain, caller, slot and binding authoritative;
    /// a selected address does not mint another endpoint or host permission.
    pub fn business_resolved(
        grant: super::CarrierBusinessGrant,
        peer: SocketAddr,
        resolver: Arc<dyn crate::contract::dns::DnsService>,
        generation: crate::contract::dns::DnsGeneration,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        grant.check_liveness()?;
        let instance = grant.binding().registration().instance();
        let endpoint = grant.endpoint().clone();
        if !matches!(endpoint.address, Address::Domain(_))
            || endpoint.port != peer.port()
            || peer.ip().is_unspecified()
            || resolver.generation() != generation
        {
            return Err(error(
                instance,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let mut native = Self::new(instance, grant.upstream().clone(), peer, egress, observer)?;
        native.endpoint = endpoint;
        native.business = Some(grant);
        native.dns = Some((resolver, generation));
        Ok(native)
    }
    /// Actual Service-root ticket, already bound to its original caller/view.
    /// The host provider independently validates its private root authority.
    pub fn service(
        grant: super::CarrierServiceGrant,
        egress: Arc<EgressInterfaceControl>,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> Result<Self, ChannelError> {
        grant.check_liveness()?;
        let instance = grant.binding().registration().instance();
        let endpoint = grant.endpoint();
        let ip = match endpoint.address {
            Address::Ipv4(ip) => std::net::Ipv4Addr::from(ip).into(),
            Address::Ipv6(ip) => std::net::Ipv6Addr::from(ip).into(),
            Address::Domain(_) => {
                return Err(error(
                    instance,
                    Stage::Prepare,
                    Reason::Unsupported,
                    DeliveryState::NotSubmitted,
                ))
            }
        };
        let mut native = Self::new(
            instance,
            grant.upstream().clone(),
            SocketAddr::new(ip, endpoint.port),
            egress,
            observer,
        )?;
        native.service = Some(grant);
        Ok(native)
    }
}
impl UpstreamOpener for NativeCarrierOpener {
    fn resolve_controlled_carrier(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<super::CarrierCandidates, ChannelError>> {
        let source = super::resolution::Source {
            grantee: self.grantee,
            binding: self.binding.clone(),
            endpoint: self.endpoint.clone(),
            egress: self.egress.clone(),
            observer: self.observer.clone(),
        };
        let result = source
            .check(binding, &destination, &context)
            .map(|()| source.candidates(vec![self.peer], context));
        Box::pin(async move { result })
    }
    fn prepare_controlled_datagram(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: CarrierGrant,
    ) -> Result<super::CarrierDatagram, ChannelError> {
        self.packet(binding, destination, context, grant)
    }
    fn prepare_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        self.prepare(binding, destination, context, grant)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async move {
            Err(error(
                self.grantee,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async move {
            Err(error(
                self.grantee,
                Stage::Prepare,
                Reason::Unsupported,
                DeliveryState::NotSubmitted,
            ))
        })
    }
}
