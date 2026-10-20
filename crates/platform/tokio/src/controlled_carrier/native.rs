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
/// No public defaults and no production registration. The host must explicitly
/// grant this exact component generation, selected peer, binding and finite owner.
pub struct NativeCarrierOpener {
    grantee: ComponentInstanceId,
    binding: UpstreamBinding,
    peer: SocketAddr,
    egress: Arc<EgressInterfaceControl>,
    observer: Option<Arc<dyn IoObserver>>,
    business: Option<super::CarrierBusinessGrant>,
    service: Option<super::CarrierServiceGrant>,
}
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
            egress,
            observer,
            business: None,
            service: None,
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

    fn prepare(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        mut grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        grant.check()?;
        if let Some(business) = &self.business {
            business.check(&grant, binding, &destination, &context)?;
            business.bind_caller(&grant.slot)?;
        }
        if let Some(service) = &self.service {
            service.check(&grant, binding, &destination, &context)?;
            service.bind_caller(&grant.slot)?;
        }
        let bound = grant.slot.owner.upgrade().and_then(|owner| {
            let state = owner.state();
            state.binding.clone()
        });
        if bound
            .as_ref()
            .is_some_and(|bound| !bound.permits_native(binding, &destination, &context))
        {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let address = match self.peer.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        if grant.instance() != self.grantee
            || binding != &self.binding
            || destination != Destination::new(address, self.peer.port())
            || self.egress.generation() != binding.network_generation.0
            || (context.session.is_some() && self.business.is_none())
        {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if context.cancel.is_cancelled() {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::Cancelled,
                DeliveryState::NotSubmitted,
            ));
        }
        if context.deadline.is_some_and(|end| Instant::now() >= end) {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::Timeout,
                DeliveryState::NotSubmitted,
            ));
        }
        let selection = self
            .egress
            .select_for_peer_with_policy(self.peer, &binding.dial)
            .map_err(|source| {
                error(
                    grant.instance(),
                    Stage::Prepare,
                    Reason::Io,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
            })?;
        if selection.generation() != binding.network_generation.0 {
            return Err(error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        // Host demand callbacks must run outside every carrier/native lock.
        let business_cancel = self
            .business
            .as_ref()
            .map(|business| business.cancellation())
            .or_else(|| self.service.as_ref().map(|service| service.cancellation()));
        {
            let mut state = grant.slot.state();
            if grant.slot.cancel.is_cancelled() || state.closed {
                return Err(error(
                    grant.instance(),
                    Stage::Prepare,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                ));
            }
            state.opening = true;
            if state.business_cancel.is_none() {
                state.business_cancel = business_cancel;
            }
            // A synchronous acquisition panic/error must not return capacity
            // based on implicit Drop of an already created native socket.
            state.unknown = Some(Arc::new(
                error(
                    grant.instance(),
                    Stage::Prepare,
                    Reason::Internal,
                    DeliveryState::Unknown,
                )
                .with_source(std::io::Error::other(
                    "native TCP preparation did not return an observed ownership result",
                )),
            ));
        }
        let (pending, control) = match PendingTcpConnect::prepare(
            self.peer,
            &binding.dial,
            &selection,
            grant.slot.cancel.clone(),
            self.observer.clone(),
        ) {
            Ok(value) => value,
            Err(source) => {
                let no_handle = source.stage() == "validate_dial_policy";
                let source = Arc::new(source);
                let failure = connect_error(grant.instance(), source.clone());
                grant.slot.state().unknown = if no_handle {
                    None
                } else {
                    Some(Arc::new(connect_error(grant.instance(), source)))
                };
                return Err(failure);
            }
        };
        {
            let mut state = grant.slot.state();
            // Sole original control is installed before PreparedCarrier exists
            // and before its inline connect can ever reach a first await.
            state.close_observer = Some(control.close_observer());
            state.control = Some(Arc::new(Mutex::new(control)));
            state.unknown = None;
        }
        super::users::acquired(&grant.slot);
        let end = Instant::now() + Duration::from_secs(10);
        let end = context.deadline.map_or(end, |caller| caller.min(end));
        grant.transferred = true;
        Ok(PreparedCarrier {
            pending,
            slot: grant.slot.clone(),
            egress: self.egress.clone(),
            generation: binding.network_generation,
            cancel: context.cancel,
            authorization: bound.as_ref().map(|bound| bound.cancellation()),
            end,
            endpoint_observer: None,
            business: self.business.clone(),
            service: self.service.clone(),
            session: context.session,
            transferred: false,
        })
    }
}
impl UpstreamOpener for NativeCarrierOpener {
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
