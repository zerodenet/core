//! A selected Direct root lends from its one registered original owner.
mod candidate;
mod opener;
mod setup;
use super::super::{
    business_carrier::BusinessCarrierRequest,
    instance::{invalid, ComponentSlot},
};
use crate::protocol_registry::UpstreamConnectServices;
use std::sync::{Arc, Weak};
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{
            CarrierContext, ControlledStreamRequest, PendingCarrierObserver, UpstreamBinding,
            UpstreamOpener, UpstreamTransport,
        },
        stream::BoxStream,
        BoxFuture, ChannelError, Reason,
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};
type Evidence = (Option<std::net::SocketAddr>, std::net::SocketAddr);
pub(in crate::component) struct Caller {
    pub(in crate::component) business: Option<BusinessCarrierRequest>,
    pub(in crate::component) call: Option<CarrierContext>,
    pub(in crate::component) waiting: Option<PendingCarrierObserver>,
}
#[derive(Clone)]
pub(in crate::component) struct Root {
    slot: Weak<ComponentSlot>,
    plan: Weak<crate::component::catalog::ComponentPlan>,
    services: UpstreamConnectServices,
    endpoint: Destination,
    binding: UpstreamBinding,
    business: Option<BusinessCarrierRequest>,
    call: Option<CarrierContext>,
    waiting: Option<PendingCarrierObserver>,
    evidence: Arc<std::sync::Mutex<Option<Evidence>>>,
}
impl Root {
    pub(in crate::component) fn transport(
        slot: &Arc<ComponentSlot>,
        plan: Weak<crate::component::catalog::ComponentPlan>,
        services: UpstreamConnectServices,
        endpoint: Destination,
        binding: UpstreamBinding,
        caller: Caller,
    ) -> Result<(Arc<Self>, UpstreamTransport), ChannelError> {
        let Caller {
            business,
            call,
            waiting,
        } = caller;
        let root = Self {
            slot: Arc::downgrade(slot),
            plan,
            services,
            endpoint,
            binding,
            business,
            call,
            waiting,
            evidence: Arc::default(),
        };
        root.owner()?;
        let root = Arc::new(root);
        Ok((
            root.clone(),
            UpstreamTransport::new(root.binding.clone(), root),
        ))
    }
    pub(in crate::component) fn take_evidence(&self) -> Option<Evidence> {
        self.evidence.lock().unwrap().take()
    }
    fn owner(&self) -> Result<Arc<ComponentSlot>, ChannelError> {
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| invalid(Reason::Revoked, "actual Direct root owner retired"))?;
        if !self.binding.prefix.is_empty() || self.binding.dial != slot.dial
            || self.binding.network_generation.0 != self.services.network_generation()
            || !slot.recipe.spec().resources.iter().any(|resource| matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {..})) {
            return Err(invalid(Reason::StalePlan,"actual Direct root binding changed"));
        }
        slot.service_admission()
            .ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "Direct root has no actual host admission",
                )
            })?
            .check_current()?;
        Ok(slot)
    }
    async fn open_loan(
        &self,
        binding: UpstreamBinding,
        mut request: ControlledStreamRequest,
    ) -> Result<StreamLoan, ChannelError> {
        let slot = self.owner()?;
        let call = self.call.as_ref().ok_or_else(|| {
            invalid(
                Reason::StalePlan,
                "Direct root has not selected its real Service call",
            )
        })?;
        let same_endpoint = request.destination == self.endpoint
            || matches!(&request.destination.address,
            zero_core::Address::Domain(name) if zero_core::address::parse_ip_literal(name).is_some_and(|ip| match (&self.endpoint.address,ip) {
                (zero_core::Address::Ipv4(bytes),std::net::IpAddr::V4(ip))=>*bytes==ip.octets(),
                (zero_core::Address::Ipv6(bytes),std::net::IpAddr::V6(ip))=>*bytes==ip.octets(),_=>false,
            })) && request.destination.port == self.endpoint.port;
        if binding != self.binding
            || !same_endpoint
            || request.context.session.is_some()
            || request.context.deadline != call.deadline
            || request.context.cancel.is_cancelled()
            || call.cancel.is_cancelled()
        {
            return Err(invalid(
                Reason::StalePlan,
                "Direct Service request differs from its selected next hop",
            ));
        }
        // Use the original selected call's token, never an independently
        // supplied metadata token with an equal deadline.
        request.context = call.clone();
        slot.check_direct_background_owner(
            &binding.dial,
            &request.grants.registration(),
            &request.tasks.scope_identity(),
        )?;
        let resolver = self.services.native_node_dns();
        let dns_generation = resolver.generation();
        let addresses = match &self.endpoint.address {
            zero_core::Address::Ipv4(bytes) => vec![std::net::IpAddr::from(*bytes)],
            zero_core::Address::Ipv6(bytes) => vec![std::net::IpAddr::from(*bytes)],
            zero_core::Address::Domain(name) => {
                let answer = resolver
                    .resolve(
                        zero_platform_tokio::contract::dns::DnsRequest {
                            name: name.clone(),
                            role: zero_platform_tokio::contract::dns::DnsQueryRole::Node,
                            family: binding.dial.effective_family().map_err(|source| {
                                invalid(Reason::InvalidConfig, "Direct dial family")
                                    .with_source(source)
                            })?,
                        },
                        zero_platform_tokio::contract::dns::DnsQueryContext {
                            deadline: call.deadline,
                            cancel: call.cancel.clone(),
                            expected_generation: Some(dns_generation),
                        },
                    )
                    .await?;
                answer
                    .addresses
                    .into_iter()
                    .map(|ip| match ip {
                        zero_traits::IpAddress::V4(bytes) => std::net::IpAddr::from(bytes),
                        zero_traits::IpAddress::V6(bytes) => std::net::IpAddr::from(bytes),
                    })
                    .collect()
            }
        };
        let candidates = addresses
            .into_iter()
            .map(|ip| std::net::SocketAddr::new(ip, self.endpoint.port))
            .filter_map(|peer| binding.dial.normalize_peer(peer).ok())
            .collect();
        let opener = candidate::Candidates {
            root: self.clone(),
            request,
            resolver,
            dns_generation,
            gate: tokio::sync::Mutex::new(()),
        };
        crate::transport::open_service_candidates_using(
            candidates,
            &self.services.egress_control(),
            &binding.dial,
            call.clone(),
            &opener,
        )
        .await
    }
}
