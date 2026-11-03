//! Business H2 ownership over a real retained Stream loan, never a naked stream.
use super::*;
use crate::{component::catalog::ComponentPlan, inventory::service::ServiceNode};
use std::sync::Weak;
use zero_platform_tokio::{
    contract::{
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        BoxFuture, ChannelError,
    },
    controlled_carrier::{CarrierGrant, CarrierIo},
};
pub(super) struct Prefix {
    upstream: UpstreamTransport,
    endpoint: Destination,
    plan: Weak<ComponentPlan>,
    role: zero_core::component::RoleInstanceId,
    pub(super) observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl Prefix {
    pub(super) async fn prepare(
        services: &TcpExecutionServices,
        nodes: &[ServiceNode],
        index: usize,
        binding: &UpstreamBinding,
        endpoint: &Destination,
        wire_endpoint: &Destination,
        end: tokio::time::Instant,
    ) -> Result<Self, ChannelError> {
        let (target, wire, tag) = match nodes.get(index + 1) {
            Some(next) => (
                next.endpoint.clone().unwrap(),
                next.wire_endpoint.clone().unwrap(),
                next.tag.as_deref().unwrap_or(""),
            ),
            None => (endpoint.clone(), wire_endpoint.clone(), ""),
        };
        let selected = UpstreamBinding {
            prefix: binding.prefix[..=index].to_vec(),
            dial: nodes
                .get(index + 1)
                .map_or(binding.dial.clone(), |next| next.dial.clone()),
            network_generation: binding.network_generation,
        };
        let upstream = crate::component::stream_bridge::relay::background::BackgroundPath {
            nodes: nodes[..=index].to_vec(),
            binding: selected,
            endpoint: target.clone(),
            wire_endpoint: wire,
            final_tag: tag,
            call: CarrierContext {
                session: None,
                deadline: Some(end),
                cancel: services.invocation.as_ref().unwrap().cancellation(),
            },
        }
        .select(services)
        .await?;
        let plan = services
            .components
            .as_ref()
            .map_err(|error| crate::component::instance::retained(error.clone()))?;
        Ok(Self {
            upstream,
            endpoint: target,
            plan: Arc::downgrade(plan),
            role: nodes[index].slot.role_id(),
            observer: None,
        })
    }
}
impl super::opener::Opener {
    pub(super) fn acquire_borrowed(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<CarrierIo, ChannelError>> {
        Box::pin(async move {
            if binding != self.binding
                || endpoint != self.endpoint
                || self.network.network_generation() != binding.network_generation.0
            {
                return Err(invalid(Reason::StalePlan, "borrowed relay binding changed"));
            }
            let prefix = self.borrowed.as_ref().expect("selected real donor");
            let ticket = self.business.authorize(&endpoint, &binding, &grant)?;
            let service = CarrierContext {
                session: None,
                deadline: Some(
                    call.deadline
                        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5)),
                ),
                cancel: call.cancel.clone(),
            };
            let attempt = prefix
                .upstream
                .borrow_stream(prefix.endpoint.clone(), service)
                .await;
            // The selected prefix already observes its actual first-hop result.
            self.observed
                .store(true, std::sync::atomic::Ordering::Release);
            let mut loan = attempt.map_err(|mut error| {
                error.retry = zero_platform_tokio::contract::RetryClass::Never;
                error.delivery = zero_platform_tokio::contract::DeliveryState::Unknown;
                error
            })?;
            let origin = loan.owner.original().clone();
            if let Some(observer) = &prefix.observer {
                let observer = observer.clone();
                loan.io.wrap(|io| {
                    Box::pin(zero_transport::observed::ObservedStream::new(io, observer))
                });
            }
            let io = grant.adopt_business_stream_loan(ticket, &binding, endpoint, &call, loan)?;
            // Custody is installed before any validation can reject the acquired
            // donor. Such rejection still leaves its original controller retained.
            prefix
                .plan
                .upgrade()
                .ok_or_else(|| invalid(Reason::Revoked, "original relay plan retired"))?
                .validate_carrier_origin(prefix.role, &origin)?;
            if self.network.network_generation() != binding.network_generation.0
                || call.cancel.is_cancelled()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "network changed while borrowing relay",
                ));
            }
            *self.remote.lock().unwrap() = origin.endpoints().map(|facts| facts.peer());
            io.prepare_stream(prepare::preparation(
                self.hops.clone(),
                self.identity.clone(),
                call,
                self.network.clone(),
            ))
            .await
        })
    }
}
