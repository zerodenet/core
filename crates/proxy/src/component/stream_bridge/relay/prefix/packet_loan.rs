//! Opt-in logical packet borrowing, separate from native UDP close authority.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use zero_core::{
    component::AssociationId,
    request::{DatagramTargets, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_platform_tokio::{
    contract::context::RequestIdentity, controlled_carrier::loan::DatagramLoan,
};
impl PrefixOpener {
    pub(super) fn borrow_selected_datagram(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<DatagramLoan, ChannelError>> {
        Box::pin(async move {
            self.borrow_datagram_request(
                binding,
                endpoint,
                self.request_destination.clone(),
                self.service_identity.clone().unwrap_or_else(|| {
                    RequestIdentity::Service(
                        Arc::new(ServiceRequestFacts {
                            origin: ServiceOrigin::Runtime,
                            purpose: ServicePurpose::BackgroundControl,
                        })
                        .into(),
                    )
                }),
                call,
            )
            .await
        })
    }
    pub(super) fn borrow_selected_target(
        &self,
        binding: UpstreamBinding,
        target: zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTarget,
    ) -> BoxFuture<
        '_,
        Result<
            zero_platform_tokio::controlled_carrier::loan::ServiceDatagramTargetLoan,
            ChannelError,
        >,
    > {
        Box::pin(async move {
            target.validate_borrow(
                &binding,
                &self.expected_endpoint,
                zero_core::component::NetworkGeneration(self.network.network_generation()),
            )?;
            let peer = target.peer();
            let numeric = Destination::new(
                match peer.ip() {
                    std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                    std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                },
                peer.port(),
            );
            let loan = self
                .borrow_datagram_request(
                    binding,
                    target.endpoint().clone(),
                    numeric,
                    target.identity().clone(),
                    target.call().clone(),
                )
                .await?;
            Ok(target.with_loan(loan))
        })
    }
    async fn borrow_datagram_request(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        request_destination: Destination,
        identity: RequestIdentity,
        call: CarrierContext,
    ) -> Result<DatagramLoan, ChannelError> {
        let shorter = self.context.upstream.binding();
        if call.session.is_some()
            || binding != self.expected_binding
            || endpoint != self.expected_endpoint
            || binding.prefix.len() != shorter.prefix.len() + 1
            || binding.prefix[..shorter.prefix.len()] != shorter.prefix[..]
            || binding.network_generation.0 != self.network.network_generation()
        {
            let mut error = self.interrupted(&call, Reason::StalePlan);
            error.delivery = DeliveryState::NotSubmitted;
            return Err(error);
        }
        let slot = self.datagram_slot.as_ref().ok_or_else(|| {
            invalid(
                Reason::Unsupported,
                "selected role has no controlled Datagram borrowing capability",
            )
        })?;
        let deadline = call
            .deadline
            .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
        let role = slot.datagram(deadline).await?;
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let mut id = NEXT.load(Ordering::Relaxed);
        let id = loop {
            let next = id.checked_add(1).ok_or_else(|| {
                invalid(
                    Reason::Capacity,
                    "background association identity exhausted",
                )
            })?;
            match NEXT.compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break id,
                Err(actual) => id = actual,
            }
        };
        let request = DatagramRequest {
            association: AssociationId(id),
            targets: DatagramTargets::Fixed(request_destination),
        };
        let context = OpenContext {
            identity,
            deadline: call.deadline,
            cancel: call.cancel.clone(),
            upstream: self.context.upstream.clone(),
            observer: None,
        };
        let attempt = tokio::select! {biased;_=call.cancel.cancelled()=>return Err(self.interrupted(&call,Reason::Cancelled)),result=with_deadline(call.deadline,role.open_carrier(request,context))=>result.map_err(|source|self.interrupted(&call,Reason::Timeout).with_source(source))?};
        if let Some(health) = &self.health {
            health.observe(&attempt);
        }
        let mut loan = attempt?;
        loan.io.set_observer(self.observer.clone());
        if call.cancel.is_cancelled()
            || binding.network_generation.0 != self.network.network_generation()
        {
            loan.owner
                .cancel(zero_platform_tokio::contract::CloseReason::Shutdown);
        }
        Ok(loan)
    }
}
