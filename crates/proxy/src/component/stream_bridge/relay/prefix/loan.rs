//! Real selected-role borrowing; no business lifetime escapes into a pool.
use super::*;
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::{
    contract::context::RequestIdentity, controlled_carrier::loan::StreamLoan,
};

impl PrefixOpener {
    pub(super) fn borrow_selected(
        &self,
        binding: UpstreamBinding,
        destination: Destination,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<StreamLoan, ChannelError>> {
        self.borrow_selected_observed(binding, destination, carrier, None)
    }
    pub(super) fn borrow_selected_observed(
        &self,
        binding: UpstreamBinding,
        destination: Destination,
        carrier: CarrierContext,
        waiting: Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<StreamLoan, ChannelError>> {
        Box::pin(async move {
            let shorter = self.context.upstream.binding();
            if carrier.session.is_some()
                || binding != self.expected_binding
                || destination != self.expected_endpoint
                || binding.prefix.len() != shorter.prefix.len() + 1
                || binding.prefix[..shorter.prefix.len()] != shorter.prefix[..]
                || binding.network_generation.0 != self.network.network_generation()
            {
                let mut error = self.interrupted(&carrier, Reason::StalePlan);
                error.delivery = DeliveryState::NotSubmitted;
                return Err(error);
            }
            if carrier.cancel.is_cancelled() {
                return Err(self.interrupted(&carrier, Reason::Cancelled));
            }
            // The existing selected role remains fenced by the real host.
            // Background borrowing uses its own caller bound/token, without
            // retaining a business SessionId, cancellation or request facts.
            let upstream = if waiting.is_some() || self.context.session_identity()?.is_some() {
                self.select_shorter(
                    &carrier,
                    waiting.or_else(|| self.context.upstream.observed_pending_carrier()),
                )
                .await?
            } else {
                self.context.upstream.clone()
            };
            let context = OpenContext {
                identity: self.service_identity.clone().unwrap_or_else(|| {
                    RequestIdentity::Service(
                        Arc::new(ServiceRequestFacts {
                            origin: ServiceOrigin::Runtime,
                            purpose: ServicePurpose::BackgroundControl,
                        })
                        .into(),
                    )
                }),
                deadline: carrier.deadline,
                cancel: carrier.cancel.clone(),
                upstream,
                observer: if self.context.session_identity()?.is_none() {
                    self.context.observer.clone()
                } else {
                    None
                },
            };
            let operation = self.role.connect_carrier(
                ConnectRequest {
                    destination: self.request_destination.clone(),
                },
                context,
            );
            // Once a loan was returned, retain its original control even when
            // generation/cancellation wins. The caller's persistent owner must
            // accept and settle it; never drop it as a successful empty retry.
            let attempt = tokio::select! { biased;
                _=carrier.cancel.cancelled()=>return Err(self.interrupted(&carrier,Reason::Cancelled)),
                result=with_deadline(carrier.deadline,operation)=>result.map_err(|source|self.interrupted(&carrier,Reason::Timeout).with_source(source))?,
            };
            if let Some(health) = &self.health {
                health.observe(&attempt);
            }
            let mut loan = attempt?.io;
            if let Some(observer) = &self.observer {
                let observer = observer.clone();
                loan.io.wrap(|io| {
                    Box::pin(zero_transport::observed::ObservedStream::new(io, observer))
                });
            }
            if self.network.network_generation() != binding.network_generation.0
                || carrier.cancel.is_cancelled()
            {
                loan.owner
                    .cancel(zero_platform_tokio::contract::CloseReason::Shutdown);
            }
            Ok(loan)
        })
    }
}
