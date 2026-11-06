//! The exact next Business owner adopts an independently controlled donor loan.
use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierGrant, CarrierIo};
impl PrefixOpener {
    pub(super) fn acquire_business(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<CarrierIo, ChannelError>> {
        Box::pin(async move {
            if binding != self.expected_binding
                || endpoint != self.expected_endpoint
                || call.session != self.context.session_identity()?
                || call.session.is_none()
                || binding.network_generation.0 != self.network.network_generation()
            {
                let mut failure = self.interrupted(&call, Reason::StalePlan);
                failure.delivery = DeliveryState::NotSubmitted;
                return Err(failure);
            }
            let business = self.business.as_ref().ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "selected relay has no actual next-owner Business ticket",
                )
            })?;
            let plan = self
                .plan
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .ok_or_else(|| invalid(Reason::Revoked, "selected relay original plan retired"))?;
            plan.authorize_carrier_prefix(grant.instance(), &binding.prefix)?;
            // This seals the same actual admitted caller on the receiver before
            // any donor acquisition can suspend. It is not Service authority.
            let ticket = business.authorize(&endpoint, &binding, &grant)?;
            let service = CarrierContext {
                session: None,
                deadline: call.deadline,
                cancel: call.cancel.clone(),
            };
            let loan = self
                .borrow_selected(binding.clone(), endpoint.clone(), service)
                .await?;
            let original = loan.owner.original().clone();
            // Install the authentic donor before post-acquisition checks. Error
            // or cancellation leaves the same actual receiver in its caller's
            // pending/bound cleanup census; no bare stream substitutes custody.
            let io = grant.adopt_business_stream_loan(ticket, &binding, endpoint, &call, loan)?;
            let donor = *binding
                .prefix
                .last()
                .ok_or_else(|| invalid(Reason::StalePlan, "Business relay lost its donor"))?;
            plan.validate_carrier_origin(donor, &original)?;
            zero_platform_tokio::controlled_carrier::CarrierBusinessDemand::check(business)?;
            if binding.network_generation.0 != self.network.network_generation()
                || call.cancel.is_cancelled()
            {
                return Err(self.interrupted(&call, Reason::StalePlan));
            }
            self.context.upstream.observe_carrier_origin(&original);
            Ok(io)
        })
    }
}
