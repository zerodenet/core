//! Install the real logical controller before any post-acquisition rejection.
use super::*;
use crate::{
    contract::{
        network::{CarrierContext, UpstreamBinding},
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{error, CarrierGrant, CarrierIo},
};
use zero_core::request::Destination;

impl CarrierGrant {
    pub fn adopt_stream_loan(
        self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: StreamLoan,
    ) -> Result<CarrierIo, ChannelError> {
        self.adopt_stream_loan_inner(binding, endpoint, context, loan, None)
    }
    pub fn adopt_dependency_stream_loan(
        self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: StreamLoan,
        issuer: super::dependency::CarrierDependencyLoanIssuer,
    ) -> Result<CarrierIo, ChannelError> {
        self.adopt_stream_loan_inner(binding, endpoint, context, loan, Some(issuer))
    }
    fn adopt_stream_loan_inner(
        mut self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: StreamLoan,
        dependency: Option<super::dependency::CarrierDependencyLoanIssuer>,
    ) -> Result<CarrierIo, ChannelError> {
        let original = loan.owner.original().clone();
        // The reservation is unique. Persist the acquired controller first;
        // even concurrent revocation must not discard an unconfirmed loan.
        {
            let mut state = self.slot.state();
            assert!(
                state.control.is_none() && state.packet_owner.is_none() && state.loan.is_none(),
                "sole unused carrier grant"
            );
            state.loan = Some(super::receiver::install_owner(&self.slot, loan.owner));
            state.loan_origin = Some(original.clone());
            state.binding = Some(binding.clone());
            state.endpoint = Some(endpoint.clone());
            state.view_live = true;
        }
        crate::controlled_carrier::users::acquired(&self.slot);
        self.transferred = true;
        let io = CarrierIo {
            io: Some(Box::pin(loan.io)),
            slot: self.slot.clone(),
        };
        let bound = self
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone());
        let path_permitted = match dependency {
            Some(issuer) => {
                binding.prefix.is_empty() && issuer.permits(&self, binding, &endpoint, &original)
            }
            None => !binding.prefix.is_empty(),
        };
        if !path_permitted
            || context.session.is_some()
            || context.cancel.is_cancelled()
            || context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            || self.slot.cancel.is_cancelled()
            || original.is_retired()
            || bound.is_none_or(|bound| {
                bound.pool_kind() != Some(false)
                    || !bound.permits_native(binding, &endpoint, context)
            })
        {
            return Err(error(
                self.instance(),
                Stage::Dial,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        Ok(io)
    }
}
