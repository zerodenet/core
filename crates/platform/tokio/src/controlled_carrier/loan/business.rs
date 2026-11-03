//! Host-issued business custody of a real, independently owned logical stream.
use super::*;
use crate::{
    contract::{
        network::{CarrierContext, UpstreamBinding},
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{error, CarrierBusinessGrant, CarrierGrant, CarrierIo},
};
use zero_core::request::Destination;

impl CarrierGrant {
    pub fn adopt_business_stream_loan(
        mut self,
        ticket: CarrierBusinessGrant,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: StreamLoan,
    ) -> Result<CarrierIo, ChannelError> {
        // Persist custody before even a post-acquisition rejection. The host's
        // existing reservation retains this controller on cancellation/error.
        let original = loan.owner.original().clone();
        let endpoints = original.endpoints();
        {
            let mut state = self.slot.state();
            assert!(
                state.control.is_none() && state.packet_owner.is_none() && state.loan.is_none(),
                "sole unused carrier grant"
            );
            state.loan = Some(Arc::new(tokio::sync::Mutex::new(loan.owner)));
            state.loan_origin = Some(original.clone());
            state.binding = Some(binding.clone());
            state.endpoint = Some(endpoint.clone());
            state.endpoints = endpoints;
            state.view_live = true;
        }
        crate::controlled_carrier::users::acquired(&self.slot);
        self.transferred = true;
        let io = CarrierIo {
            io: Some(Box::pin(loan.io)),
            slot: self.slot.clone(),
        };
        ticket.check(&self, binding, &endpoint, context)?;
        ticket.bind_caller(&self.slot)?;
        let bound = self
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone());
        if !ticket.is_relay()
            || original.is_retired()
            || self.slot.cancel.is_cancelled()
            || context.cancel.is_cancelled()
            || context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            || bound.is_none_or(|bound| !ticket.binding().matches_authority(&bound))
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
