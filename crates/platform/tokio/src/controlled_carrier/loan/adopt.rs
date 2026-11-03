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
        mut self,
        binding: &UpstreamBinding,
        endpoint: Destination,
        context: &CarrierContext,
        loan: StreamLoan,
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
            state.loan = Some(Arc::new(tokio::sync::Mutex::new(loan.owner)));
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
        if binding.prefix.is_empty()
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
