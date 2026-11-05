//! Stateless wrapping preserves the unique underlying loan and its real origin.
use super::*;
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;
impl Outbound {
    pub(super) async fn borrow(
        &self,
        request: ConnectRequest,
        context: OpenContext,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        let session = self.admission.check(&context)?;
        let reject = |reason| {
            self.admission
                .error(session, Stage::Prepare, reason, DeliveryState::NotSubmitted)
        };
        if session.is_some() || context.upstream.binding().dial != self.options.dial {
            return Err(reject(Reason::InvalidConfig));
        }
        let carriers = self
            .carriers
            .as_ref()
            .ok_or_else(|| reject(Reason::Unsupported))?;
        let request =
            wire::build_connect_request(&request.destination.address, request.destination.port)
                .map_err(|source| reject(Reason::InvalidConfig).with_source(source))?;
        let call = CarrierContext {
            session: None,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let end = context
            .deadline
            .ok_or_else(|| reject(Reason::InvalidConfig))?;
        carriers.reap_returned(end).await?;
        let mut grant = carriers.reserve()?;
        if let Some(waiting) = context.upstream.observed_pending_carrier() {
            waiting.retain(grant.adoption())?;
        }
        let view = grant.claim_transferred_view()?;
        let io = if context.upstream.binding().prefix.is_empty() {
            let prepared = context.upstream.prepare_controlled_stream(
                self.options.endpoint.clone(),
                call,
                grant,
            )?;
            prepared.open(end).await?.into_parts().0
        } else {
            let loan = context
                .upstream
                .borrow_stream(self.options.endpoint.clone(), call.clone())
                .await?;
            // Install the real upstream logical controller in our persistent
            // original slot before authentication/CONNECT can suspend or fail.
            grant.adopt_stream_loan(
                context.upstream.binding(),
                self.options.endpoint.clone(),
                &call,
                loan,
            )?
        };
        let loan = io.into_stream_loan(view)?;
        context
            .upstream
            .observe_carrier_origin(loan.owner.original());
        if let Some(observer) = &context.observer {
            observer.record(None, ConnectFact::CarrierReady);
        }
        let StreamLoan { io, owner } = loan;
        let connected = self.handshake(io, request, &context, None).await?;
        Ok(Connected {
            io: StreamLoan {
                io: connected.io,
                owner,
            },
            peer_confirmation: connected.peer_confirmation,
        })
    }
}
