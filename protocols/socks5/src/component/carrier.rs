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
        if self.carriers.is_none() {
            return Err(reject(Reason::Unsupported));
        }
        let request =
            wire::build_connect_request(&request.destination.address, request.destination.port)
                .map_err(|source| reject(Reason::InvalidConfig).with_source(source))?;
        context
            .deadline
            .ok_or_else(|| reject(Reason::InvalidConfig))?;
        let (io, view, mut attempt) = self.acquire_original(&context, None, true).await?;
        let loan = io.into_stream_loan(view)?;
        context
            .upstream
            .observe_carrier_origin(loan.owner.original());
        if let Some(observer) = &context.observer {
            observer.record(None, ConnectFact::CarrierReady);
        }
        let StreamLoan { io, owner } = loan;
        let connected = self.handshake(io, request, &context, None).await?;
        attempt.publish();
        Ok(Connected {
            io: StreamLoan {
                io: connected.io,
                owner,
            },
            peer_confirmation: connected.peer_confirmation,
        })
    }
}
