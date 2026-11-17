//! Transform a real preceding standard loan without replacing its custody.
use super::*;
use zero_platform_tokio::controlled_carrier::loan::StreamLoan;
impl Outbound {
    pub(super) async fn borrow_prefix(
        &self,
        request: ConnectRequest,
        context: OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<Connected<StreamLoan>, ChannelError> {
        let call = CarrierContext {
            session,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        let opened = context
            .upstream
            .borrow_stream(self.options.endpoint.clone(), call);
        let StreamLoan { io, owner } = tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session, Stage::Dial, Reason::Revoked, DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session, Stage::Dial, Reason::Cancelled, DeliveryState::Unknown)),
            result = with_deadline(context.deadline, opened) => result.map_err(|source| self.admission.error(session, Stage::Dial, Reason::Timeout, DeliveryState::Unknown).with_source(source))?,
        }.map_err(|error| self.admission.attach(error, session))?;
        let mut peer_confirmation = PeerConfirmation::Deferred;
        let confirmation = &mut peer_confirmation;
        let io = io
            .try_map(|io| async move {
                let io = self.wrap_transport(io, &context, session).await?;
                let connected = submit::submit(
                    &self.options,
                    &self.admission,
                    request,
                    context,
                    session,
                    io,
                )
                .await?;
                *confirmation = connected.peer_confirmation;
                Ok::<_, ChannelError>(connected.io)
            })
            .await?;
        Ok(Connected {
            io: StreamLoan { io, owner },
            peer_confirmation,
        })
    }
}
