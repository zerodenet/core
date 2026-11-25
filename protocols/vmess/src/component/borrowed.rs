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
        let call = GuardedCall::from_admitted(&self.admission, &context, session);
        let StreamLoan { io, owner } = call.borrow_stream(self.options.endpoint.clone()).await?;
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
