//! One protocol-owned framing/submission path for ordinary and borrowed IO.
use super::*;
impl Outbound {
    pub(super) fn prepare_stream(
        &self,
        request: &ConnectRequest,
        context: &OpenContext,
    ) -> Result<(Option<zero_core::component::SessionId>, Vec<u8>), ChannelError> {
        let session = self.admission.check(context)?;
        if context.upstream.binding().dial != self.options.dial {
            return Err(self.admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let bytes = crate::shared::build_request_with_hash(
            &self.options.password,
            &request.destination.address,
            request.destination.port,
            crate::shared::CMD_TCP,
        )
        .map_err(|source| {
            self.admission
                .error(
                    session,
                    Stage::Prepare,
                    Reason::InvalidConfig,
                    DeliveryState::NotSubmitted,
                )
                .with_source(source)
        })?;
        Ok((session, bytes))
    }
    pub(super) async fn submit_stream(
        &self,
        mut io: zero_transport::TcpRelayStream,
        bytes: Vec<u8>,
        context: OpenContext,
        session: Option<zero_core::component::SessionId>,
    ) -> Result<Connected<BoxStream>, ChannelError> {
        let submit = async {
            io.write_all(&bytes).await?;
            io.flush().await
        };
        tokio::select! { biased;
            _ = self.admission.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(self.admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,submit) => result.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| self.admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
        if let Some(observer) = &context.observer {
            // WebSocket early data can defer the upgrade until write/flush.
            // Only successful flush proves that the complete carrier is ready.
            observer.record(session, ConnectFact::CarrierReady);
            observer.record(session, ConnectFact::RequestSubmitted);
            observer.protocol_traffic(session, 0, bytes.len() as u64);
        }
        // Trojan sends no successful CONNECT acknowledgement. write/flush is
        // local submission only; the first business bytes may expose rejection.
        Ok(Connected {
            io: Box::pin(io),
            peer_confirmation: PeerConfirmation::Deferred,
        })
    }
}
