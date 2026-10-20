//! Shared ordinary and controlled-gRPC VMess submission/confirmation semantics.
use super::*;
pub(super) async fn submit(
    options: &Options,
    admission: &SessionAdmission,
    request: ConnectRequest,
    context: OpenContext,
    session: Option<zero_core::component::SessionId>,
    mut io: zero_transport::TcpRelayStream,
) -> Result<Connected<BoxStream>, ChannelError> {
    // AuthID timestamps and body keys belong to this actual submission,
    // after DNS/dial/TLS and carrier preparation have completed.
    let (bytes, state) = crate::shared::prepare_request(
        &request.destination.address,
        request.destination.port,
        &options.uuid,
        options.cipher,
        crate::shared::CMD_TCP,
    )
    .map_err(|source| {
        let mut error = admission.error(
            session,
            Stage::Handshake,
            Reason::Protocol,
            DeliveryState::Unknown,
        );
        if context.upstream.binding().prefix.is_empty() {
            error.delivery = DeliveryState::NotSubmitted;
            error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
        }
        error.with_source(source)
    })?;
    let submit = async {
        io.write_all(&bytes).await?;
        io.flush().await
    };
    tokio::select! { biased;
            _ = admission.cancelled() => return Err(admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,submit) => result.map_err(|source| admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source))?;
    if let Some(observer) = &context.observer {
        // WebSocket early data can defer the upgrade until write/flush.
        // Only successful flush proves that the complete carrier is ready.
        observer.record(session, ConnectFact::CarrierReady);
        observer.record(session, ConnectFact::RequestSubmitted);
        observer.protocol_traffic(session, 0, bytes.len() as u64);
    }
    // The encrypted response header is validated by the protocol stream on
    // its first read, after application bytes can trigger the peer reply.
    let stream = crate::stream::VmessAeadStream::outbound(io, state)
        .map_err(|source| {
            admission
                .error(
                    session,
                    Stage::Handshake,
                    Reason::Protocol,
                    DeliveryState::Unknown,
                )
                .with_source(source)
        })?
        .with_connect_observer(session, context.observer);
    Ok(Connected {
        io: Box::pin(stream),
        peer_confirmation: PeerConfirmation::Deferred,
    })
}
