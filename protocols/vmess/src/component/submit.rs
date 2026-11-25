//! Shared ordinary and controlled-gRPC VMess submission/confirmation semantics.
use super::*;

pub(super) async fn submit(
    options: &Options,
    admission: &Arc<SessionAdmission>,
    request: ConnectRequest,
    context: OpenContext,
    session: Option<zero_core::component::SessionId>,
    mut io: zero_transport::TcpRelayStream,
) -> Result<Connected<BoxStream>, ChannelError> {
    let call = GuardedCall::from_admitted(admission, &context, session);
    let (bytes, state) = crate::shared::prepare_request(
        &request.destination.address,
        request.destination.port,
        &options.uuid,
        options.cipher,
        crate::shared::CMD_TCP,
    )
    .map_err(|source| {
        let mut error = call.error(Stage::Handshake, Reason::Protocol, DeliveryState::Unknown);
        if context.upstream.binding().prefix.is_empty() {
            error.delivery = DeliveryState::NotSubmitted;
            error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
        }
        error.with_source(source)
    })?;
    call.run_io(Stage::Handshake, DeliveryState::Unknown, async {
        io.write_all(&bytes).await?;
        io.flush().await
    })
    .await?;
    let facts = call.facts();
    facts.record(ConnectFact::CarrierReady);
    facts.record(ConnectFact::RequestSubmitted);
    facts.protocol_traffic(0, bytes.len() as u64);
    let mut stream = crate::stream::VmessAeadStream::outbound(io, state)
        .map_err(|source| {
            call.error(Stage::Handshake, Reason::Protocol, DeliveryState::Unknown)
                .with_source(source)
        })?
        .with_connect_observer(session, context.observer);
    if options.grpc.is_some() {
        stream = stream.with_grpc_request_end();
    }
    Ok(Connected {
        io: Box::pin(stream),
        peer_confirmation: PeerConfirmation::Deferred,
    })
}
