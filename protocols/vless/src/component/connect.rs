//! The same VLESS request, deferred response and accounting for every native carrier.
use super::Options;
use tokio::io::AsyncWriteExt;
use zero_core::component::SessionId;
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        context::{with_deadline, ConnectFact, OpenContext},
        stream::{BoxStream, Connected, PeerConfirmation},
        ChannelError, DeliveryState, Reason, Stage,
    },
    TcpRelayStream,
};
pub(super) async fn submit(
    options: &Options,
    admission: &SessionAdmission,
    session: Option<SessionId>,
    context: OpenContext,
    mut io: TcpRelayStream,
    bytes: Vec<u8>,
) -> Result<Connected<BoxStream>, ChannelError> {
    let control = zero_traits::AsyncSocket::transport_bypass_control(&io);
    if crate::flow::is_vision_flow(options.flow) && control.is_none() {
        let mut error = admission.error(
            session,
            Stage::Handshake,
            Reason::Unsupported,
            DeliveryState::Unknown,
        );
        if context.upstream.binding().prefix.is_empty() {
            error.delivery = DeliveryState::NotSubmitted;
            error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
        }
        return Err(error.with_source(zero_core::Error::Unsupported(
            "Vision requires TLS 1.3, REALITY or VLESS Encryption",
        )));
    }
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
    // The response is validated when the caller reads, so peers that wait
    // for application data do not deadlock against an eager response read.
    let stream = crate::deferred_response::DeferredVlessResponseStream::new(io)
        .with_connect_observer(session, context.observer);
    let io: BoxStream = if crate::flow::is_vision_flow(options.flow) {
        Box::pin(crate::vision::VisionStream::with_testseed(
            stream,
            options.uuid,
            control,
            options.testseed,
        ))
    } else {
        Box::pin(stream)
    };
    Ok(Connected {
        io,
        peer_confirmation: PeerConfirmation::Deferred,
    })
}
