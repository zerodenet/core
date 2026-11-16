//! The same VLESS request, deferred response and accounting for every native carrier.
use super::Options;
use std::sync::Arc;
mod service;
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
    admission: &Arc<SessionAdmission>,
    session: Option<SessionId>,
    context: OpenContext,
    io: TcpRelayStream,
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
    let framing = Framing {
        uuid: options.uuid,
        flow: options.flow,
        testseed: options.testseed,
    };
    if super::request::dialed_ingress(&context) {
        let admission = admission.clone();
        // Return the real loan/controller before its Rvs handshake can suspend.
        // The same IO performs authentication when its retained caller polls it.
        let prepare = async move {
            submit_actual(framing, &admission, session, context, io, bytes)
                .await
                .map(|connected| connected.io)
        };
        return Ok(Connected {
            io: Box::pin(service::PreparingStream::new(prepare)),
            peer_confirmation: PeerConfirmation::Deferred,
        });
    }
    submit_actual(framing, admission, session, context, io, bytes).await
}
#[derive(Clone, Copy)]
struct Framing {
    uuid: [u8; 16],
    flow: Option<&'static str>,
    testseed: [u32; 4],
}
async fn submit_actual(
    options: Framing,
    admission: &SessionAdmission,
    session: Option<SessionId>,
    context: OpenContext,
    mut io: TcpRelayStream,
    bytes: Vec<u8>,
) -> Result<Connected<BoxStream>, ChannelError> {
    admission.check(&context)?;
    if context.deadline_elapsed() {
        return Err(admission.error(
            session,
            Stage::Handshake,
            Reason::Timeout,
            DeliveryState::Unknown,
        ));
    }
    let control = zero_traits::AsyncSocket::transport_bypass_control(&io);
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
    // Rvs authentication is complete before publishing its request source.
    // Other TCP peers can wait for application data, so retain deferred reads.
    let reverse = super::request::dialed_ingress(&context);
    if reverse {
        let read = crate::shared::read_response_len(&mut io);
        let read_bytes = tokio::select! { biased;
            _ = admission.cancelled() => return Err(admission.error(session,Stage::Handshake,Reason::Revoked,DeliveryState::Unknown)),
            _ = context.cancel.cancelled() => return Err(admission.error(session,Stage::Handshake,Reason::Cancelled,DeliveryState::Unknown)),
            result = with_deadline(context.deadline,read) => result.map_err(|source| admission.error(session,Stage::Handshake,Reason::Timeout,DeliveryState::Unknown).with_source(source))?,
        }.map_err(|source| {
            if let Some(observer) = &context.observer { observer.record(session, ConnectFact::PeerRejected); }
            admission.error(session,Stage::Handshake,Reason::Io,DeliveryState::Unknown).with_source(source)
        })?;
        if let Some(observer) = &context.observer {
            observer.record(session, ConnectFact::PeerConfirmed);
            observer.protocol_traffic(session, read_bytes as u64, bytes.len() as u64);
        }
        let io: BoxStream = if crate::flow::is_vision_flow(options.flow) {
            Box::pin(crate::vision::VisionStream::with_testseed(
                io,
                options.uuid,
                control,
                options.testseed,
            ))
        } else {
            Box::pin(io)
        };
        return Ok(Connected {
            io,
            peer_confirmation: PeerConfirmation::Confirmed,
        });
    }
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
