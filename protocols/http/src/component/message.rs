//! HTTP owns one real message, its body framing and connection continuation.
use super::connection::Connection;
use crate::{HttpConnectInbound, HttpForwardCommand};
use alloc::{vec, vec::Vec};
use std::{fmt, io};
use tokio::sync::oneshot;
use zero_core::request::RequestFacts;
use zero_platform_tokio::{
    contract::{
        inbound::{MessageAction, MessageContext, MessageReply, MessageTransfer},
        ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    rate_limit::SharedRateLimiter,
    TcpRelayStream,
};
const REQUEST_COALESCE_LIMIT: u64 = 4 * 1024;

pub(super) struct ForwardReply {
    client: Connection,
    request: HttpForwardCommand,
    facts: RequestFacts,
    continuation: Option<oneshot::Sender<Option<Connection>>>,
}
impl ForwardReply {
    pub(super) fn new(
        client: Connection,
        request: HttpForwardCommand,
        facts: RequestFacts,
        continuation: oneshot::Sender<Option<Connection>>,
    ) -> Result<Self, ChannelError> {
        if facts.target.as_ref() != Some(request.destination())
            || facts.protocol != crate::HttpConnectInbound.protocol()
        {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::InvalidConfig,
                Impact::Session,
            ));
        }
        Ok(Self {
            client,
            request,
            facts,
            continuation: Some(continuation),
        })
    }
}
impl MessageReply for ForwardReply {
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(mut self, action: MessageAction) -> Result<MessageTransfer, ChannelError> {
        match action {
            MessageAction::Blocked => {
                HttpConnectInbound
                    .send_blocked_response(&mut self.client)
                    .await
                    .map_err(WireError::from)
                    .map_err(|source| source.channel(self.facts.session))?;
            }
            MessageAction::UpstreamFailed => {
                HttpConnectInbound
                    .send_upstream_failure_response(&mut self.client)
                    .await
                    .map_err(WireError::from)
                    .map_err(|source| source.channel(self.facts.session))?;
            }
            MessageAction::Redirect { status, location } => {
                HttpConnectInbound
                    .send_redirect_response(&mut self.client, status, &location)
                    .await
                    .map_err(WireError::from)
                    .map_err(|source| source.channel(self.facts.session))?;
            }
            MessageAction::Transfer {
                upstream,
                effective_target,
                context,
            } => {
                if self.request.destination() != &effective_target {
                    self.request
                        .set_effective_authority(&effective_target.address, effective_target.port);
                }
                match transfer(&mut self.client, upstream, &self.request, &context)
                    .await
                    .map_err(|source| source.channel(self.facts.session))?
                {
                    TransferOutcome::Continue => {
                        let _ = self
                            .continuation
                            .take()
                            .expect("one message continuation")
                            .send(Some(self.client));
                    }
                    TransferOutcome::Close => {}
                    TransferOutcome::Upgraded(upstream) => {
                        return Ok(MessageTransfer::Upgraded {
                            inbound: self.client.into_relay(),
                            upstream: Box::new(upstream),
                        })
                    }
                }
            }
        }
        Ok(MessageTransfer::Complete)
    }
}
enum TransferOutcome {
    Continue,
    Close,
    Upgraded(TcpRelayStream),
}
async fn transfer(
    client: &mut Connection,
    mut upstream: TcpRelayStream,
    request: &HttpForwardCommand,
    context: &MessageContext,
) -> Result<TransferOutcome, WireError> {
    if request.expect_continue() {
        throttle_and_write(&mut upstream, request.head(), context.upload_limiter()).await?;
        context.record_upload(request.head().len() as u64);
        crate::HttpConnectInbound
            .send_continue_response(client)
            .await?;
    }

    let coalesced_body_length = match request.body() {
        crate::HttpBodyKind::ContentLength(length)
            if !request.expect_continue() && length <= REQUEST_COALESCE_LIMIT =>
        {
            Some(length)
        }
        _ => None,
    };
    let request_body = if let Some(length) = coalesced_body_length {
        let mut throttled = RateLimitedSocket::new(client, context.upload_limiter(), &context);
        let body = read_fixed_body(&mut throttled, length).await?;
        if let Some(limiter) = context.upload_limiter() {
            limiter.throttle(request.head().len()).await;
        }
        let mut message = Vec::with_capacity(request.head().len() + body.len());
        message.extend_from_slice(request.head());
        message.extend_from_slice(&body);
        zero_traits::AsyncSocket::write_all(&mut upstream, &message).await?;
        context.record_upload(request.head().len() as u64);
        crate::HttpTransferCount {
            read: body.len() as u64,
            written: body.len() as u64,
        }
    } else {
        if !request.expect_continue() {
            throttle_and_write(&mut upstream, request.head(), context.upload_limiter()).await?;
            context.record_upload(request.head().len() as u64);
        }
        let mut throttled = RateLimitedSocket::new(client, context.upload_limiter(), &context);
        crate::relay_http_body(&mut throttled, &mut upstream, request.body()).await?
    };
    context.record_upload_io(request_body.read, request_body.written);

    loop {
        let response = {
            let mut active_upstream = RateLimitedSocket::new(&mut upstream, None, &context);
            crate::HttpConnectInbound
                .accept_response(
                    &mut active_upstream,
                    request.method(),
                    request.upgrade_requested(),
                    request.close_after_response(),
                    request.supports_chunked_response(),
                )
                .await?
        };
        throttle_and_write(client, response.head(), context.download_limiter()).await?;
        context.record_download(response.head().len() as u64);

        if response.upgrade_accepted() {
            return Ok(TransferOutcome::Upgraded(upstream));
        }

        let response_body = {
            let mut throttled =
                RateLimitedSocket::new(&mut upstream, context.download_limiter(), &context);
            if response.chunk_close_delimited() {
                crate::relay_close_delimited_as_chunked(&mut throttled, client).await?
            } else {
                crate::relay_http_body(&mut throttled, client, response.body()).await?
            }
        };
        context.record_download_io(response_body.read, response_body.written);
        if !response.informational() {
            return Ok(if response.close_after_response() {
                TransferOutcome::Close
            } else {
                TransferOutcome::Continue
            });
        }
    }
}

async fn read_fixed_body<S>(stream: &mut S, length: u64) -> Result<Vec<u8>, WireError>
where
    S: zero_traits::AsyncSocket<Error = io::Error>,
{
    let length = usize::try_from(length)
        .map_err(|_| WireError::from(zero_core::Error::Protocol("HTTP body is too large")))?;
    let mut body = vec![0_u8; length];
    let mut offset = 0;
    while offset < body.len() {
        let read = zero_traits::AsyncSocket::read(stream, &mut body[offset..]).await?;
        if read == 0 {
            return Err(WireError::from(zero_core::Error::Protocol(
                "HTTP body ended before Content-Length",
            )));
        }
        offset += read;
    }
    Ok(body)
}

async fn throttle_and_write<S>(
    stream: &mut S,
    bytes: &[u8],
    limiter: Option<SharedRateLimiter>,
) -> Result<(), WireError>
where
    S: zero_traits::AsyncSocket<Error = io::Error>,
{
    if let Some(limiter) = limiter {
        limiter.throttle(bytes.len()).await;
    }
    zero_traits::AsyncSocket::write_all(stream, bytes).await?;
    Ok(())
}

struct RateLimitedSocket<'a, S> {
    inner: &'a mut S,
    limiter: Option<SharedRateLimiter>,
    context: &'a MessageContext,
}

impl<'a, S> RateLimitedSocket<'a, S> {
    fn new(
        inner: &'a mut S,
        limiter: Option<SharedRateLimiter>,
        context: &'a MessageContext,
    ) -> Self {
        Self {
            inner,
            limiter,
            context,
        }
    }
}

impl<S> zero_traits::AsyncSocket for RateLimitedSocket<'_, S>
where
    S: zero_traits::AsyncSocket<Error = io::Error>,
{
    type Error = io::Error;

    async fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Self::Error> {
        let read = zero_traits::AsyncSocket::read(self.inner, buffer).await?;
        if let Some(limiter) = self.limiter.as_ref() {
            limiter.throttle(read).await;
        }
        if read != 0 {
            self.context.record_activity();
        }
        Ok(read)
    }

    async fn write_all(&mut self, buffer: &[u8]) -> Result<(), Self::Error> {
        zero_traits::AsyncSocket::write_all(self.inner, buffer).await?;
        if !buffer.is_empty() {
            self.context.record_activity();
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        zero_traits::AsyncSocket::shutdown(self.inner).await
    }
}

#[derive(Debug)]
enum WireError {
    Io(io::Error),
    Protocol(zero_core::Error),
}
impl From<io::Error> for WireError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<zero_core::Error> for WireError {
    fn from(error: zero_core::Error) -> Self {
        Self::Protocol(error)
    }
}
impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Protocol(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for WireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
        }
    }
}
impl WireError {
    fn channel(self, session: zero_core::component::SessionId) -> ChannelError {
        let mut error =
            ChannelError::new(Stage::Send, Reason::Io, Impact::Session).with_source(self);
        error.session = Some(session);
        error.delivery = DeliveryState::Unknown;
        error
    }
}
