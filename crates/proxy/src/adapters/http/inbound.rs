use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use http::{HttpConnectInbound, HttpForwardRequest, HttpInboundMode};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{InboundConnectionContext, TcpInboundListenerOperation};
use crate::runtime::tcp_ingress::{
    InboundProtocol, MessageInboundProtocol, MessageRelayContext, MessageRelayOutcome,
};
use crate::transport::{ClientStream, TcpRelayStream};

// Keep small fixed request bodies behind the normalized head until the complete
// prefix can be written upstream. Some servers answer without consuming the
// body and may otherwise close while the proxy is still fetching a tiny tail.
const REQUEST_COALESCE_LIMIT: u64 = 4 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct HttpConnectInboundHandler {
    http_inbound: HttpConnectInbound,
    send_connect_response: bool,
}

impl Default for HttpConnectInboundHandler {
    fn default() -> Self {
        Self {
            http_inbound: HttpConnectInbound,
            send_connect_response: true,
        }
    }
}

impl HttpConnectInboundHandler {
    pub(crate) fn for_mode(self, mode: HttpInboundMode) -> Self {
        Self {
            send_connect_response: mode == HttpInboundMode::Connect,
            ..self
        }
    }
}

pub(crate) fn replay_http_request(stream: TcpRelayStream, replay: Vec<u8>) -> TcpRelayStream {
    if replay.is_empty() {
        return stream;
    }

    let local_addr = stream.local_addr().ok();
    let stream = HttpRequestReplayStream::new(stream, replay);
    match local_addr {
        Some(addr) => TcpRelayStream::with_local_addr(stream, addr),
        None => TcpRelayStream::new(stream),
    }
}

pub(crate) async fn handle_http_connection(
    mut client: TcpRelayStream,
    handler: HttpConnectInboundHandler,
    context: InboundConnectionContext,
) -> Result<(), EngineError> {
    loop {
        let request = match handler.http_inbound.accept_next_request(&mut client).await {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => {
                if handler
                    .http_inbound
                    .send_accept_error_response(&mut client, &error)
                    .await
                    .unwrap_or(false)
                {
                    return Ok(());
                }
                return Err(EngineError::from(error));
            }
        };

        match request.mode() {
            HttpInboundMode::Connect => {
                let (session, mode, replay) = request.into_parts();
                let client = replay_http_request(client, replay);
                return context.serve(session, client, handler.for_mode(mode)).await;
            }
            HttpInboundMode::Forward => {
                if let Some((status, location)) = context.select_http_redirect(request.session()) {
                    handler
                        .http_inbound
                        .send_redirect_response(&mut client, status, &location)
                        .await
                        .map_err(EngineError::from)?;
                    return Ok(());
                }
                let forward = request
                    .into_forward()
                    .expect("forward-mode HTTP request must carry forward metadata");
                match context
                    .serve_message(&handler, &mut client, forward)
                    .await?
                {
                    MessageRelayOutcome::Continue => {}
                    MessageRelayOutcome::Close | MessageRelayOutcome::Upgraded => return Ok(()),
                }
            }
        }
    }
}

#[async_trait]
impl MessageInboundProtocol for HttpConnectInboundHandler {
    type Request = HttpForwardRequest;

    fn session<'a>(&self, request: &'a Self::Request) -> &'a zero_core::Session {
        request.session()
    }

    fn set_effective_target(
        &self,
        request: &mut Self::Request,
        target: &zero_core::Address,
        port: u16,
    ) {
        request.set_effective_authority(target, port);
    }

    async fn send_blocked(&self, client: &mut TcpRelayStream) -> Result<(), EngineError> {
        self.http_inbound
            .send_blocked_response(client)
            .await
            .map_err(EngineError::from)
    }

    async fn send_upstream_failure(&self, client: &mut TcpRelayStream) -> Result<(), EngineError> {
        self.http_inbound
            .send_upstream_failure_response(client)
            .await
            .map_err(EngineError::from)
    }

    async fn relay(
        &self,
        client: &mut TcpRelayStream,
        mut upstream: TcpRelayStream,
        request: &Self::Request,
        context: MessageRelayContext,
    ) -> Result<MessageRelayOutcome, EngineError> {
        tokio::time::timeout(context.idle_timeout(), async {
            if request.expect_continue() {
                throttle_and_write(&mut upstream, request.head(), context.upload_limiter()).await?;
                context.record_upload(request.head().len() as u64);
                self.http_inbound.send_continue_response(client).await?;
            }

            let coalesced_body_length = match request.body() {
                http::HttpBodyKind::ContentLength(length)
                    if !request.expect_continue() && length <= REQUEST_COALESCE_LIMIT =>
                {
                    Some(length)
                }
                _ => None,
            };
            let request_body = if let Some(length) = coalesced_body_length {
                let mut throttled = RateLimitedSocket::new(client, context.upload_limiter());
                let body = read_fixed_body(&mut throttled, length).await?;
                if let Some(limiter) = context.upload_limiter() {
                    limiter.throttle(request.head().len()).await;
                }
                let mut message = Vec::with_capacity(request.head().len() + body.len());
                message.extend_from_slice(request.head());
                message.extend_from_slice(&body);
                zero_traits::AsyncSocket::write_all(&mut upstream, &message).await?;
                context.record_upload(request.head().len() as u64);
                http::HttpTransferCount {
                    read: body.len() as u64,
                    written: body.len() as u64,
                }
            } else {
                if !request.expect_continue() {
                    throttle_and_write(&mut upstream, request.head(), context.upload_limiter())
                        .await?;
                    context.record_upload(request.head().len() as u64);
                }
                let mut throttled = RateLimitedSocket::new(client, context.upload_limiter());
                http::relay_http_body(&mut throttled, &mut upstream, request.body()).await?
            };
            context.record_upload_io(request_body.read, request_body.written);

            loop {
                let response = self
                    .http_inbound
                    .accept_response(
                        &mut upstream,
                        request.method(),
                        request.upgrade_requested(),
                        request.close_after_response(),
                        request.supports_chunked_response(),
                    )
                    .await?;
                throttle_and_write(client, response.head(), context.download_limiter()).await?;
                context.record_download(response.head().len() as u64);

                if response.upgrade_accepted() {
                    context.relay_bidirectional(client, upstream).await?;
                    return Ok(MessageRelayOutcome::Upgraded);
                }

                let response_body = {
                    let mut throttled =
                        RateLimitedSocket::new(&mut upstream, context.download_limiter());
                    if response.chunk_close_delimited() {
                        http::relay_close_delimited_as_chunked(&mut throttled, client).await?
                    } else {
                        http::relay_http_body(&mut throttled, client, response.body()).await?
                    }
                };
                context.record_download_io(response_body.read, response_body.written);
                if !response.informational() {
                    return Ok(if response.close_after_response() {
                        MessageRelayOutcome::Close
                    } else {
                        MessageRelayOutcome::Continue
                    });
                }
            }
        })
        .await
        .map_err(|_| {
            EngineError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP forward transaction idle timeout",
            ))
        })?
    }
}

async fn read_fixed_body<S>(stream: &mut S, length: u64) -> Result<Vec<u8>, EngineError>
where
    S: zero_traits::AsyncSocket<Error = io::Error>,
{
    let length = usize::try_from(length)
        .map_err(|_| EngineError::from(zero_core::Error::Protocol("HTTP body is too large")))?;
    let mut body = vec![0_u8; length];
    let mut offset = 0;
    while offset < body.len() {
        let read = zero_traits::AsyncSocket::read(stream, &mut body[offset..]).await?;
        if read == 0 {
            return Err(EngineError::from(zero_core::Error::Protocol(
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
    limiter: Option<crate::transport::SharedRateLimiter>,
) -> Result<(), EngineError>
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
    limiter: Option<crate::transport::SharedRateLimiter>,
}

impl<'a, S> RateLimitedSocket<'a, S> {
    fn new(inner: &'a mut S, limiter: Option<crate::transport::SharedRateLimiter>) -> Self {
        Self { inner, limiter }
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
        Ok(read)
    }

    async fn write_all(&mut self, buffer: &[u8]) -> Result<(), Self::Error> {
        zero_traits::AsyncSocket::write_all(self.inner, buffer).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        zero_traits::AsyncSocket::shutdown(self.inner).await
    }
}

struct HttpRequestReplayStream {
    inner: TcpRelayStream,
    replay: Vec<u8>,
    offset: usize,
}

impl HttpRequestReplayStream {
    fn new(inner: TcpRelayStream, replay: Vec<u8>) -> Self {
        Self {
            inner,
            replay,
            offset: 0,
        }
    }
}

impl AsyncRead for HttpRequestReplayStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.offset < self.replay.len() {
            let available = self.replay.len() - self.offset;
            let to_copy = available.min(buf.remaining());
            if to_copy > 0 {
                let start = self.offset;
                let end = start + to_copy;
                buf.put_slice(&self.replay[start..end]);
                self.offset = end;
            }
            return Poll::Ready(Ok(()));
        }

        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for HttpRequestReplayStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[async_trait]
impl InboundProtocol for HttpConnectInboundHandler {
    type ClientStream = TcpRelayStream;

    async fn send_ok(&self, client: &mut TcpRelayStream) -> Result<(), EngineError> {
        if self.send_connect_response {
            self.http_inbound
                .send_success_response(client)
                .await
                .map_err(EngineError::from)?;
        }
        Ok(())
    }

    async fn send_blocked(&self, client: &mut TcpRelayStream) -> Result<(), EngineError> {
        let _ = self.http_inbound.send_blocked_response(client).await;
        let _ = AsyncWriteExt::shutdown(client).await;
        Ok(())
    }

    async fn send_upstream_failure(&self, client: &mut TcpRelayStream) -> Result<(), EngineError> {
        let _ = self
            .http_inbound
            .send_upstream_failure_response(client)
            .await;
        let _ = AsyncWriteExt::shutdown(client).await;
        Ok(())
    }
}

impl crate::adapters::http::HttpConnectAdapter {
    pub(super) fn prepare_inbound_listener_impl(
        &self,
        _inbound: zero_config::InboundConfig,
    ) -> Result<
        Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation>,
        EngineError,
    > {
        Ok(Box::new(TcpInboundListenerOperation {
            protocol_name: "http",
            error_protocol_name: "http",
            request: HttpConnectInboundHandler::default(),
            dispatch: |handler: HttpConnectInboundHandler,
                       socket,
                       context: InboundConnectionContext| async move {
                handle_http_connection(TcpRelayStream::from(socket), handler, context).await
            },
        }))
    }
}
