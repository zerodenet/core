use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use http::{HttpConnectInbound, HttpInboundMode};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{InboundConnectionContext, TcpInboundListenerOperation};
use crate::runtime::tcp_ingress::InboundProtocol;
use crate::transport::{ClientStream, MeteredStream, TcpRelayStream};

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
    pub(crate) fn http_inbound(&self) -> HttpConnectInbound {
        self.http_inbound
    }

    pub(crate) fn for_mode(self, mode: HttpInboundMode) -> Self {
        Self {
            send_connect_response: mode == HttpInboundMode::Connect,
            ..self
        }
    }
}

pub(crate) fn replay_http_request(
    stream: TcpRelayStream,
    replay: Vec<u8>,
) -> TcpRelayStream {
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
                let mut metered = MeteredStream::new(TcpRelayStream::from(socket));
                match handler.http_inbound.accept_request(&mut metered).await {
                    Ok(request) => {
                        let (session, mode, replay) = request.into_parts();
                        if let Some((status, location)) = context.select_http_redirect(&session) {
                            handler
                                .http_inbound
                                .send_redirect_response(&mut metered, status, &location)
                                .await
                                .map_err(EngineError::from)
                        } else {
                            let client = replay_http_request(metered.into_inner(), replay);
                            context
                                .serve(session, client, handler.for_mode(mode))
                                .await
                        }
                    }
                    Err(error) => {
                        if handler
                            .http_inbound
                            .send_accept_error_response(&mut metered, &error)
                            .await
                            .unwrap_or(false)
                        {
                            Ok(())
                        } else {
                            Err(EngineError::from(error))
                        }
                    }
                }
            },
        }))
    }
}
