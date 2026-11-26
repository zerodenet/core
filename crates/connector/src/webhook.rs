mod config;

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{HeaderMap, CONTENT_TYPE};
use http::{Method, Request, Uri};
use http_body_util::Full;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioIo;
use tower_service::Service;
use zero_api::{ApiResult, PublishResult, RawApiEvent};

use crate::network::{EventDispatcherNetwork, EventSinkTcpDialer, EventSinkTcpStream};
use crate::registry::AsyncDeliverySink;
use config::{
    build_tls_config, default_port, format_error_chain, invalid_uri, is_retryable_status,
    parse_headers, parse_webhook_uri, request_failure,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const JSON_CONTENT_TYPE: &str = "application/json";

type WebhookHttpClient = Client<HttpsConnector<EgressHttpConnector>, Full<Bytes>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WebhookEventSinkConfig {
    url: String,
    headers: BTreeMap<String, String>,
    timeout: Duration,
    allow_insecure: bool,
}

impl WebhookEventSinkConfig {
    pub(crate) fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: BTreeMap::new(),
            timeout: DEFAULT_TIMEOUT,
            allow_insecure: false,
        }
    }

    pub(crate) fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub(crate) fn with_allow_insecure(mut self, allow: bool) -> Self {
        self.allow_insecure = allow;
        self
    }
}

#[derive(Clone)]
pub(crate) struct WebhookEventSink {
    client: WebhookHttpClient,
    uri: Uri,
    headers: HeaderMap,
    timeout: Duration,
    dialer: Arc<dyn EventSinkTcpDialer>,
}

impl std::fmt::Debug for WebhookEventSink {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebhookEventSink")
            .field("uri", &self.uri)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl WebhookEventSink {
    pub(crate) fn with_config(
        config: WebhookEventSinkConfig,
        network: &EventDispatcherNetwork,
    ) -> ApiResult<Self> {
        let uri = parse_webhook_uri(&config.url)?;
        let headers = parse_headers(&config.headers)?;
        let tls = build_tls_config(config.allow_insecure)?;
        let connector = HttpsConnectorBuilder::new()
            .with_tls_config(tls)
            .https_or_http()
            .enable_http1()
            .wrap_connector(EgressHttpConnector::new(network.dialer()));
        let dialer = network.dialer();
        let client = Client::builder(EventExecutor(dialer.clone())).build(connector);

        Ok(Self {
            client,
            uri,
            headers,
            timeout: config.timeout,
            dialer,
        })
    }

    fn request(&self, event: &RawApiEvent) -> Result<Request<Full<Bytes>>, String> {
        let body = crate::encoding::to_vec(event)
            .map_err(|error| format!("serialize webhook event: {error}"))?;
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(self.uri.clone())
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .body(Full::new(Bytes::from(body)))
            .map_err(|error| format!("build webhook request: {error}"))?;
        request.headers_mut().extend(self.headers.clone());
        Ok(request)
    }
}

#[async_trait]
impl AsyncDeliverySink for WebhookEventSink {
    async fn publish(&self, event: Arc<RawApiEvent>) -> ApiResult<PublishResult> {
        let request = match self.request(&event) {
            Ok(request) => request,
            Err(error) => return Ok(request_failure(error)),
        };

        let deadline = tokio::time::Instant::now() + self.timeout;
        if let Err(error) = self.dialer.prepare_request(deadline).await {
            return Ok(request_failure(format!(
                "webhook network preparation: {error}"
            )));
        }
        let client = self.client.clone();
        let timeout = self.timeout;
        let (mut result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task = Box::pin(async move {
            let request = async move {
                match tokio::time::timeout_at(deadline, client.request(request)).await {
                    Ok(Ok(response)) => {
                        let status = response.status();
                        if status.is_success() {
                            Ok(PublishResult::delivered())
                        } else {
                            Ok(PublishResult {
                                delivered: false,
                                retryable: is_retryable_status(status),
                                message: Some(format!("webhook returned HTTP {}", status.as_u16())),
                            })
                        }
                    }
                    Ok(Err(error)) => Ok(request_failure(format_error_chain(
                        "webhook request failed",
                        &error,
                    ))),
                    Err(_) => Ok(request_failure(format!(
                        "webhook request failed: request timed out after {} ms",
                        timeout.as_millis()
                    ))),
                }
            };
            tokio::select! {
                biased;
                _ = result_tx.closed() => {},
                result = request => { let _ = result_tx.send(result); }
            }
        });
        if let Err(error) = self.dialer.spawn_event_task(task) {
            return Ok(request_failure(format!("webhook task admission: {error}")));
        }
        tokio::select! {
            result = result_rx => result.unwrap_or_else(|_| Ok(request_failure("webhook owned request stopped"))),
            _ = self.dialer.drive_event_tasks() => Ok(request_failure("webhook task supervisor stopped")),
        }
    }

    fn supports_cancellation(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct EventExecutor(Arc<dyn EventSinkTcpDialer>);
impl<F> hyper::rt::Executor<F> for EventExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        if let Err(error) = self.0.spawn_event_task(Box::pin(async move {
            let _ = future.await;
        })) {
            tracing::warn!(%error, "webhook HTTP driver admission rejected");
        }
    }
}

#[derive(Clone)]
struct EgressHttpConnector {
    dialer: Arc<dyn EventSinkTcpDialer>,
}

impl EgressHttpConnector {
    fn new(dialer: Arc<dyn EventSinkTcpDialer>) -> Self {
        Self { dialer }
    }
}

impl Service<Uri> for EgressHttpConnector {
    type Response = EventSinkConnection;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = io::Result<EventSinkConnection>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let dialer = self.dialer.clone();
        let host = match uri.host() {
            Some(host) => host.to_owned(),
            None => return Box::pin(async { Err(invalid_uri("webhook URI has no host")) }),
        };
        let port = match uri.port_u16().or_else(|| default_port(&uri)) {
            Some(port) => port,
            None => return Box::pin(async { Err(invalid_uri("webhook URI has no usable port")) }),
        };
        Box::pin(async move {
            let stream = dialer.connect(host, port).await?;
            Ok(EventSinkConnection::new(stream))
        })
    }
}

struct EventSinkConnection {
    inner: TokioIo<Box<dyn EventSinkTcpStream>>,
}

impl EventSinkConnection {
    fn new(stream: Box<dyn EventSinkTcpStream>) -> Self {
        Self {
            inner: TokioIo::new(stream),
        }
    }
}

impl Connection for EventSinkConnection {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

impl Read for EventSinkConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

impl Write for EventSinkConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(context, buffers)
    }
}
