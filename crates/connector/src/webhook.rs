use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use http::{Method, Request, StatusCode, Uri};
use http_body_util::Full;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tower_service::Service;
use zero_api::{ApiError, ApiErrorCode, ApiResult, PublishResult, RawApiEvent};

use crate::network::{EventDispatcherNetwork, EventSinkTcpDialer, EventSinkTcpStream};
use crate::registry::AsyncDeliverySink;

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
        let client = Client::builder(TokioExecutor::new()).build(connector);

        Ok(Self {
            client,
            uri,
            headers,
            timeout: config.timeout,
        })
    }

    fn request(&self, event: &RawApiEvent) -> Result<Request<Full<Bytes>>, String> {
        let body = serde_json::to_vec(event)
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
    async fn publish(&self, event: RawApiEvent) -> ApiResult<PublishResult> {
        let request = match self.request(&event) {
            Ok(request) => request,
            Err(error) => return Ok(request_failure(error)),
        };

        match tokio::time::timeout(self.timeout, self.client.request(request)).await {
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
                self.timeout.as_millis()
            ))),
        }
    }

    fn supports_cancellation(&self) -> bool {
        true
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

fn parse_webhook_uri(raw_url: &str) -> ApiResult<Uri> {
    let uri = raw_url.parse::<Uri>().map_err(|error| ApiError {
        code: ApiErrorCode::InvalidArgument,
        message: "webhook url is invalid".to_owned(),
        field_path: Some("url".to_owned()),
        cause: Some(error.to_string()),
        details: Vec::new(),
    })?;

    match uri.scheme_str() {
        Some("http" | "https") if uri.host().is_some() => Ok(uri),
        Some(scheme) if scheme != "http" && scheme != "https" => Err(ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "webhook url scheme must be http or https".to_owned(),
            field_path: Some("url".to_owned()),
            cause: Some(format!("unsupported scheme `{scheme}`")),
            details: Vec::new(),
        }),
        _ => Err(ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "webhook url requires an http/https scheme and host".to_owned(),
            field_path: Some("url".to_owned()),
            cause: None,
            details: Vec::new(),
        }),
    }
}

fn parse_headers(headers: &BTreeMap<String, String>) -> ApiResult<HeaderMap> {
    let mut parsed = HeaderMap::new();
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "webhook header name is invalid".to_owned(),
            field_path: Some(format!("headers.{name}")),
            cause: Some(error.to_string()),
            details: Vec::new(),
        })?;
        let value = HeaderValue::from_str(value).map_err(|error| ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "webhook header value is invalid".to_owned(),
            field_path: Some(format!("headers.{name}")),
            cause: Some(error.to_string()),
            details: Vec::new(),
        })?;
        parsed.insert(name, value);
    }
    Ok(parsed)
}

fn build_tls_config(allow_insecure: bool) -> ApiResult<rustls::ClientConfig> {
    let provider = rustls::crypto::ring::default_provider();
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(tls_config_error)?;
    let mut config = builder.with_root_certificates(roots).with_no_client_auth();
    if allow_insecure {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(InsecureCertVerifier));
    }
    Ok(config)
}

#[derive(Debug)]
struct InsecureCertVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _certificate: &rustls::pki_types::CertificateDer<'_>,
        _signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _certificate: &rustls::pki_types::CertificateDer<'_>,
        _signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn default_port(uri: &Uri) -> Option<u16> {
    match uri.scheme_str() {
        Some("http") => Some(80),
        Some("https") => Some(443),
        _ => None,
    }
}

fn invalid_uri(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn request_failure(message: impl Into<String>) -> PublishResult {
    PublishResult {
        delivered: false,
        retryable: true,
        message: Some(message.into()),
    }
}

fn format_error_chain(prefix: &str, error: &dyn StdError) -> String {
    let mut message = format!("{prefix}: {error}");
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

fn tls_config_error(error: rustls::Error) -> ApiError {
    ApiError {
        code: ApiErrorCode::Internal,
        message: "failed to build webhook event sink TLS client".to_owned(),
        field_path: None,
        cause: Some(error.to_string()),
        details: Vec::new(),
    }
}
