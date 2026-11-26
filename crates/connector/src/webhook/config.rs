use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::io;
use std::sync::Arc;

use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::{StatusCode, Uri};
use zero_api::{ApiError, ApiErrorCode, ApiResult, PublishResult};

pub(super) fn parse_webhook_uri(raw_url: &str) -> ApiResult<Uri> {
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

pub(super) fn parse_headers(headers: &BTreeMap<String, String>) -> ApiResult<HeaderMap> {
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

pub(super) fn build_tls_config(allow_insecure: bool) -> ApiResult<rustls::ClientConfig> {
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

pub(super) fn default_port(uri: &Uri) -> Option<u16> {
    match uri.scheme_str() {
        Some("http") => Some(80),
        Some("https") => Some(443),
        _ => None,
    }
}

pub(super) fn invalid_uri(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

pub(super) fn request_failure(message: impl Into<String>) -> PublishResult {
    PublishResult {
        delivered: false,
        retryable: true,
        message: Some(message.into()),
    }
}

pub(super) fn format_error_chain(prefix: &str, error: &dyn StdError) -> String {
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
