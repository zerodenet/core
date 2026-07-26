use std::collections::BTreeMap;
use std::time::Duration;

use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{StatusCode, Url};
use zero_api::{ApiError, ApiErrorCode, ApiResult, EventSink, PublishResult, RawApiEvent};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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

#[derive(Debug, Clone)]
pub(crate) struct WebhookEventSink {
    client: Client,
    url: Url,
    headers: HeaderMap,
}

impl WebhookEventSink {
    pub(crate) fn with_config(config: WebhookEventSinkConfig) -> ApiResult<Self> {
        let url = parse_webhook_url(&config.url)?;
        let headers = parse_headers(&config.headers)?;
        let mut builder = Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none());
        if config.allow_insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder.build().map_err(client_error)?;

        Ok(Self {
            client,
            url,
            headers,
        })
    }
}

impl EventSink for WebhookEventSink {
    fn name(&self) -> &str {
        "webhook"
    }

    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        let mut request = self.client.post(self.url.clone()).json(event);
        if !self.headers.is_empty() {
            request = request.headers(self.headers.clone());
        }

        match request.send() {
            Ok(response) => {
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
            Err(error) => Ok(PublishResult {
                delivered: false,
                retryable: true,
                message: Some(format!("webhook request failed: {error}")),
            }),
        }
    }
}

fn parse_webhook_url(raw_url: &str) -> ApiResult<Url> {
    let url = Url::parse(raw_url).map_err(|error| ApiError {
        code: ApiErrorCode::InvalidArgument,
        message: "webhook url is invalid".to_owned(),
        field_path: Some("url".to_owned()),
        cause: Some(error.to_string()),
        details: Vec::new(),
    })?;

    match url.scheme() {
        "http" | "https" => Ok(url),
        scheme => Err(ApiError {
            code: ApiErrorCode::InvalidArgument,
            message: "webhook url scheme must be http or https".to_owned(),
            field_path: Some("url".to_owned()),
            cause: Some(format!("unsupported scheme `{scheme}`")),
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

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn client_error(error: reqwest::Error) -> ApiError {
    ApiError {
        code: ApiErrorCode::Internal,
        message: "failed to build webhook event sink client".to_owned(),
        field_path: None,
        cause: Some(error.to_string()),
        details: Vec::new(),
    }
}
