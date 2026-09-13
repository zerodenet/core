//! XHTTP request semantics from the pinned Xray v26.3.27 carrier contract.
mod metadata;
mod padding;
mod payload;
use super::{body::Body, XhttpMode};
use http::Request;
use rand::Rng;
use std::io;
use zero_traits::{SplitHttpOptions, SplitHttpRange, SplitHttpTransportProfile};

#[derive(Clone)]
pub(in crate::split_http) struct StreamRequest(pub bool);

#[derive(Clone)]
pub(super) struct Profile {
    pub(super) host: String,
    pub(super) path: String,
    pub(super) query: String,
    pub(super) mode: XhttpMode,
    pub(super) check_host: bool,
    pub(super) options: SplitHttpOptions,
}
pub(super) fn sample(range: SplitHttpRange) -> u32 {
    rand::rng().random_range(range.from..=range.to.max(range.from))
}
impl Profile {
    pub(super) fn new(config: &(impl SplitHttpTransportProfile + ?Sized)) -> Self {
        let (base, query) = config.path().split_once('?').unwrap_or((config.path(), ""));
        let mut path = base.to_owned();
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        if !path.ends_with('/') {
            path.push('/');
        }
        let mut options = config.options();
        let defaults = SplitHttpOptions::default();
        if options.x_padding_bytes.to == 0 {
            options.x_padding_bytes = defaults.x_padding_bytes;
        }
        if options.sc_max_each_post_bytes.to == 0 {
            options.sc_max_each_post_bytes = defaults.sc_max_each_post_bytes;
        }
        if options.sc_min_posts_interval_ms.to == 0 {
            options.sc_min_posts_interval_ms = defaults.sc_min_posts_interval_ms;
        }
        if options.sc_stream_up_server_secs.to == 0 {
            options.sc_stream_up_server_secs = defaults.sc_stream_up_server_secs;
        }
        if options.sc_max_buffered_posts == 0 {
            options.sc_max_buffered_posts = 30;
        }
        if options.server_max_header_bytes == 0 {
            options.server_max_header_bytes = 8192;
        }
        if options.session_placement.is_empty() {
            options.session_placement = "path".into();
        }
        if options.seq_placement.is_empty() {
            options.seq_placement = "path".into();
        }
        if options.uplink_data_placement.is_empty() {
            options.uplink_data_placement = "auto".into();
        }
        if options.uplink_http_method.is_empty() {
            options.uplink_http_method = "POST".into();
        }
        options.uplink_http_method.make_ascii_uppercase();
        if options.uplink_data_key.is_empty() {
            options.uplink_data_key = if options.uplink_data_placement == "cookie" {
                "x_data"
            } else {
                "X-Data"
            }
            .into();
        }
        if options.uplink_chunk_size.to == 0 {
            options.uplink_chunk_size = match options.uplink_data_placement.as_str() {
                "header" => SplitHttpRange::new(3000, 4000),
                "cookie" => SplitHttpRange::new(2048, 3072),
                _ => options.sc_max_each_post_bytes,
            };
        }
        options.uplink_chunk_size.from = options.uplink_chunk_size.from.max(64);
        options.uplink_chunk_size.to = options.uplink_chunk_size.to.max(64);
        Self {
            host: config.host().unwrap_or("localhost").into(),
            path,
            query: query.into(),
            mode: XhttpMode::parse(config.mode()),
            check_host: config.host().is_some(),
            options,
        }
    }
    pub(super) fn request(
        &self,
        method: &str,
        session: &str,
        seq: Option<u64>,
        body: Body,
    ) -> io::Result<Request<Body>> {
        let path = format!(
            "{}{}{}",
            self.path,
            if self.query.is_empty() { "" } else { "?" },
            self.query
        );
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", &self.host)
            .body(body)
            .map_err(io::Error::other)?;
        request
            .extensions_mut()
            .insert(StreamRequest(seq.is_none()));
        for (k, v) in &self.options.headers {
            request.headers_mut().insert(
                http::HeaderName::from_bytes(k.as_bytes()).map_err(io::Error::other)?,
                v.parse().map_err(io::Error::other)?,
            );
        }
        crate::browser::apply_fetch_headers(request.headers_mut());
        self.pad_request(&mut request)?;
        self.apply_meta(&mut request, session, seq)?;
        if method != "GET" && seq.is_none() && !self.options.no_grpc_header {
            request.headers_mut().insert(
                "content-type",
                http::HeaderValue::from_static("application/grpc"),
            );
        }
        Ok(request)
    }
    pub(super) fn response_headers(
        &self,
        request: &Request<impl Sized>,
    ) -> io::Result<http::HeaderMap> {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "access-control-allow-origin",
            request
                .headers()
                .get("origin")
                .cloned()
                .unwrap_or_else(|| http::HeaderValue::from_static("*")),
        );
        let o = &self.options;
        if [
            &o.session_placement,
            &o.seq_placement,
            &o.x_padding_placement,
            &o.uplink_data_placement,
        ]
        .iter()
        .any(|p| p.as_str() == "cookie")
        {
            headers.insert(
                "access-control-allow-credentials",
                http::HeaderValue::from_static("true"),
            );
        }
        if request.method() == http::Method::OPTIONS {
            for (from, to) in [
                (
                    "access-control-request-method",
                    "access-control-allow-methods",
                ),
                (
                    "access-control-request-headers",
                    "access-control-allow-headers",
                ),
            ] {
                headers.insert(
                    to,
                    request
                        .headers()
                        .get(from)
                        .cloned()
                        .unwrap_or_else(|| http::HeaderValue::from_static("*")),
                );
            }
        }
        self.pad_response(&mut headers)?;
        Ok(headers)
    }
}

#[cfg(test)]
#[path = "../../tests/xhttp_request/mod.rs"]
mod tests;
