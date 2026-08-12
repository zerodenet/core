use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use zero_core::{Error, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;

use crate::body::HttpBodyKind;
use crate::parse::{parse_request_line, ParsedHttpRequestLine};
use crate::wire::{
    append_header, connection_tokens, content_length, eq_ascii, has_token, header_values,
    is_hop_header, named_by_connection, parse_head, read_head, transfer_encoding,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpInboundMode {
    Connect,
    Forward,
}

#[derive(Debug)]
pub struct HttpInboundRequest {
    session: Session,
    mode: HttpInboundMode,
    replay: Vec<u8>,
    forward: Option<HttpForwardRequest>,
}

impl HttpInboundRequest {
    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn mode(&self) -> HttpInboundMode {
        self.mode
    }

    pub fn into_parts(self) -> (Session, HttpInboundMode, Vec<u8>) {
        (self.session, self.mode, self.replay)
    }

    pub fn into_forward(self) -> Option<HttpForwardRequest> {
        self.forward
    }
}

#[derive(Debug)]
pub struct HttpForwardRequest {
    session: Session,
    method: String,
    head: Vec<u8>,
    body: HttpBodyKind,
    close_after_response: bool,
    upgrade_requested: bool,
    expect_continue: bool,
    supports_chunked_response: bool,
}

impl HttpForwardRequest {
    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn into_session(self) -> Session {
        self.session
    }

    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn head(&self) -> &[u8] {
        &self.head
    }

    pub fn body(&self) -> HttpBodyKind {
        self.body
    }

    pub fn close_after_response(&self) -> bool {
        self.close_after_response
    }

    pub fn upgrade_requested(&self) -> bool {
        self.upgrade_requested
    }

    pub fn expect_continue(&self) -> bool {
        self.expect_continue
    }

    pub fn supports_chunked_response(&self) -> bool {
        self.supports_chunked_response
    }

    pub fn set_effective_authority(&mut self, target: &zero_core::Address, port: u16) {
        let authority = format_authority(target, port);
        let Some(host_start) = self
            .head
            .windows(6)
            .position(|window| window.eq_ignore_ascii_case(b"Host: "))
        else {
            return;
        };
        let value_start = host_start + 6;
        let Some(relative_end) = self.head[value_start..]
            .windows(2)
            .position(|window| window == b"\r\n")
        else {
            return;
        };
        self.head
            .splice(value_start..value_start + relative_end, authority.bytes());
        self.session.target = target.clone();
        self.session.port = port;
    }

    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        Session,
        String,
        Vec<u8>,
        HttpBodyKind,
        bool,
        bool,
        bool,
        bool,
    ) {
        (
            self.session,
            self.method,
            self.head,
            self.body,
            self.close_after_response,
            self.upgrade_requested,
            self.expect_continue,
            self.supports_chunked_response,
        )
    }
}

#[derive(Debug)]
pub struct HttpForwardResponse {
    head: Vec<u8>,
    body: HttpBodyKind,
    close_after_response: bool,
    upgrade_accepted: bool,
    chunk_close_delimited: bool,
    informational: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpgradeIntent {
    None,
    Requested,
    Accepted,
}

impl HttpForwardResponse {
    pub fn head(&self) -> &[u8] {
        &self.head
    }

    pub fn body(&self) -> HttpBodyKind {
        self.body
    }

    pub fn close_after_response(&self) -> bool {
        self.close_after_response
    }

    pub fn upgrade_accepted(&self) -> bool {
        self.upgrade_accepted
    }

    pub fn chunk_close_delimited(&self) -> bool {
        self.chunk_close_delimited
    }

    pub fn informational(&self) -> bool {
        self.informational
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpConnectResponse {
    ConnectionEstablished,
    Continue,
    BadRequest,
    MethodNotAllowed,
    Forbidden,
    BadGateway,
}

impl HttpConnectResponse {
    fn status_line(self) -> &'static str {
        match self {
            Self::ConnectionEstablished => "HTTP/1.1 200 Connection Established\r\n\r\n",
            Self::Continue => "HTTP/1.1 100 Continue\r\n\r\n",
            Self::BadRequest => {
                "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            }
            Self::MethodNotAllowed => {
                "HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            }
            Self::Forbidden => {
                "HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            }
            Self::BadGateway => {
                "HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct HttpConnectInbound;

impl HttpConnectInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("http")
    }

    pub async fn accept_request<S>(&self, stream: &mut S) -> Result<HttpInboundRequest, Error>
    where
        S: AsyncSocket,
    {
        self.accept_next_request(stream)
            .await?
            .ok_or(Error::Io("unexpected EOF while reading HTTP request"))
    }

    pub async fn accept_next_request<S>(
        &self,
        stream: &mut S,
    ) -> Result<Option<HttpInboundRequest>, Error>
    where
        S: AsyncSocket,
    {
        let Some(head) = read_head(stream).await? else {
            return Ok(None);
        };
        let (line, headers) = parse_head(&head)?;
        let line = core::str::from_utf8(line)
            .map_err(|_| Error::Protocol("HTTP request line is not ASCII"))?;

        match parse_request_line(line)? {
            ParsedHttpRequestLine::Connect { target, port } => {
                let session =
                    Session::new(0, target, port, Network::Tcp, ProtocolType::new("http"));
                Ok(Some(HttpInboundRequest {
                    session,
                    mode: HttpInboundMode::Connect,
                    replay: Vec::new(),
                    forward: None,
                }))
            }
            ParsedHttpRequestLine::Forward {
                method,
                target,
                port,
                authority,
                origin_form,
                version,
            } => {
                let session =
                    Session::new(0, target, port, Network::Tcp, ProtocolType::new("http"));
                let transfer_encoding = transfer_encoding(&headers)?;
                let content_length = content_length(&headers)?;
                if transfer_encoding.is_some() && content_length.is_some() {
                    return Err(Error::Protocol(
                        "HTTP request contains both Transfer-Encoding and Content-Length",
                    ));
                }
                let body = if transfer_encoding.is_some() {
                    HttpBodyKind::Chunked
                } else if let Some(length) = content_length {
                    HttpBodyKind::ContentLength(length)
                } else {
                    HttpBodyKind::None
                };
                let connection = connection_tokens(&headers)?;
                let upgrade_requested = connection.iter().any(|token| eq_ascii(token, b"upgrade"))
                    && !header_values(&headers, b"upgrade").is_empty();
                let close_after_response = connection.iter().any(|token| eq_ascii(token, b"close"))
                    || (version.eq_ignore_ascii_case("HTTP/1.0")
                        && !connection
                            .iter()
                            .any(|token| eq_ascii(token, b"keep-alive")));
                let expect_continue =
                    has_token(&headers, b"expect", b"100-continue") && body != HttpBodyKind::None;

                let normalized = normalize_request_head(
                    &method,
                    &origin_form,
                    &version,
                    &authority,
                    &headers,
                    &connection,
                    transfer_encoding.as_deref(),
                    content_length,
                    upgrade_requested,
                );
                let forward = HttpForwardRequest {
                    session: session.clone(),
                    method,
                    head: normalized.clone(),
                    body,
                    close_after_response,
                    upgrade_requested,
                    expect_continue,
                    supports_chunked_response: version.eq_ignore_ascii_case("HTTP/1.1"),
                };
                Ok(Some(HttpInboundRequest {
                    session,
                    mode: HttpInboundMode::Forward,
                    replay: normalized,
                    forward: Some(forward),
                }))
            }
        }
    }

    pub async fn accept_response<S>(
        &self,
        stream: &mut S,
        request_method: &str,
        request_upgrade: bool,
        client_close: bool,
        client_supports_chunked: bool,
    ) -> Result<HttpForwardResponse, Error>
    where
        S: AsyncSocket,
    {
        let head = read_head(stream)
            .await?
            .ok_or(Error::Protocol("upstream closed before HTTP response"))?;
        let (line, headers) = parse_head(&head)?;
        let (version, status, reason) = parse_status_line(line)?;
        let transfer_encoding = transfer_encoding(&headers)?;
        let content_length = content_length(&headers)?;
        if transfer_encoding.is_some() && content_length.is_some() {
            return Err(Error::Protocol(
                "HTTP response contains both Transfer-Encoding and Content-Length",
            ));
        }
        let connection = connection_tokens(&headers)?;
        let informational = (100..200).contains(&status) && status != 101;
        let upgrade_accepted = status == 101 && request_upgrade;
        if status == 101 && !request_upgrade {
            return Err(Error::Protocol("unexpected HTTP protocol switch"));
        }
        let no_body = request_method.eq_ignore_ascii_case("HEAD")
            || informational
            || matches!(status, 101 | 204 | 304);
        let body = if no_body {
            HttpBodyKind::None
        } else if transfer_encoding.is_some() {
            HttpBodyKind::Chunked
        } else if let Some(length) = content_length {
            HttpBodyKind::ContentLength(length)
        } else {
            HttpBodyKind::UntilClose
        };
        if !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
            return Err(Error::Unsupported("HTTP response version is not supported"));
        }
        let close_after_response =
            client_close || (body == HttpBodyKind::UntilClose && !client_supports_chunked);
        let chunk_close_delimited = body == HttpBodyKind::UntilClose && !close_after_response;
        let upgrade_intent = if upgrade_accepted {
            UpgradeIntent::Accepted
        } else if request_upgrade && !informational {
            UpgradeIntent::Requested
        } else {
            UpgradeIntent::None
        };
        let normalized = normalize_response_head(
            version,
            status,
            reason,
            &headers,
            &connection,
            transfer_encoding.as_deref(),
            content_length,
            close_after_response,
            upgrade_intent,
            chunk_close_delimited,
        );
        Ok(HttpForwardResponse {
            head: normalized,
            body,
            close_after_response,
            upgrade_accepted,
            chunk_close_delimited,
            informational,
        })
    }

    pub async fn send_response<S>(
        &self,
        stream: &mut S,
        response: HttpConnectResponse,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        stream
            .write_all(response.status_line().as_bytes())
            .await
            .map_err(|_| Error::Io("failed to write HTTP response"))
    }

    pub async fn send_success_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream, HttpConnectResponse::ConnectionEstablished)
            .await
    }

    pub async fn send_continue_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream, HttpConnectResponse::Continue)
            .await
    }

    pub async fn send_bad_request_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream, HttpConnectResponse::BadRequest)
            .await
    }

    pub async fn send_method_not_allowed_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream, HttpConnectResponse::MethodNotAllowed)
            .await
    }

    pub async fn send_blocked_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream, HttpConnectResponse::Forbidden)
            .await
    }

    pub async fn send_upstream_failure_response<S>(&self, stream: &mut S) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        self.send_response(stream, HttpConnectResponse::BadGateway)
            .await
    }

    pub async fn send_accept_error_response<S>(
        &self,
        stream: &mut S,
        error: &Error,
    ) -> Result<bool, Error>
    where
        S: AsyncSocket,
    {
        match error {
            Error::Unsupported(_) => {
                self.send_method_not_allowed_response(stream).await?;
                Ok(true)
            }
            Error::Protocol(_) => {
                self.send_bad_request_response(stream).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub fn redirect_response(&self, status: u16, location: &str) -> String {
        format!(
            "HTTP/1.1 {status} Found\r\nLocation: {location}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
        )
    }

    pub async fn send_redirect_response<S>(
        &self,
        stream: &mut S,
        status: u16,
        location: &str,
    ) -> Result<(), Error>
    where
        S: AsyncSocket,
    {
        let response = self.redirect_response(status, location);
        stream
            .write_all(response.as_bytes())
            .await
            .map_err(|_| Error::Io("failed to write HTTP redirect response"))
    }

    pub async fn handshake<S>(&self, stream: &mut S) -> Result<HttpInboundRequest, Error>
    where
        S: AsyncSocket,
    {
        let request = self.accept_request(stream).await?;
        if request.mode() == HttpInboundMode::Connect {
            self.send_success_response(stream).await?;
        }
        Ok(request)
    }
}

#[allow(clippy::too_many_arguments)]
fn normalize_request_head(
    method: &str,
    origin_form: &str,
    version: &str,
    authority: &str,
    headers: &[crate::wire::Header],
    connection: &[Vec<u8>],
    transfer_encoding: Option<&str>,
    content_length: Option<u64>,
    upgrade: bool,
) -> Vec<u8> {
    let mut output = format!("{method} {origin_form} {version}\r\n").into_bytes();
    append_header(&mut output, b"Host", authority.as_bytes());
    for header in headers {
        if eq_ascii(&header.name, b"host")
            || is_hop_header(&header.name)
            || named_by_connection(&header.name, connection)
            || eq_ascii(&header.name, b"expect")
            || eq_ascii(&header.name, b"content-length")
        {
            continue;
        }
        append_header(&mut output, &header.name, &header.value);
    }
    if let Some(encoding) = transfer_encoding {
        append_header(&mut output, b"Transfer-Encoding", encoding.as_bytes());
    } else if let Some(length) = content_length {
        append_header(
            &mut output,
            b"Content-Length",
            length.to_string().as_bytes(),
        );
    }
    if upgrade {
        append_header(&mut output, b"Connection", b"Upgrade");
        if let Some(value) = header_values(headers, b"upgrade").first() {
            append_header(&mut output, b"Upgrade", value);
        }
    } else {
        append_header(&mut output, b"Connection", b"close");
    }
    output.extend_from_slice(b"\r\n");
    output
}

#[allow(clippy::too_many_arguments)]
fn normalize_response_head(
    version: &str,
    status: u16,
    reason: &str,
    headers: &[crate::wire::Header],
    connection: &[Vec<u8>],
    transfer_encoding: Option<&str>,
    content_length: Option<u64>,
    close: bool,
    upgrade: UpgradeIntent,
    chunk_close_delimited: bool,
) -> Vec<u8> {
    let mut output = format!("{version} {status} {reason}\r\n").into_bytes();
    for header in headers {
        if is_hop_header(&header.name)
            || named_by_connection(&header.name, connection)
            || eq_ascii(&header.name, b"content-length")
        {
            continue;
        }
        append_header(&mut output, &header.name, &header.value);
    }
    if chunk_close_delimited {
        append_header(&mut output, b"Transfer-Encoding", b"chunked");
    } else if let Some(encoding) = transfer_encoding {
        append_header(&mut output, b"Transfer-Encoding", encoding.as_bytes());
    } else if let Some(length) = content_length {
        append_header(
            &mut output,
            b"Content-Length",
            length.to_string().as_bytes(),
        );
    }
    if upgrade == UpgradeIntent::Accepted {
        append_header(&mut output, b"Connection", b"Upgrade");
        if let Some(value) = header_values(headers, b"upgrade").first() {
            append_header(&mut output, b"Upgrade", value);
        }
    } else if close || upgrade == UpgradeIntent::Requested {
        append_header(&mut output, b"Connection", b"close");
    }
    output.extend_from_slice(b"\r\n");
    output
}

fn parse_status_line(line: &[u8]) -> Result<(&str, u16, &str), Error> {
    let line =
        core::str::from_utf8(line).map_err(|_| Error::Protocol("HTTP status line is not ASCII"))?;
    let mut parts = line.splitn(3, ' ');
    let version = parts
        .next()
        .filter(|version| version.starts_with("HTTP/"))
        .ok_or(Error::Protocol("HTTP response version is invalid"))?;
    let status = parts
        .next()
        .ok_or(Error::Protocol("HTTP response status is missing"))?
        .parse::<u16>()
        .map_err(|_| Error::Protocol("HTTP response status is invalid"))?;
    let reason = parts.next().unwrap_or_default();
    Ok((version, status, reason))
}

fn format_authority(target: &zero_core::Address, port: u16) -> String {
    let host = match target {
        zero_core::Address::Domain(domain) => domain.clone(),
        zero_core::Address::Ipv4(octets) => core::net::Ipv4Addr::from(*octets).to_string(),
        zero_core::Address::Ipv6(octets) => format!("[{}]", core::net::Ipv6Addr::from(*octets)),
    };
    if port == 80 {
        host
    } else {
        format!("{host}:{port}")
    }
}
