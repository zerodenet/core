use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use zero_core::{Error, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;

use crate::parse::{first_line, parse_request_line, ParsedHttpRequestLine};

const MAX_REQUEST_SIZE: usize = 8192;
const HEADERS_END: &[u8] = b"\r\n\r\n";
const LINE_END: &[u8] = b"\r\n";

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpConnectResponse {
    ConnectionEstablished,
    BadRequest,
    MethodNotAllowed,
    Forbidden,
    BadGateway,
}

impl HttpConnectResponse {
    fn status_line(self) -> &'static str {
        match self {
            Self::ConnectionEstablished => "HTTP/1.1 200 Connection Established\r\n\r\n",
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
        let request = read_request_head(stream).await?;
        let line = first_line(&request)?;
        let line_end = request
            .windows(LINE_END.len())
            .position(|window| window == LINE_END)
            .ok_or(Error::Protocol("HTTP request line is incomplete"))?;

        let (target, port, mode, replay) = match parse_request_line(line)? {
            ParsedHttpRequestLine::Connect { target, port } => {
                (target, port, HttpInboundMode::Connect, Vec::new())
            }
            ParsedHttpRequestLine::Forward {
                target,
                port,
                origin_form_line,
            } => {
                let mut replay = origin_form_line.into_bytes();
                replay.extend_from_slice(&request[line_end + LINE_END.len()..]);
                (target, port, HttpInboundMode::Forward, replay)
            }
        };

        Ok(HttpInboundRequest {
            session: Session::new(0, target, port, Network::Tcp, ProtocolType::new("http")),
            mode,
            replay,
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
        format!("HTTP/1.1 {status} Found\r\nLocation: {location}\r\nConnection: close\r\n\r\n")
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
            self.send_response(stream, HttpConnectResponse::ConnectionEstablished)
                .await?;
        }
        Ok(request)
    }
}

async fn read_request_head<S>(stream: &mut S) -> Result<Vec<u8>, Error>
where
    S: AsyncSocket,
{
    let mut request = Vec::new();

    loop {
        if request.len() >= MAX_REQUEST_SIZE {
            return Err(Error::Protocol("HTTP request head is too large"));
        }

        let mut byte = [0_u8; 1];
        let read = stream
            .read(&mut byte)
            .await
            .map_err(|_| Error::Io("failed to read HTTP request"))?;

        if read == 0 {
            return Err(Error::Io("unexpected EOF while reading HTTP request"));
        }

        request.push(byte[0]);

        if request.ends_with(HEADERS_END) {
            return Ok(request);
        }
    }
}
