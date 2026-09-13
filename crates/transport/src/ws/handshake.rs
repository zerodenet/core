use super::*;
use base64::Engine;
use tokio_tungstenite::tungstenite::{
    handshake::server::{ErrorResponse, Request as ServerRequest, Response},
    http::Request,
};
use zero_traits::WebSocketTransportProfile;

pub async fn accept_ws<S>(stream: S, path: &str) -> Result<WebSocketSocket<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    accept(stream, path, None).await
}
pub async fn accept_ws_profile<S, T: WebSocketTransportProfile + ?Sized>(
    stream: S,
    profile: &T,
) -> Result<WebSocketSocket<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let headers = profile.header_pairs();
    let host = profile.host().or_else(|| {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("host"))
            .map(|(_, value)| value.as_str())
    });
    let mut socket = accept(stream, profile.path(), host).await?;
    socket.set_heartbeat(profile.heartbeat_period_secs());
    Ok(socket)
}
async fn accept<S>(
    stream: S,
    path: &str,
    host: Option<&str>,
) -> Result<WebSocketSocket<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (path, _) = crate::http_early_data::path_options(path);
    let mut early = Vec::new();
    #[allow(clippy::result_large_err)]
    let callback =
        |request: &ServerRequest, mut response: Response| -> Result<Response, ErrorResponse> {
            if request.uri().path() != path.split('?').next().unwrap_or("/")
                || host
                    .filter(|host| !host.is_empty())
                    .is_some_and(|expected| {
                        !request
                            .headers()
                            .get("host")
                            .and_then(|v| v.to_str().ok())
                            .is_some_and(|actual| {
                                crate::http_early_data::host_matches(actual, expected)
                            })
                    })
            {
                let mut error = ErrorResponse::new(Some("WebSocket host/path mismatch".to_owned()));
                *error.status_mut() = http::StatusCode::NOT_FOUND;
                return Err(error);
            }
            if let Some(value) = request.headers().get("sec-websocket-protocol") {
                if let Ok(text) = value.to_str() {
                    let normalized = text.replace('+', "-").replace('/', "_").replace('=', "");
                    if let Ok(bytes) =
                        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(normalized)
                    {
                        if !bytes.is_empty() {
                            early = bytes;
                            response
                                .headers_mut()
                                .insert("sec-websocket-protocol", value.clone());
                        }
                    }
                }
            }
            Ok(response)
        };
    let stream = tokio_tungstenite::accept_hdr_async(stream, callback)
        .await
        .map_err(io::Error::other)?;
    let mut socket = WebSocketSocket::new(stream);
    socket.read_buffer = early;
    Ok(socket)
}
pub async fn connect_ws<S, T: WebSocketTransportProfile + ?Sized>(
    stream: S,
    profile: &T,
    server: &str,
    port: u16,
) -> Result<WebSocketClient<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let host = format!("{server}:{port}");
    let (path, early) = crate::http_early_data::path_options(profile.path());
    let headers = profile.header_pairs();
    let header_host = profile
        .host()
        .or_else(|| {
            headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("host"))
                .map(|(_, value)| value.as_str())
        })
        .unwrap_or(&host);
    let mut request = Request::builder()
        .uri(format!("ws://{host}{path}"))
        .header("Host", header_host)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        );
    for (key, value) in headers {
        if !key.eq_ignore_ascii_case("host") {
            request = request.header(key, value);
        }
    }
    let mut request = request.body(()).map_err(io::Error::other)?;
    crate::browser::apply_websocket_headers(request.headers_mut());
    let mut connection =
        WebSocketClient::new(stream, request, early, profile.heartbeat_period_secs());
    if early == 0 {
        std::future::poll_fn(|cx| connection.ready(cx)).await?;
    }
    Ok(connection)
}

/// Open the official Browser Dialer WebSocket path without consuming a native
/// carrier. Callers must select this before opening a socket and reject relay,
/// egress-binding, REALITY, or other carrier requirements during preparation.
pub async fn connect_ws_with_browser<T: WebSocketTransportProfile + ?Sized>(
    dialer: &crate::browser_dialer::BrowserDialer,
    profile: &T,
    server: &str,
    port: u16,
    secure: bool,
    early_data: Option<&[u8]>,
) -> Result<crate::browser_dialer::BrowserStream, RuntimeError> {
    let headers = profile.header_pairs();
    if profile.host().is_some_and(|host| !host.is_empty()) || !headers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Browser Dialer WebSocket cannot apply custom Host or request headers",
        )
        .into());
    }
    let (path, limit) = crate::http_early_data::path_options(profile.path());
    if early_data.is_some_and(|data| limit == 0 || data.len() > limit as usize) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Browser Dialer WebSocket early data exceeds the configured limit",
        )
        .into());
    }
    let authority = if (secure && port == 443) || (!secure && port == 80) {
        server.to_owned()
    } else if server.contains(':') && !server.starts_with('[') {
        format!("[{server}]:{port}")
    } else {
        format!("{server}:{port}")
    };
    let url = format!("{}://{authority}{path}", if secure { "wss" } else { "ws" });
    dialer
        .dial_websocket(&url, early_data, profile.heartbeat_period_secs())
        .await
        .map_err(Into::into)
}
