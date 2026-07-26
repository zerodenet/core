use std::future::poll_fn;
use std::io;

use bytes::Bytes;
use zero_transport::RuntimeError;

type H3RequestSender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type H3ServerConnection = h3::server::Connection<h3_quinn::Connection, Bytes>;

pub(crate) struct Hysteria2Http3ServerGuard {
    _connection: std::sync::Mutex<H3ServerConnection>,
}

/// One authenticated standard Hysteria2 HTTP/3 session.
///
/// The request sender and driver must remain alive for as long as raw proxy
/// streams/datagrams use the underlying QUIC connection. Dropping the final
/// sender closes the HTTP/3 connection, so they are intentionally owned by
/// this guard instead of being temporary authentication locals.
pub struct Hysteria2AuthenticatedConnection {
    connection: quinn::Connection,
    authentication: AuthenticationGuard,
}

enum AuthenticationGuard {
    Http3 {
        _request_sender: H3RequestSender,
        driver: tokio::task::JoinHandle<()>,
    },
    Legacy,
}

impl Hysteria2AuthenticatedConnection {
    pub fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    pub(crate) fn legacy(connection: quinn::Connection) -> Self {
        Self {
            connection,
            authentication: AuthenticationGuard::Legacy,
        }
    }
}

impl Drop for Hysteria2AuthenticatedConnection {
    fn drop(&mut self) {
        if let AuthenticationGuard::Http3 { driver, .. } = &self.authentication {
            driver.abort();
        }
    }
}

pub async fn authenticate_http3(
    connection: quinn::Connection,
    password: &str,
) -> Result<Hysteria2AuthenticatedConnection, RuntimeError> {
    let h3_connection = h3_quinn::Connection::new(connection.clone());
    let (mut driver, mut request_sender) = h3::client::new(h3_connection)
        .await
        .map_err(h3_error("initialize HTTP/3 client"))?;
    let driver = tokio::spawn(async move {
        let _ = poll_fn(|context| driver.poll_close(context)).await;
    });

    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("https://hysteria/auth")
        .header("Hysteria-Auth", password)
        .header("Hysteria-CC-RX", "0")
        .body(())
        .map_err(|error| {
            RuntimeError::Io(io::Error::other(format!(
                "hysteria2 build authentication request: {error}"
            )))
        })?;
    let mut request_stream = request_sender
        .send_request(request)
        .await
        .map_err(h3_error("send authentication request"))?;
    request_stream
        .finish()
        .await
        .map_err(h3_error("finish authentication request"))?;
    let response = request_stream
        .recv_response()
        .await
        .map_err(h3_error("receive authentication response"))?;
    if response.status().as_u16() != 233 {
        driver.abort();
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "hysteria2 authentication rejected with HTTP status {}",
                response.status()
            ),
        )));
    }
    drop(request_stream);

    Ok(Hysteria2AuthenticatedConnection {
        connection,
        authentication: AuthenticationGuard::Http3 {
            _request_sender: request_sender,
            driver,
        },
    })
}

pub(crate) async fn authenticate_http3_inbound(
    connection: quinn::Connection,
    profile: &crate::inbound::Hysteria2InboundProfile,
) -> Result<(zero_core::SessionAuth, Hysteria2Http3ServerGuard), RuntimeError> {
    let h3_connection = h3_quinn::Connection::new(connection);
    let mut server = h3::server::builder()
        .build(h3_connection)
        .await
        .map_err(h3_error("initialize HTTP/3 server"))?;
    let resolver = server
        .accept()
        .await
        .map_err(h3_error("accept authentication request"))?
        .ok_or_else(|| {
            RuntimeError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "hysteria2 HTTP/3 connection closed before authentication",
            ))
        })?;
    let (request, mut request_stream) = resolver
        .resolve_request()
        .await
        .map_err(h3_error("decode authentication request"))?;
    let authority_is_hysteria = request
        .uri()
        .authority()
        .map(|authority| authority.host() == "hysteria")
        .unwrap_or(false);
    let auth_value = request
        .headers()
        .get("hysteria-auth")
        .and_then(|value| value.to_str().ok());
    let is_auth_request = request.method() == http::Method::POST
        && request.uri().path() == "/auth"
        && authority_is_hysteria;
    let auth = if is_auth_request {
        auth_value.and_then(|password| profile.authenticate_password(password).ok())
    } else {
        None
    };

    let status = http::StatusCode::from_u16(if auth.is_some() { 233 } else { 404 })
        .expect("valid Hysteria2 authentication status");
    let mut response = http::Response::builder().status(status);
    if auth.is_some() {
        response = response
            .header("Hysteria-UDP", "true")
            .header("Hysteria-CC-RX", "0");
    }
    request_stream
        .send_response(response.body(()).map_err(|error| {
            RuntimeError::Io(io::Error::other(format!(
                "hysteria2 build authentication response: {error}"
            )))
        })?)
        .await
        .map_err(h3_error("send authentication response"))?;
    request_stream
        .finish()
        .await
        .map_err(h3_error("finish authentication response"))?;
    drop(request_stream);

    let auth = auth.ok_or_else(|| {
        RuntimeError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "hysteria2 authentication rejected",
        ))
    })?;
    Ok((
        auth,
        Hysteria2Http3ServerGuard {
            _connection: std::sync::Mutex::new(server),
        },
    ))
}

fn h3_error<E>(stage: &'static str) -> impl FnOnce(E) -> RuntimeError
where
    E: std::fmt::Display,
{
    move |error| RuntimeError::Io(io::Error::other(format!("hysteria2 {stage}: {error}")))
}
