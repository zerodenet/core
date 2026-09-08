use std::future::poll_fn;
use std::io;

use bytes::Bytes;
use zero_transport::RuntimeError;

type H3RequestSender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
/// One authenticated standard Hysteria2 HTTP/3 session.
///
/// The request sender and driver must remain alive for as long as raw proxy
/// streams/datagrams use the underlying QUIC connection. Dropping the final
/// sender closes the HTTP/3 connection, so they are intentionally owned by
/// this guard instead of being temporary authentication locals.
pub struct Hysteria2AuthenticatedConnection {
    connection: quinn::Connection,
    authentication: AuthenticationGuard,
    negotiated: crate::handshake::AuthResponse,
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

    pub fn negotiated(&self) -> crate::handshake::AuthResponse {
        self.negotiated
    }

    pub(crate) fn require_udp(&self) -> Result<(), RuntimeError> {
        if !self.negotiated.udp_enabled || self.connection.max_datagram_size().is_none() {
            return Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "hysteria2 server does not support UDP relay",
            )));
        }
        Ok(())
    }

    pub(crate) fn legacy(connection: quinn::Connection) -> Self {
        Self {
            connection,
            authentication: AuthenticationGuard::Legacy,
            negotiated: crate::handshake::AuthResponse::from_headers(Some("true"), Some("0")),
        }
    }
}

impl Drop for Hysteria2AuthenticatedConnection {
    fn drop(&mut self) {
        self.connection.close(0x100u32.into(), b"");
        if let AuthenticationGuard::Http3 { driver, .. } = &self.authentication {
            driver.abort();
        }
    }
}

#[cfg(test)]
pub async fn authenticate_http3(
    connection: quinn::Connection,
    password: &str,
) -> Result<Hysteria2AuthenticatedConnection, RuntimeError> {
    authenticate_http3_with_settings(connection, password, Default::default()).await
}

pub async fn authenticate_http3_with_settings(
    connection: quinn::Connection,
    password: &str,
    settings: crate::settings::Settings,
) -> Result<Hysteria2AuthenticatedConnection, RuntimeError> {
    let request = http::Request::builder()
        .method(http::Method::POST)
        .uri("https://hysteria/auth")
        .header("Hysteria-Auth", password)
        .header("Hysteria-CC-RX", settings.download.to_string())
        .body(())
        .map_err(|error| {
            RuntimeError::Io(io::Error::other(format!(
                "hysteria2 build authentication request: {error}"
            )))
        })?;
    let h3_connection = h3_quinn::Connection::new(connection.clone());
    let (mut driver, request_sender) = h3::client::new(h3_connection)
        .await
        .map_err(h3_error("initialize HTTP/3 client"))?;
    let driver = tokio::spawn(async move {
        let _ = poll_fn(|context| driver.poll_close(context)).await;
    });

    let mut authenticated = Hysteria2AuthenticatedConnection {
        connection,
        negotiated: crate::handshake::AuthResponse::from_headers(None, None),
        authentication: AuthenticationGuard::Http3 {
            _request_sender: request_sender,
            driver,
        },
    };
    let AuthenticationGuard::Http3 {
        _request_sender: request_sender,
        ..
    } = &mut authenticated.authentication
    else {
        unreachable!()
    };
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
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "hysteria2 authentication rejected with HTTP status {}",
                response.status()
            ),
        )));
    }
    authenticated.negotiated = crate::handshake::AuthResponse::from_headers(
        response
            .headers()
            .get("hysteria-udp")
            .and_then(|value| value.to_str().ok()),
        response
            .headers()
            .get("hysteria-cc-rx")
            .and_then(|value| value.to_str().ok()),
    );
    super::congestion::negotiate(
        &authenticated.connection,
        settings.client_send_rate(authenticated.negotiated.receive_bandwidth),
    );
    drop(request_stream);
    Ok(authenticated)
}

fn h3_error<E>(stage: &'static str) -> impl FnOnce(E) -> RuntimeError
where
    E: std::fmt::Display,
{
    move |error| RuntimeError::Io(io::Error::other(format!("hysteria2 {stage}: {error}")))
}

#[cfg(test)]
#[path = "tests/auth.rs"]
mod tests;
