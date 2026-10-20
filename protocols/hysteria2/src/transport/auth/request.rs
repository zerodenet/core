//! One protocol exchange for both original and explicitly owned H3 senders.
use super::{h3_error, Bytes, RuntimeError};
use std::io;

/// Execute the original HY2 POST, request FIN, status and negotiation parsing.
/// The caller retains its actual connection, sender and registered H3 driver.
/// This function spawns no task, opens no socket and issues no lifecycle proof.
pub async fn authenticate_http3_request<T>(
    sender: &mut h3::client::SendRequest<T, Bytes>,
    password: &str,
    settings: crate::settings::Settings,
) -> Result<crate::handshake::AuthResponse, RuntimeError>
where
    T: h3::quic::OpenStreams<Bytes>,
{
    exchange(sender, authentication_request(password, settings)?).await
}

pub(super) fn authentication_request(
    password: &str,
    settings: crate::settings::Settings,
) -> Result<http::Request<()>, RuntimeError> {
    http::Request::builder()
        .method(http::Method::POST)
        .uri("https://hysteria/auth")
        .header("Hysteria-Auth", password)
        .header("Hysteria-CC-RX", settings.download.to_string())
        .body(())
        .map_err(h3_error("build authentication request"))
}

pub(super) async fn exchange<T>(
    sender: &mut h3::client::SendRequest<T, Bytes>,
    request: http::Request<()>,
) -> Result<crate::handshake::AuthResponse, RuntimeError>
where
    T: h3::quic::OpenStreams<Bytes>,
{
    let mut stream = sender
        .send_request(request)
        .await
        .map_err(h3_error("send authentication request"))?;
    stream
        .finish()
        .await
        .map_err(h3_error("finish authentication request"))?;
    let response = stream
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
    Ok(crate::handshake::AuthResponse::from_headers(
        response
            .headers()
            .get("hysteria-udp")
            .and_then(|value| value.to_str().ok()),
        response
            .headers()
            .get("hysteria-cc-rx")
            .and_then(|value| value.to_str().ok()),
    ))
}
