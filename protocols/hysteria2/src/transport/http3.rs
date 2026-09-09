//! Persistent HTTP/3 authentication and masquerade, with protocol stream dispatch.
use super::{Hysteria2AuthenticatedInboundProfile, Hysteria2Stream};
use bytes::Bytes;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use zero_transport::RuntimeError;
mod dispatch;
mod exchange;
mod masquerade;
pub use masquerade::Masquerade;
type RequestStream = h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

pub(super) struct Hysteria2Http3ServerGuard {
    connection: quinn::Connection,
    driver: tokio::task::JoinHandle<()>,
    dispatch: tokio::task::JoinHandle<()>,
    streams: AsyncMutex<mpsc::Receiver<(zero_core::Session, Hysteria2Stream)>>,
}
impl Drop for Hysteria2Http3ServerGuard {
    fn drop(&mut self) {
        self.connection.close(0x100u32.into(), b"");
        self.driver.abort();
        self.dispatch.abort();
    }
}
impl Hysteria2Http3ServerGuard {
    pub(super) async fn next_stream(&self) -> Option<(zero_core::Session, Hysteria2Stream)> {
        self.streams.lock().await.recv().await
    }
}
struct AuthState {
    authenticated: Arc<AtomicBool>,
    session: Mutex<Option<zero_core::SessionAuth>>,
    sender: Mutex<Option<oneshot::Sender<zero_core::SessionAuth>>>,
}
pub(super) async fn accept(
    connection: quinn::Connection,
    profile: &Hysteria2AuthenticatedInboundProfile,
) -> Result<(zero_core::SessionAuth, Hysteria2Http3ServerGuard), RuntimeError> {
    let (web_tx, web_rx) = mpsc::channel(128);
    let (tcp_tx, tcp_rx) = mpsc::channel(128);
    let (auth_tx, auth_rx) = oneshot::channel();
    let authenticated = Arc::new(AtomicBool::new(false));
    let dispatch = tokio::spawn(dispatch::run(
        connection.clone(),
        authenticated.clone(),
        web_tx,
        tcp_tx,
    ));
    let incoming = futures_util::stream::unfold(web_rx, |mut rx| async move {
        let stream = rx
            .recv()
            .await
            .unwrap_or(Err(quinn::ConnectionError::LocallyClosed));
        Some((stream, rx))
    });
    let carrier = h3_quinn::Connection::with_incoming_bidi(connection.clone(), incoming);
    let profile = profile.clone();
    let state = Arc::new(AuthState {
        authenticated,
        session: Mutex::new(None),
        sender: Mutex::new(Some(auth_tx)),
    });
    let driver_connection = connection.clone();
    let driver = tokio::spawn(async move {
        let mut server = match h3::server::builder().build(carrier).await {
            Ok(server) => server,
            Err(_) => return,
        };
        let mut requests = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                request = server.accept(), if requests.len() < 64 => {
                    let resolver = match request { Ok(Some(resolver)) => resolver, _ => break };
                    let state = state.clone(); let profile = profile.clone(); let connection = driver_connection.clone();
                    requests.spawn(async move {
                        let work = async {
                            let (request, mut stream) = resolver.resolve_request().await.map_err(io::Error::other)?;
                            serve(request, &mut stream, &profile, &state, &connection).await
                        };
                        // Bound slow headers, request bodies and stalled origins independently.
                        let _ = tokio::time::timeout(std::time::Duration::from_secs(30), work).await;
                    });
                }
                _ = requests.join_next(), if !requests.is_empty() => {}
                _ = driver_connection.closed() => break,
            }
        }
        driver_connection.close(0x100u32.into(), b"");
    });
    // Own both tasks before awaiting auth: cancellation must close this connection.
    let guard = Hysteria2Http3ServerGuard {
        connection,
        driver,
        dispatch,
        streams: AsyncMutex::new(tcp_rx),
    };
    let auth = auth_rx.await.map_err(|_| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "hysteria2 connection ended without authentication",
        )
    })?;
    Ok((auth, guard))
}
async fn serve(
    request: http::Request<()>,
    stream: &mut RequestStream,
    profile: &Hysteria2AuthenticatedInboundProfile,
    state: &AuthState,
    connection: &quinn::Connection,
) -> io::Result<()> {
    let is_auth = request.method() == http::Method::POST
        && request.uri().path() == "/auth"
        && request
            .uri()
            .authority()
            .is_some_and(|a| a.host() == "hysteria");
    let auth = if is_auth {
        let mut current = state.session.lock().unwrap();
        if current.is_none() {
            *current = request
                .headers()
                .get("hysteria-auth")
                .and_then(|v| v.to_str().ok())
                .and_then(|password| profile.protocol.authenticate_password(password).ok());
            if current.is_some() {
                let receive = crate::handshake::AuthResponse::from_headers(
                    None,
                    request
                        .headers()
                        .get("hysteria-cc-rx")
                        .and_then(|v| v.to_str().ok()),
                )
                .receive_bandwidth;
                let receive = match receive {
                    crate::handshake::ReceiveBandwidth::Limit(rate) => rate,
                    _ => 0,
                };
                super::congestion::negotiate(
                    connection,
                    profile.settings.server_send_rate(receive),
                );
                state.authenticated.store(true, Ordering::Release);
            }
        }
        current.clone()
    } else {
        None
    };
    if let Some(auth) = auth {
        let receive = if profile.settings.ignore_client_bandwidth {
            "auto".into()
        } else {
            profile.settings.download.to_string()
        };
        stream
            .send_response(
                http::Response::builder()
                    .status(233)
                    .header("Hysteria-UDP", "true")
                    .header("Hysteria-CC-RX", receive)
                    .body(())
                    .map_err(io::Error::other)?,
            )
            .await
            .map_err(io::Error::other)?;
        stream.finish().await.map_err(io::Error::other)?;
        if let Some(sender) = state.sender.lock().unwrap().take() {
            let _ = sender.send(auth);
        }
        Ok(())
    } else {
        let mut request = request;
        request
            .extensions_mut()
            .insert(zero_transport::http_server::RequestContext {
                peer: connection.remote_address(),
                tls: true,
            });
        profile
            .masquerade
            .serve(request, &mut exchange::Exchange(stream))
            .await
    }
}
