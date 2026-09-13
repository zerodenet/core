//! Explicit, loopback-only data channel for requests executed by a real browser.
//!
//! The listener is owned by the caller and never installs process-global state.
//! A browser registers authenticated, one-task WebSockets; outbound transports
//! borrow [`BrowserDialer`] from their prepared runtime service.

mod server;
mod socket;
pub(crate) mod task;

use futures_util::SinkExt;
use std::{io, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use zero_platform_tokio::PrefixedSocket;

pub use server::BrowserDialerServer;
pub use socket::BrowserStream;

type ControlSocket = WebSocketStream<PrefixedSocket>;

struct IdleConnection {
    socket: ControlSocket,
    _permit: tokio::sync::OwnedSemaphorePermit,
    cancellation: tokio_util::sync::CancellationToken,
}

struct Inner {
    idle: Mutex<mpsc::Receiver<IdleConnection>>,
    cancellation: tokio_util::sync::CancellationToken,
    task_timeout: Duration,
    max_task_bytes: usize,
    max_payload_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct BrowserDialerOptions {
    pub idle_capacity: usize,
    pub task_timeout: Duration,
    pub max_task_bytes: usize,
    pub max_payload_bytes: usize,
}

impl Default for BrowserDialerOptions {
    fn default() -> Self {
        Self {
            idle_capacity: 64,
            task_timeout: Duration::from_secs(30),
            max_task_bytes: 64 * 1024,
            max_payload_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct BrowserDialer {
    inner: Arc<Inner>,
}

impl BrowserDialer {
    /// Retire this listener generation and reject future task assignments.
    pub fn close(&self) {
        self.inner.cancellation.cancel();
    }

    async fn assign(&self, task: &task::Task) -> io::Result<IdleConnection> {
        if self.inner.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Browser Dialer is stopped",
            ));
        }
        let encoded = serde_json::to_string(task).map_err(io::Error::other)?;
        if encoded.len() > self.inner.max_task_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Browser Dialer task is too large",
            ));
        }
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        loop {
            let mut idle = self.inner.idle.lock().await;
            let connection = tokio::select! {
                _ = self.inner.cancellation.cancelled() => None,
                result = tokio::time::timeout_at(deadline, idle.recv()) => {
                    result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Browser Dialer has no idle browser connection"))?
                }
            };
            drop(idle);
            let Some(mut connection) = connection else {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Browser Dialer is stopped",
                ));
            };
            let sent = tokio::time::timeout_at(
                deadline,
                connection.socket.send(Message::Text(encoded.clone())),
            )
            .await;
            if !matches!(sent, Ok(Ok(()))) {
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Browser Dialer task dispatch timed out",
                    ));
                }
                continue;
            }
            // Once assigned, the browser may already have opened the remote
            // request. Match the reference: surface its acknowledgement error
            // instead of replaying the task or replacing the error with timeout.
            task::read_ack(&mut connection.socket, deadline).await?;
            return Ok(connection);
        }
    }

    pub async fn dial_websocket(
        &self,
        url: &str,
        early_data: Option<&[u8]>,
        heartbeat_period_secs: u32,
    ) -> io::Result<BrowserStream> {
        task::validate_url(url, &["ws", "wss"])?;
        let task = task::Task::websocket(url, early_data);
        let connection = self.assign(&task).await?;
        Ok(BrowserStream::new(connection, heartbeat_period_secs))
    }

    pub async fn dial_get(
        &self,
        url: &str,
        headers: &http::HeaderMap,
    ) -> io::Result<BrowserStream> {
        task::validate_url(url, &["http", "https"])?;
        let task = task::Task::http("GET", url, headers, true)?;
        let connection = self.assign(&task).await?;
        Ok(BrowserStream::new(connection, 0))
    }

    pub async fn send_packet(
        &self,
        method: &http::Method,
        url: &str,
        headers: &http::HeaderMap,
        payload: &[u8],
    ) -> io::Result<()> {
        task::validate_url(url, &["http", "https"])?;
        if payload.len() > self.inner.max_payload_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Browser Dialer payload is too large",
            ));
        }
        if matches!(method.as_str(), "CONNECT" | "TRACE" | "TRACK") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Browser Dialer packet method is forbidden by Fetch",
            ));
        }
        let task = task::Task::http(method.as_str(), url, headers, false)?;
        let mut connection = self.assign(&task).await?;
        connection
            .socket
            .send(Message::Binary(payload.to_vec()))
            .await
            .map_err(io::Error::other)?;
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        // A missing final acknowledgement is intentionally not retried: the
        // browser may already have delivered the business bytes.
        task::read_ack(&mut connection.socket, deadline).await
    }
}
