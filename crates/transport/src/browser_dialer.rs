//! Explicit, loopback-only data channel for requests executed by a real browser.
//!
//! The listener is owned by the caller and never installs process-global state.
//! A browser registers authenticated, one-task WebSockets; outbound transports
//! borrow [`BrowserDialer`] from their prepared runtime service.

mod idle;
mod memory;
mod original;
mod owned;
mod server;
mod socket;
pub(crate) mod task;
mod wire;

use futures_util::SinkExt;
use idle::IdleQueue;
use std::ops::Deref;
use std::{io, sync::Arc, time::Duration};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
use zero_traits::packet_metadata::PacketMetadataArc;

pub use original::BrowserOriginal;
pub use owned::ScopedBrowserDialer;
pub use server::BrowserDialerServer;
pub use socket::BrowserStream;

type ControlSocket = WebSocketStream<wire::Socket>;

struct IdleConnection {
    socket: ControlSocket,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    cancellation: tokio_util::sync::CancellationToken,
    original: Option<original::BrowserUse>,
    memory: memory::Memory,
}

struct Inner {
    idle: IdleQueue,
    cancellation: tokio_util::sync::CancellationToken,
    task_timeout: Duration,
    max_task_bytes: usize,
    max_payload_bytes: usize,
    factory: Option<memory::Factory>,
}

#[derive(Clone)]
enum InnerRef {
    Library(Arc<Inner>),
    Hosted(PacketMetadataArc<Inner>),
}
impl InnerRef {
    fn hosted(value: Inner, memory: &PacketDriverMetadataView) -> io::Result<Self> {
        paid_arc(value, memory).map(Self::Hosted)
    }
}
impl Deref for InnerRef {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        match self {
            Self::Library(inner) => inner,
            Self::Hosted(inner) => inner,
        }
    }
}
fn paid_arc<T>(value: T, memory: &PacketDriverMetadataView) -> io::Result<PacketMetadataArc<T>> {
    let layout = PacketMetadataArc::<T>::layout().map_err(io::Error::other)?;
    let credit = memory.reserve(layout).map_err(io::Error::other)?;
    PacketMetadataArc::from_prepaid(value, credit)
        .map_err(|(source, _, _)| io::Error::other(source))
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
    inner: InnerRef,
}

impl BrowserDialer {
    /// Retire this listener generation and reject future task assignments.
    pub fn close(&self) {
        self.inner.cancellation.cancel();
        self.inner.idle.close();
    }

    async fn reserve_assignment(
        &self,
        bytes: usize,
        deadline: tokio::time::Instant,
    ) -> io::Result<Option<zero_platform_tokio::controlled_carrier::CarrierBufferCredit>> {
        let Some(factory) = &self.inner.factory else {
            return Ok(None);
        };
        tokio::select! { biased;
            _ = self.inner.cancellation.cancelled() => Err(io::ErrorKind::ConnectionAborted.into()),
            credit = tokio::time::timeout_at(deadline, factory.prepare(bytes)) =>
                credit.map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?.map(Some),
        }
    }
    async fn assign(
        &self,
        task: &task::Task<'_>,
        deadline: tokio::time::Instant,
    ) -> io::Result<IdleConnection> {
        if self.inner.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Browser Dialer is stopped",
            ));
        }
        let length = task.encoded_len()?;
        if length > self.inner.max_task_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Browser Dialer task is too large",
            ));
        }
        let _encoded_credit = self.reserve_assignment(length, deadline).await?;
        let encoded = task.encode(length)?;
        loop {
            let connection = tokio::select! {
                _ = self.inner.cancellation.cancelled() => None,
                result = tokio::time::timeout_at(deadline, self.inner.idle.recv()) => {
                    result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Browser Dialer has no idle browser connection"))?
                }
            };
            let Some(mut connection) = connection else {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Browser Dialer is stopped",
                ));
            };
            let transmit = length
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(512 * 1024))
                .ok_or(io::ErrorKind::InvalidInput)?;
            tokio::select! { biased;
                _ = self.inner.cancellation.cancelled() => return Err(io::ErrorKind::ConnectionAborted.into()),
                result = tokio::time::timeout_at(deadline, connection.memory.transmit(transmit)) =>
                    result.map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))??,
            }
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
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        task::validate_url(url, &["ws", "wss"])?;
        let _early_credit = match early_data {
            Some(bytes) => {
                let size =
                    base64::encoded_len(bytes.len(), false).ok_or(io::ErrorKind::InvalidInput)?;
                self.reserve_assignment(size, deadline).await?
            }
            None => None,
        };
        let task = task::Task::websocket(url, early_data);
        let connection = self.assign(&task, deadline).await?;
        Ok(BrowserStream::new(connection, heartbeat_period_secs))
    }

    pub async fn dial_get(
        &self,
        url: &str,
        headers: &http::HeaderMap,
    ) -> io::Result<BrowserStream> {
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        task::validate_url(url, &["http", "https"])?;
        let task = task::Task::http("GET", url, headers, true)?;
        let connection = self.assign(&task, deadline).await?;
        Ok(BrowserStream::new(connection, 0))
    }

    pub async fn send_packet(
        &self,
        method: &http::Method,
        url: &str,
        headers: &http::HeaderMap,
        payload: &[u8],
    ) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
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
        let mut connection = self.assign(&task, deadline).await?;
        let peak = payload
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(512 * 1024))
            .ok_or(io::ErrorKind::InvalidInput)?;
        tokio::select! { biased;
            _ = connection.cancellation.cancelled() => return Err(io::ErrorKind::ConnectionAborted.into()),
            result = connection.memory.transmit(peak) => result?,
        }
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

#[cfg(test)]
#[path = "../tests/browser_dialer/owned/mod.rs"]
mod owned_tests;
