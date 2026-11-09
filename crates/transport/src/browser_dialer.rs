//! Explicit, loopback-only data channel for requests executed by a real browser.
//!
//! The listener is owned by the caller and never installs process-global state.
//! A browser registers authenticated, one-task WebSockets; outbound transports
//! borrow [`BrowserDialer`] from their prepared runtime service.

mod idle;
mod loan;
mod memory;
pub use loan::PreparedBrowserLoan;
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

// Cancelling actual assignment after taking an idle Source cannot leave its
// provider waiting on an ownerless task. Drop is only a stop request: the
// original Root/provider/Scope still decide native closure and accounting.
struct AssignmentOriginal(Option<BrowserOriginal>);
impl Drop for AssignmentOriginal {
    fn drop(&mut self) {
        if let Some(original) = &self.0 {
            original.request_stop();
        }
    }
}

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
    /// Same actual accounting pair, without IO, policy or Scope authority.
    pub fn same_owned_budget(
        &self,
        book: &zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
        memory: &PacketDriverMetadataView,
    ) -> bool {
        self.inner.factory.as_ref().is_some_and(|actual| {
            actual.book.same_budget(book) && actual.metadata.same_root(memory)
        })
    }
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
        self.assign_inner(task, deadline, None).await
    }
    async fn assign_inner(
        &self,
        task: &task::Task<'_>,
        deadline: tokio::time::Instant,
        upstream: Option<&zero_platform_tokio::contract::network::UpstreamTransport>,
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
            let mut assigned = AssignmentOriginal(upstream.and_then(|_| {
                connection
                    .original
                    .as_ref()
                    .map(|original| original.0.clone())
            }));
            if let Some(upstream) = upstream {
                // This exact Source is registered before its original handler
                // firstawait. The caller receives the same original BEFORE any
                // control task may reach the browser or open a remote request.
                let original = connection.original.as_ref().ok_or_else(|| {
                    io::Error::other("hosted Browser assignment has no actual accepted original")
                })?;
                let pending = original
                    .0
                    .protocol_original(upstream)
                    .map_err(io::Error::other)?;
                if let Some(pending) = pending {
                    let observer = upstream.protocol_original_observer().ok_or_else(|| {
                        io::Error::other("actual Browser pending receiver disappeared")
                    })?;
                    if let Err(source) = observer.retain_protocol_original(pending) {
                        original.0.request_stop();
                        return Err(io::Error::other(source));
                    }
                } else {
                    original.0.request_stop();
                    return Err(io::Error::other(
                        "hosted Browser assignment requires its actual original receiver",
                    ));
                }
            }
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
                // An owned source has already been submitted to the browser.
                // A partially written WS task is Unknown, never authorization
                // to replay it on a different accepted original.
                if upstream.is_some() {
                    return match sent {
                        Ok(Err(source)) => Err(io::Error::other(source)),
                        Err(source) => Err(io::Error::new(io::ErrorKind::TimedOut, source)),
                        Ok(Ok(())) => unreachable!(),
                    };
                }
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
            drop(assigned.0.take());
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

    pub async fn dial_websocket_owned(
        &self,
        url: &str,
        early_data: Option<&[u8]>,
        heartbeat_period_secs: u32,
        upstream: &zero_platform_tokio::contract::network::UpstreamTransport,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> io::Result<BrowserStream> {
        if call.cancel.is_cancelled() {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        let deadline = call.deadline.map_or(deadline, |end| end.min(deadline));
        task::validate_url(url, &["ws", "wss"])?;
        let _early_credit = match early_data {
            Some(bytes) => {
                let size =
                    base64::encoded_len(bytes.len(), false).ok_or(io::ErrorKind::InvalidInput)?;
                tokio::select! { biased;
                    _ = call.cancel.cancelled() => return Err(io::ErrorKind::ConnectionAborted.into()),
                    result = self.reserve_assignment(size, deadline) => result?,
                }
            }
            None => None,
        };
        let task = task::Task::websocket(url, early_data);
        let connection = tokio::select! { biased;
            _ = call.cancel.cancelled() => return Err(io::ErrorKind::ConnectionAborted.into()),
            result = self.assign_inner(&task, deadline, Some(upstream)) => result?,
        };
        Ok(BrowserStream::new(connection, heartbeat_period_secs))
    }
    pub async fn dial_get_owned(
        &self,
        url: &str,
        headers: &http::HeaderMap,
        upstream: &zero_platform_tokio::contract::network::UpstreamTransport,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> io::Result<BrowserStream> {
        if call.cancel.is_cancelled() {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        let deadline = call.deadline.map_or(deadline, |end| end.min(deadline));
        task::validate_url(url, &["http", "https"])?;
        let task = task::Task::http("GET", url, headers, true)?;
        let connection = tokio::select! { biased;
            _ = call.cancel.cancelled() => return Err(io::ErrorKind::ConnectionAborted.into()),
            result = self.assign_inner(&task, deadline, Some(upstream)) => result?,
        };
        Ok(BrowserStream::new(connection, 0))
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

    /// One real Source task, retained by the caller's actual original ledger
    /// before either ACK or payload submission. No per-packet task is spawned.
    pub async fn send_packet_owned(
        &self,
        method: &http::Method,
        url: &str,
        headers: &http::HeaderMap,
        payload: &[u8],
        upstream: &zero_platform_tokio::contract::network::UpstreamTransport,
        call: &zero_platform_tokio::contract::network::CarrierContext,
    ) -> io::Result<()> {
        if self.inner.factory.is_none() || upstream.protocol_original_observer().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned Browser packet has no installed Source/receiver",
            ));
        }
        if call.cancel.is_cancelled() {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        let deadline = tokio::time::Instant::now() + self.inner.task_timeout;
        let deadline = call.deadline.map_or(deadline, |end| end.min(deadline));
        task::validate_url(url, &["http", "https"])?;
        if payload.len() > self.inner.max_payload_bytes
            || matches!(method.as_str(), "CONNECT" | "TRACE" | "TRACK")
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let task = task::Task::http(method.as_str(), url, headers, false)?;
        let mut connection = tokio::select! {biased;
            _=call.cancel.cancelled()=>return Err(io::ErrorKind::ConnectionAborted.into()),
            connection=self.assign_inner(&task,deadline,Some(upstream))=>connection?,
        };
        let original = connection
            .original
            .as_ref()
            .map(|original| original.0.clone())
            .ok_or_else(|| io::Error::other("owned packet lost its actual Source original"))?;
        let peak = payload
            .len()
            .checked_mul(4)
            .and_then(|bytes| bytes.checked_add(512 * 1024))
            .ok_or(io::ErrorKind::InvalidInput)?;
        let work = async {
            connection.memory.transmit(peak).await?;
            connection
                .socket
                .send(Message::Binary(payload.to_vec()))
                .await
                .map_err(io::Error::other)?;
            // Preserve the original separate final-ACK timer, additionally
            // bounded by this current actual driver call, never the old Query.
            let end = tokio::time::Instant::now() + self.inner.task_timeout;
            let end = call.deadline.map_or(end, |bound| bound.min(end));
            task::read_ack(&mut connection.socket, end).await
        };
        let result = tokio::select! {biased;
            _=call.cancel.cancelled()=>Err(io::ErrorKind::ConnectionAborted.into()),
            _=original.0.cancel.cancelled()=>Err(io::ErrorKind::ConnectionAborted.into()),
            result=work=>result,
        };
        original.request_stop();
        drop(connection); // Actual socket/frame backing is gone before its tickets.
                          // A real error returns immediately to the unique original driver so
                          // it can persist the exact source before another cleanup wait. Its
                          // already-strong child ledger continues to own this same Source.
        result?;
        // No next ordered POST can take another original before this one has
        // independently left the actual global Source book.
        original
            .0
            .lineage
            .wait_settled_original()
            .await
            .map_err(io::Error::other)
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
