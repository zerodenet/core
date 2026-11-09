use super::{
    idle::IdleQueue, BrowserDialer, BrowserDialerOptions, IdleConnection, Inner, InnerRef,
};
use base64::Engine;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use zero_platform_tokio::{PrefixedSocket, TokioSocket};

const MAX_REQUEST_HEADER: usize = 16 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(4);

pub struct BrowserDialerServer {
    dialer: BrowserDialer,
    local_addr: SocketAddr,
    page_url: String,
    task: Option<JoinHandle<()>>,
}

impl BrowserDialerServer {
    pub async fn bind(address: SocketAddr, options: BrowserDialerOptions) -> io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Browser Dialer listener must use a loopback address",
            ));
        }
        validate_options(&options)?;
        let listener = TcpListener::bind(address).await?;
        let local_addr = listener.local_addr()?;
        let registration = registration(local_addr, &options);
        let page_url = registration.page_url.clone();
        let idle = IdleQueue::library(options.idle_capacity)?;
        let cancellation = tokio_util::sync::CancellationToken::new();
        let inner = InnerRef::Library(Arc::new(Inner {
            idle: idle.clone(),
            cancellation: cancellation.clone(),
            task_timeout: options.task_timeout,
            max_task_bytes: options.max_task_bytes,
            max_payload_bytes: options.max_payload_bytes,
            factory: None,
        }));
        let permits = Arc::new(Semaphore::new(options.idle_capacity.saturating_add(8)));
        let task = tokio::spawn(run(listener, idle, permits, registration, cancellation));
        Ok(Self {
            dialer: BrowserDialer { inner },
            local_addr,
            page_url,
            task: Some(task),
        })
    }

    pub fn dialer(&self) -> BrowserDialer {
        self.dialer.clone()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn page_url(&self) -> &str {
        &self.page_url
    }

    pub async fn shutdown(mut self) {
        self.dialer.close();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for BrowserDialerServer {
    fn drop(&mut self) {
        self.dialer.close();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run(
    listener: TcpListener,
    idle: IdleQueue,
    permits: Arc<Semaphore>,
    registration: RegistrationRef,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) if peer.ip().is_loopback() => {
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        continue;
                    };
                    connections.spawn(serve(
                        TokioSocket::new(stream),
                        idle.clone(),
                        registration.clone(),
                        cancellation.clone(),
                        Some(permit),
                        None,
                        super::memory::Memory::Library,
                    ));
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(%error, "Browser Dialer listener stopped");
                    break;
                }
            }
        }
    }
    cancellation.cancel();
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

pub(super) async fn serve(
    stream: TokioSocket,
    idle_queue: IdleQueue,
    registration: RegistrationRef,
    cancellation: tokio_util::sync::CancellationToken,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    original: Option<super::original::BrowserUse>,
    memory: super::memory::Memory,
) {
    let stopped = cancellation.clone();
    tokio::select! { biased;
        _ = stopped.cancelled() => {},
        _ = serve_inner(stream, idle_queue, registration, cancellation, permit, original, memory) => {},
    }
}
async fn serve_inner(
    mut stream: TokioSocket,
    idle_queue: IdleQueue,
    registration: RegistrationRef,
    cancellation: tokio_util::sync::CancellationToken,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    original: Option<super::original::BrowserUse>,
    memory: super::memory::Memory,
) {
    let Registration {
        token,
        origin,
        page,
        ..
    } = &*registration;
    let mut prefix = Vec::with_capacity(1024);
    let read = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        loop {
            if prefix.len() == MAX_REQUEST_HEADER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Browser Dialer request header is too large",
                ));
            }
            let mut chunk = [0u8; 1024];
            let capacity = chunk.len().min(MAX_REQUEST_HEADER - prefix.len());
            #[cfg(not(test))]
            let size = stream.read(&mut chunk[..capacity]).await?;
            #[cfg(test)]
            let size = {
                use std::future::Future;
                let reading = stream.read(&mut chunk[..capacity]);
                tokio::pin!(reading);
                std::future::poll_fn(|cx| {
                    let outcome = reading.as_mut().poll(cx);
                    if outcome.is_pending() {
                        if let Some(original) = &original {
                            original
                                .0
                                 .0
                                .read_pending
                                .store(true, std::sync::atomic::Ordering::Release);
                            original.0 .0.notify();
                        }
                    }
                    outcome
                })
                .await?
            };
            if size == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            prefix.extend_from_slice(&chunk[..size]);
            if prefix.windows(4).any(|window| window == b"\r\n\r\n") {
                return Ok(());
            }
        }
    })
    .await;
    if !matches!(read, Ok(Ok(()))) {
        return;
    }
    let Ok(head) = std::str::from_utf8(&prefix) else {
        return;
    };
    let Some(request) = ParsedRequest::parse(head) else {
        return;
    };
    if request.method != "GET" {
        let _ = write_response(&mut stream, 403, "text/plain", "forbidden").await;
        return;
    }
    if request.path == "/" && !request.websocket {
        let _ = write_response(&mut stream, 200, "text/html; charset=utf-8", page).await;
        return;
    }
    if request.token.as_deref() != Some(token.as_str())
        || request.path != "/websocket"
        || !request.websocket
        || request.origin != Some(origin.as_str())
    {
        let _ = write_response(&mut stream, 403, "text/plain", "forbidden").await;
        return;
    }
    let http_length = prefix
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("validated same header")
        + 4;
    let socket = match super::wire::Socket::new(
        PrefixedSocket::from_prefix(stream, prefix),
        http_length,
        memory.clone(),
        cancellation.clone(),
    ) {
        Ok(socket) => socket,
        Err(_) => return,
    };
    let expected_origin = origin.clone();
    let upgraded = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async(
            socket,
            // Tungstenite requires its unboxed ErrorResponse in this callback.
            #[allow(clippy::result_large_err)]
            move |request: &Request, response: Response| {
                if request
                    .headers()
                    .get("origin")
                    .and_then(|value| value.to_str().ok())
                    != Some(expected_origin.as_str())
                {
                    let mut error = ErrorResponse::new(Some("forbidden".into()));
                    *error.status_mut() = http::StatusCode::FORBIDDEN;
                    return Err(error);
                }
                Ok(response)
            },
        ),
    )
    .await;
    let Ok(Ok(socket)) = upgraded else {
        return;
    };
    let idle = IdleConnection {
        socket,
        _permit: permit,
        cancellation: cancellation.clone(),
        original,
        memory,
    };
    tokio::select! {
        result = idle_queue.send(idle) => {
            if result.is_err() {
                tracing::debug!("Browser Dialer stopped before browser registration");
            }
        }
        _ = cancellation.cancelled() => {}
    }
}

async fn write_response(
    stream: &mut TokioSocket,
    status: u16,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let reason = if status == 200 { "OK" } else { "Forbidden" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await
}

struct ParsedRequest<'a> {
    method: &'a str,
    path: String,
    token: Option<String>,
    origin: Option<&'a str>,
    websocket: bool,
}

impl<'a> ParsedRequest<'a> {
    fn parse(head: &'a str) -> Option<Self> {
        let mut lines = head.split("\r\n");
        let mut request = lines.next()?.split_whitespace();
        let method = request.next()?;
        let target = request.next()?;
        let url = url::Url::parse(&format!("http://localhost{target}")).ok()?;
        let token = url
            .query_pairs()
            .find(|(key, _)| key == "token")
            .map(|(_, value)| value.into_owned());
        let mut origin = None;
        let mut websocket = false;
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            if name.eq_ignore_ascii_case("origin") {
                origin = Some(value);
            } else if name.eq_ignore_ascii_case("upgrade")
                && value.eq_ignore_ascii_case("websocket")
            {
                websocket = true;
            }
        }
        Some(Self {
            method,
            path: url.path().into(),
            token,
            origin,
            websocket,
        })
    }
}

pub(super) fn validate_options(options: &BrowserDialerOptions) -> io::Result<()> {
    if options.idle_capacity == 0
        || options.idle_capacity > 4096
        || options.task_timeout.is_zero()
        || options.max_task_bytes == 0
        || options.max_payload_bytes == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Browser Dialer resource limits must be non-zero",
        ));
    }
    Ok(())
}
pub(super) struct Registration {
    token: String,
    origin: String,
    page: String,
    pub(super) page_url: String,
    _storage: Option<zero_platform_tokio::controlled_carrier::CarrierBufferCredit>,
}
pub(super) fn registration(
    local_addr: SocketAddr,
    options: &BrowserDialerOptions,
) -> RegistrationRef {
    RegistrationRef::Library(Arc::new(registration_value(local_addr, options, None)))
}
fn registration_value(
    local_addr: SocketAddr,
    options: &BrowserDialerOptions,
    storage: Option<zero_platform_tokio::controlled_carrier::CarrierBufferCredit>,
) -> Registration {
    let authority = local_addr.to_string();
    let origin = format!("http://{authority}");
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let page_url = format!("{origin}/");
    let page = include_str!("page.html")
        .replace("__TOKEN__", &token)
        .replace("__IDLE_TARGET__", &options.idle_capacity.min(8).to_string());
    Registration {
        token,
        origin,
        page,
        page_url,
        _storage: storage,
    }
}

#[derive(Clone)]
pub(super) enum RegistrationRef {
    Library(Arc<Registration>),
    Hosted(zero_traits::packet_metadata::PacketMetadataArc<Registration>),
}
impl std::ops::Deref for RegistrationRef {
    type Target = Registration;
    fn deref(&self) -> &Registration {
        match self {
            Self::Library(value) => value,
            Self::Hosted(value) => value,
        }
    }
}
pub(super) fn registration_hosted(
    local: SocketAddr,
    options: &BrowserDialerOptions,
    factory: &super::memory::Factory,
) -> io::Result<RegistrationRef> {
    let bytes = include_str!("page.html")
        .len()
        .checked_mul(4)
        .and_then(|n| n.checked_add(4096))
        .ok_or(io::ErrorKind::InvalidInput)?;
    let layout = zero_traits::packet_metadata::PacketMetadataArc::<Registration>::layout()
        .map_err(io::Error::other)?;
    let header = factory.metadata.reserve(layout).map_err(io::Error::other)?;
    let storage = factory.book.try_reserve(bytes)?;
    let value = registration_value(local, options, Some(storage));
    zero_traits::packet_metadata::PacketMetadataArc::from_prepaid(value, header)
        .map(RegistrationRef::Hosted)
        .map_err(|(source, _, _)| io::Error::other(source))
}
