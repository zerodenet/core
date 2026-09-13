use super::{BrowserDialer, BrowserDialerOptions, IdleConnection, Inner};
use base64::Engine;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Semaphore},
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
        let listener = TcpListener::bind(address).await?;
        let local_addr = listener.local_addr()?;
        let authority = match local_addr {
            SocketAddr::V4(value) => value.to_string(),
            SocketAddr::V6(value) => format!("[{}]:{}", value.ip(), value.port()),
        };
        let origin = format!("http://{authority}");
        let token =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
        let page_url = format!("{origin}/");
        let page = include_str!("page.html")
            .replace("__TOKEN__", &token)
            .replace("__IDLE_TARGET__", &options.idle_capacity.min(8).to_string());
        let (sender, receiver) = mpsc::channel(options.idle_capacity);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let inner = Arc::new(Inner {
            idle: tokio::sync::Mutex::new(receiver),
            cancellation: cancellation.clone(),
            task_timeout: options.task_timeout,
            max_task_bytes: options.max_task_bytes,
            max_payload_bytes: options.max_payload_bytes,
        });
        let permits = Arc::new(Semaphore::new(options.idle_capacity.saturating_add(8)));
        let task = tokio::spawn(run(
            listener,
            sender,
            permits,
            token,
            origin,
            Arc::<str>::from(page),
            cancellation,
        ));
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
    sender: mpsc::Sender<IdleConnection>,
    permits: Arc<Semaphore>,
    token: String,
    origin: String,
    page: Arc<str>,
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
                        stream,
                        sender.clone(),
                        token.clone(),
                        origin.clone(),
                        page.clone(),
                        cancellation.clone(),
                        permit,
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

async fn serve(
    mut stream: TcpStream,
    sender: mpsc::Sender<IdleConnection>,
    token: String,
    origin: String,
    page: Arc<str>,
    cancellation: tokio_util::sync::CancellationToken,
    permit: tokio::sync::OwnedSemaphorePermit,
) {
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
            let size = stream.read(&mut chunk[..capacity]).await?;
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
        let _ = write_response(&mut stream, 200, "text/html; charset=utf-8", &page).await;
        return;
    }
    if request.token.as_deref() != Some(token.as_str())
        || request.path != "/websocket"
        || !request.websocket
        || request.origin.as_deref() != Some(origin.as_str())
    {
        let _ = write_response(&mut stream, 403, "text/plain", "forbidden").await;
        return;
    }
    let socket = PrefixedSocket::from_prefix(TokioSocket::new(stream), prefix);
    let expected_origin = origin.clone();
    let upgraded = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        tokio_tungstenite::accept_hdr_async(
            socket,
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
    };
    tokio::select! {
        result = sender.send(idle) => {
            if result.is_err() {
                tracing::debug!("Browser Dialer stopped before browser registration");
            }
        }
        _ = cancellation.cancelled() => {}
    }
}

async fn write_response(
    stream: &mut TcpStream,
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
