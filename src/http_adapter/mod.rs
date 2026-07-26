pub mod handlers;
pub mod ratelimit;
pub mod response;
pub mod router;
pub mod sse;

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use zero_api::{AuthContext, Permission};
use zero_proxy::ProxyHandle;

use router::{HttpRequest, RouteResult};

const MAX_REQUEST_SIZE: usize = 8192;
const MAX_BODY_SIZE: usize = 256 * 1024;
const REQUEST_END: &[u8] = b"\r\n\r\n";

pub struct HttpServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

/// A single token entry parsed from configuration.
#[derive(Debug, Clone)]
struct TokenEntry {
    name: String,
    key: String,
    permissions: Vec<Permission>,
}

/// Multi-token authentication store for the HTTP API.
///
/// If `tokens` is empty (no auth configured), all requests are treated as
/// admin for local-only control plane usage.
#[derive(Debug, Clone)]
pub struct HttpServerAuth {
    tokens: Vec<TokenEntry>,
}

impl HttpServerAuth {
    /// Create an auth store with a single admin-scoped key.
    pub fn single_admin(api_key: String) -> Self {
        Self {
            tokens: vec![TokenEntry {
                name: "default".to_owned(),
                key: api_key,
                permissions: vec![
                    Permission::Read,
                    Permission::Control,
                    Permission::Config,
                    Permission::Admin,
                ],
            }],
        }
    }

    fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl HttpServerHandle {
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.await.expect("http server task should join")
    }
}

pub async fn spawn_http_server(
    proxy_handle: ProxyHandle,
    listen: &str,
    auth: Option<HttpServerAuth>,
) -> io::Result<HttpServerHandle> {
    let listener = TcpListener::bind(listen).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task =
        tokio::spawn(async move { run_server(listener, proxy_handle, auth, shutdown_rx).await });

    info!(listen = %local_addr, "http api server ready");

    Ok(HttpServerHandle {
        shutdown: Some(shutdown_tx),
        task,
    })
}

async fn run_server(
    listener: TcpListener,
    handle: ProxyHandle,
    auth: Option<HttpServerAuth>,
    mut shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    let limiters = std::sync::Arc::new(ratelimit::ApiRateLimiters::default());
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept() => {
                let (stream, remote_addr) = accept_result?;
                let handle = handle.clone();
                let auth = auth.clone();
                let limiters = limiters.clone();
                connections.spawn(async move {
                    if let Err(error) = serve_connection(stream, handle, auth.as_ref(), &limiters).await {
                        if is_transient_disconnect(&error) {
                            debug!(?remote_addr, error = %error, "http connection closed early");
                        } else {
                            warn!(?remote_addr, error = %error, "http connection failed");
                        }
                    }
                });
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    if !error.is_cancelled() {
                        error!(error = %error, "http connection task panicked");
                    }
                }
            }
        }
    }

    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() {
                error!(error = %error, "http connection task panicked during shutdown");
            }
        }
    }

    info!("http api server stopped");
    Ok(())
}

async fn serve_connection(
    mut stream: TcpStream,
    handle: ProxyHandle,
    auth: Option<&HttpServerAuth>,
    limiters: &ratelimit::ApiRateLimiters,
) -> io::Result<()> {
    let request = read_request(&mut stream).await?;

    let auth_ctx = authenticate(&request, auth);
    if auth_ctx.permissions.is_empty() {
        return write_response(
            &mut stream,
            "HTTP/1.1 401 Unauthorized\r\n",
            br#"{"error":"unauthorized"}"#,
        )
        .await;
    }

    // Rate limiting.
    let category = rate_limit_category(&request);
    let allowed = match category {
        RateLimitCategory::Query => limiters.query.allow(),
        RateLimitCategory::Command => limiters.command.allow(),
        RateLimitCategory::Sse => limiters.sse_connections.allow(),
    };
    if !allowed {
        return write_response_with_headers(
            &mut stream,
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\n",
            br#"{"error":"rate limit exceeded"}"#,
        )
        .await;
    }

    match router::route(&request, &handle, &auth_ctx).await {
        RouteResult::Respond(status, body) => write_response(&mut stream, &status, &body).await,
        RouteResult::Sse {
            subscriber,
            catch_up,
        } => {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            tokio::spawn(async move {
                let _ = shutdown_tx;
            });
            sse::run_sse_stream(
                stream,
                subscriber,
                catch_up,
                Duration::from_secs(30),
                shutdown_rx,
            )
            .await
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> io::Result<HttpRequest> {
    let mut request = Vec::new();

    loop {
        if request.len() >= MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "http request head too large",
            ));
        }

        let mut byte = [0_u8; 1];
        let read = stream.read(&mut byte).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading http request",
            ));
        }

        request.push(byte[0]);
        if request.ends_with(REQUEST_END) {
            break;
        }
    }

    let request = String::from_utf8(request)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request is not utf-8"))?;
    let first_line = request
        .lines()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut parts = first_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing method"))?;
    let path = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing path"))?;

    let headers = parse_headers(&request);
    let content_length = content_length(&headers)?;
    if content_length > MAX_BODY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "request body too large",
        ));
    }

    let mut body = vec![0_u8; content_length];
    read_request_body(stream, &mut body).await?;

    Ok(HttpRequest {
        method: method.to_owned(),
        path: path.to_owned(),
        headers,
        body,
    })
}

async fn read_request_body(stream: &mut TcpStream, body: &mut [u8]) -> io::Result<()> {
    let mut offset = 0;
    while offset < body.len() {
        let read = stream.read(&mut body[offset..]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF while reading request body",
            ));
        }
        offset += read;
    }
    Ok(())
}

fn parse_headers(request: &str) -> Vec<(String, String)> {
    request
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect()
}

fn content_length(headers: &[(String, String)]) -> io::Result<usize> {
    let Some((_, value)) = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
    else {
        return Ok(0);
    };
    value
        .parse::<usize>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid content-length"))
}

/// Authenticate the HTTP request and return an `AuthContext`.
///
/// - No auth configured → admin access for local-only control.
/// - Valid token → permissions from the matching token entry.
/// - Invalid / missing token → empty permissions (handler gates will reject).
fn authenticate(request: &HttpRequest, auth: Option<&HttpServerAuth>) -> AuthContext {
    let Some(auth) = auth else {
        return AuthContext {
            subject: None,
            permissions: vec![
                Permission::Read,
                Permission::Control,
                Permission::Config,
                Permission::Admin,
            ],
        };
    };

    if auth.is_empty() {
        return AuthContext {
            subject: None,
            permissions: vec![
                Permission::Read,
                Permission::Control,
                Permission::Config,
                Permission::Admin,
            ],
        };
    }

    // Look for a Bearer token or X-Zero-Api-Key header.
    let presented = request.headers.iter().find_map(|(name, value)| {
        if name.eq_ignore_ascii_case("authorization") {
            value.strip_prefix("Bearer ").map(|t| t.to_owned())
        } else if name.eq_ignore_ascii_case("x-zero-api-key") {
            Some(value.clone())
        } else {
            None
        }
    });

    let Some(token) = presented else {
        return AuthContext {
            subject: None,
            permissions: vec![],
        };
    };

    // Constant-time comparison to avoid timing side-channels.
    for entry in &auth.tokens {
        let match_len = entry.key.len().min(token.len());
        let eq = {
            use std::cmp::Ordering;
            entry.key.as_bytes()[..match_len]
                .iter()
                .zip(token.as_bytes()[..match_len].iter())
                .fold(Ordering::Equal, |acc, (a, b)| match (acc, a.cmp(b)) {
                    (Ordering::Equal, Ordering::Equal) => Ordering::Equal,
                    _ => Ordering::Less,
                })
                == Ordering::Equal
                && entry.key.len() == token.len()
        };
        if eq {
            return AuthContext {
                subject: Some(entry.name.clone()),
                permissions: entry.permissions.clone(),
            };
        }
    }

    AuthContext {
        subject: None,
        permissions: vec![],
    }
}

async fn write_response(stream: &mut TcpStream, status_line: &str, body: &[u8]) -> io::Result<()> {
    write_response_with_headers(stream, status_line, body).await
}

async fn write_response_with_headers(
    stream: &mut TcpStream,
    status_and_extra_headers: &str,
    body: &[u8],
) -> io::Result<()> {
    let headers = format!(
        "{status_and_extra_headers}\
        Content-Type: application/json\r\n\
        Content-Length: {}\r\n\
        Access-Control-Allow-Origin: *\r\n\
        Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
        Access-Control-Allow-Headers: Authorization, Content-Type, X-Zero-Api-Key\r\n\
        Connection: close\r\n\
        \r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await
}

enum RateLimitCategory {
    Query,
    Command,
    Sse,
}

fn rate_limit_category(request: &HttpRequest) -> RateLimitCategory {
    let method = request.method.as_str();
    let path = &request.path;

    if path.contains("/events/stream") {
        return RateLimitCategory::Sse;
    }
    if method == "POST" || path.contains("/commands") {
        return RateLimitCategory::Command;
    }
    RateLimitCategory::Query
}

fn is_transient_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}
