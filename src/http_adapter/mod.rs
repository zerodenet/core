mod connections;
pub mod handlers;
pub mod ratelimit;
pub mod response;
pub mod router;
pub mod sse;

use std::io;
use std::time::Duration;

use std::{num::NonZeroUsize, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};
use zero_api::{AuthContext, Permission};
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::CancellationToken;
use zero_platform_tokio::contract::{
    component::{AcceptedTcpLimits, ResourceDeclaration},
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
use zero_platform_tokio::{AcceptedTcpClaim, ControlledTcpListener, ControlledTcpListenerOwner};
use zero_proxy::ProxyHandle;

use router::{HttpRequest, RouteResult};

const MAX_REQUEST_SIZE: usize = 8192;
const MAX_BODY_SIZE: usize = 256 * 1024;
const REQUEST_END: &[u8] = b"\r\n\r\n";

pub struct HttpServerHandle {
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    task: crate::ipc::owner::ControlTask,
    original: Arc<tokio::sync::Mutex<Option<ControlledTcpListenerOwner>>>,
    scope: Arc<tokio::sync::Mutex<TaskScope>>,
    listen: String,
    auth: Option<HttpServerAuth>,
    started: std::sync::atomic::AtomicBool,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    connections: Arc<connections::Connections>,
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
    pub(crate) async fn wait_terminated(&self) -> io::Result<()> {
        self.task.join().await
    }
    pub async fn shutdown(&self) -> io::Result<()> {
        if let Some(shutdown) = self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = shutdown.send(());
        }
        let server = self.task.join().await;
        let tasks = self
            .scope
            .lock()
            .await
            .shutdown(crate::ipc::owner::close_deadline())
            .await;
        let mut original = self.original.lock().await;
        let native_complete = match original.as_mut() {
            Some(original) => original
                .wait_closed(crate::ipc::owner::close_deadline())
                .await
                .map_err(io::Error::other)?
                .is_complete(),
            None => true,
        };
        if !tasks.is_complete() || !native_complete {
            return Err(io::Error::other(
                "HTTP original tasks or native IO remain retained",
            ));
        }
        self.connections.confirm_all_closed();
        server
    }
}

pub fn prepare_http_server(listen: &str, auth: Option<HttpServerAuth>) -> Arc<HttpServerHandle> {
    prepare_http_server_with_budget(
        listen,
        auth,
        zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                &zero_config::RuntimeOptionsConfig::default(),
            ),
        ),
    )
}
pub fn prepare_http_server_with_budget(
    listen: &str,
    auth: Option<HttpServerAuth>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> Arc<HttpServerHandle> {
    // The finite validated ceiling allocates no idle socket/task. The shared
    // actual root owner admits occupancy under the current configured cap.
    let sockets = 4096;
    Arc::new(HttpServerHandle {
        shutdown: std::sync::Mutex::new(None),
        task: crate::ipc::owner::ControlTask::empty(),
        original: Arc::new(tokio::sync::Mutex::new(None)),
        scope: Arc::new(tokio::sync::Mutex::new(TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(sockets * 3 + 1).unwrap(),
        ))),
        listen: listen.to_owned(),
        auth,
        started: std::sync::atomic::AtomicBool::new(false),
        budget,
        connections: Arc::new(connections::Connections::default()),
    })
}
impl HttpServerHandle {
    pub(crate) async fn start(&self, proxy_handle: ProxyHandle) -> io::Result<()> {
        if self.started.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Err(io::Error::other("HTTP original owner already started"));
        }
        let sockets = NonZeroUsize::new(4096).unwrap();
        let per_entry = NonZeroUsize::new(3).unwrap();
        let mut scope = self.scope.lock().await;
        let listener =
            bind_control_listener(&self.listen, &mut scope, sockets, per_entry, &self.original)
                .await?;
        drop(scope);
        let local_addr = listener.local_addr().map_err(io::Error::other)?;
        let receiver = self
            .original
            .lock()
            .await
            .as_ref()
            .expect("cold HTTP listener installed before bind")
            .accepted_task_receiver()
            .expect("declared HTTP accepted graph");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(shutdown_tx);
        let task_scope = self.scope.clone();
        let task_original = self.original.clone();
        let auth = self.auth.clone();
        let budget = self.budget.clone();
        let connections = self.connections.clone();
        self.task.install(tokio::spawn(async move {
            run_server(
                listener,
                proxy_handle,
                auth,
                shutdown_rx,
                task_scope,
                task_original,
                receiver,
                budget,
                connections,
            )
            .await
        }))?;
        info!(listen = %local_addr, "http api server ready");
        Ok(())
    }
}
pub async fn spawn_http_server(
    proxy_handle: ProxyHandle,
    listen: &str,
    auth: Option<HttpServerAuth>,
) -> io::Result<Arc<HttpServerHandle>> {
    let owner = prepare_http_server(listen, auth);
    owner.start(proxy_handle).await?;
    Ok(owner)
}

fn component_io(error: io::Error) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(error)
}

async fn bind_control_listener(
    listen: &str,
    scope: &mut TaskScope,
    sockets: NonZeroUsize,
    per_entry: NonZeroUsize,
    slot: &tokio::sync::Mutex<Option<ControlledTcpListenerOwner>>,
) -> io::Result<ControlledTcpListener> {
    let attempts = NonZeroUsize::new(16).unwrap();
    let limits = AcceptedTcpLimits {
        max_sockets: sockets,
        max_tasks_per_socket: per_entry,
        max_resources_per_socket: 0,
    };
    let literal = listen.parse::<std::net::SocketAddr>();
    let (destination, addresses) = if let Ok(address) = literal {
        let host = match address.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        };
        (Destination::new(host, address.port()), vec![address])
    } else {
        let (host, port) = listen.rsplit_once(':').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTP listen requires host:port",
            )
        })?;
        let port = port
            .parse::<u16>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let requested = listen.to_owned();
        let material = Arc::new(std::sync::Mutex::new(None));
        let result = material.clone();
        scope
            .registrar()
            .spawn_blocking_tracked(TaskClass::Auxiliary, move || {
                use std::net::ToSocketAddrs;
                let addresses = requested
                    .to_socket_addrs()
                    .map(|addresses| addresses.take(17).collect::<Vec<_>>());
                *result.lock().unwrap() = Some(addresses);
                Ok(())
            })
            .map_err(io::Error::other)?;
        let exit = scope
            .next_exit()
            .await
            .expect("actual resolver registration");
        exit.result.map_err(io::Error::other)?;
        let addresses = material
            .lock()
            .unwrap()
            .take()
            .expect("actual completed resolver result")?;
        if addresses.is_empty() || addresses.len() > attempts.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTP listener address set exceeds its declared attempts",
            ));
        }
        (
            Destination::new(Address::Domain(host.to_owned()), port),
            addresses,
        )
    };
    let declaration = ResourceDeclaration::TcpListener {
        destination,
        accepted: Some(limits),
        max_bind_attempts: attempts,
    };
    let mut owner = if literal.is_ok() {
        ControlledTcpListenerOwner::new_with_accepted_graph(
            &declaration,
            attempts,
            CancellationToken::new(),
            sockets,
            per_entry,
            &scope.registrar(),
        )
    } else {
        ControlledTcpListenerOwner::new_resolving_with_accepted_graph(
            &declaration,
            attempts,
            CancellationToken::new(),
            sockets,
            per_entry,
            &scope.registrar(),
        )
    }
    .map_err(io::Error::other)?;
    if literal.is_err() {
        owner
            .install_resolved_addresses(addresses.clone().into())
            .map_err(io::Error::other)?;
    }
    let mut slot = slot.lock().await;
    *slot = Some(owner);
    let owner = slot
        .as_mut()
        .expect("cold native owner installed before bind");
    let mut failure = None;
    for address in addresses {
        match owner.bind_listener(address) {
            Ok(listener) => return Ok(listener),
            Err(error) => {
                failure = Some(error);
                if owner.native_unconfirmed() {
                    break;
                }
            }
        }
    }
    let close = owner
        .wait_closed(crate::ipc::owner::close_deadline())
        .await
        .map_err(io::Error::other)?;
    if !close.is_complete() {
        return Err(io::Error::other("failed HTTP bind retains an original"));
    }
    Err(io::Error::other(
        failure.expect("nonempty frozen bind addresses"),
    ))
}

async fn run_server(
    mut listener: ControlledTcpListener,
    handle: ProxyHandle,
    auth: Option<HttpServerAuth>,
    mut shutdown: oneshot::Receiver<()>,
    scope: Arc<tokio::sync::Mutex<TaskScope>>,
    original: Arc<tokio::sync::Mutex<Option<ControlledTcpListenerOwner>>>,
    receiver: zero_platform_tokio::AcceptedTcpTaskReceiver,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    connections: Arc<connections::Connections>,
) -> io::Result<()> {
    let limiters = Arc::new(ratelimit::ApiRateLimiters::default());
    let stop = CancellationToken::new();
    let mut failure = None;
    let mut scope = scope.lock().await;
    let work = crate::ipc::owner::catch_work(async {
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accept_result = listener.accept_owned_with_admission(|| connections.before(&budget), || connections.no_native()) => {
                    let (accepted, remote_addr) = match accept_result {
                        Ok(value) => value,
                        Err(error) if error.reason == Reason::Capacity => {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            continue;
                        }
                        Err(error) => { failure.get_or_insert_with(|| io::Error::other(error)); break; }
                    };
                    connections.installed(accepted.claim());
                    let handle = handle.clone();
                    let auth = auth.clone();
                    let limiters = limiters.clone();
                    let connection_stop = stop.child_token();
                    let connection_budget = budget.clone();
                    match accepted.spawn_handler_bound(TaskClass::Auxiliary, move |stream, _tasks| async move {
                        if let Err(error) = serve_connection(stream, handle, auth.as_ref(), &limiters, connection_stop, connection_budget).await {
                            if is_transient_disconnect(&error) { debug!(?remote_addr, %error, "http connection closed early"); }
                            else { warn!(?remote_addr, %error, "http connection failed"); }
                            return Err(component_io(error));
                        }
                        Ok(())
                    }) {
                        Ok(_) => {}
                        Err(error) => { failure.get_or_insert_with(|| io::Error::other(error)); break; }
                    }
                }
                exit = scope.next_exit() => {
                    if let Some(exit) = exit {
                        receiver.observe_joined(exit.id);
                        connections.reap(&receiver).await?;
                        if let Err(error) = exit.result {
                            // Keep the actual original task failure in the accepted
                            // owner; client IO errors remain local to that session.
                            if error.impact == Impact::Component && error.reason == Reason::Internal {
                                failure.get_or_insert_with(|| io::Error::other(error));
                                break;
                            }
                            debug!(%error, "HTTP original handler joined with failure");
                        }
                    }
                }
            }
        }
        io::Result::Ok(())
    }).await;
    stop.cancel();
    // Keep the original listener view alive while handlers finish their own
    // subscription joins. Revoking its graph now would drop those futures.
    scope.begin_shutdown();
    while let Some(exit) = scope.next_exit().await {
        receiver.observe_joined(exit.id);
    }
    drop(listener);
    let native = original
        .lock()
        .await
        .as_mut()
        .expect("installed actual HTTP native owner")
        .wait_closed(crate::ipc::owner::close_deadline())
        .await
        .map_err(io::Error::other);
    let work = match work {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    work?;
    if !native?.is_complete() {
        return Err(io::Error::other("HTTP original closure incomplete"));
    }
    connections.confirm_all_closed();
    info!("http api server stopped");
    failure.map_or(Ok(()), Err)
}

async fn serve_connection(
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    handle: ProxyHandle,
    auth: Option<&HttpServerAuth>,
    limiters: &ratelimit::ApiRateLimiters,
    stop: CancellationToken,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> io::Result<()> {
    let request = tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        result = read_request(&mut stream) => result?,
    };

    let auth_ctx = authenticate(&request, auth);
    if auth_ctx.permissions.is_empty() {
        return tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = write_response(&mut stream, "HTTP/1.1 401 Unauthorized\r\n", br#"{"error":"unauthorized"}"#) => result,
        };
    }

    // Rate limiting.
    let category = rate_limit_category(&request);
    let allowed = match category {
        RateLimitCategory::Query => limiters.query.allow(),
        RateLimitCategory::Command => limiters.command.allow(),
        RateLimitCategory::Sse => limiters.sse_connections.allow(),
    };
    if !allowed {
        return tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = write_response_with_headers(&mut stream, "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\n", br#"{"error":"rate limit exceeded"}"#) => result,
        };
    }

    let _subscription_charge = if matches!(category, RateLimitCategory::Sse) {
        match budget.reserve(zero_runtime::configuration::ControlCharge::Subscription) {
            Ok(charge) => Some(charge),
            Err(_) => {
                return tokio::select! { _ = stop.cancelled() => Ok(()), result = write_response(&mut stream, "HTTP/1.1 503 Service Unavailable\r\n", br#"{"error":"subscription capacity exhausted"}"#) => result }
            }
        }
    } else {
        None
    };
    let route = tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        result = router::route(&request, &handle, &auth_ctx) => result,
    };
    match route {
        RouteResult::Respond(status, body) => tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = write_response(&mut stream, &status, &body) => result,
        },
        RouteResult::Sse {
            subscriber,
            catch_up,
        } => {
            sse::run_sse_stream_with_budget(
                stream,
                subscriber,
                catch_up,
                Duration::from_secs(30),
                stop.cancelled_owned(),
                budget,
            )
            .await
        }
    }
}

async fn read_request(
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
) -> io::Result<HttpRequest> {
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

async fn read_request_body(
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    body: &mut [u8],
) -> io::Result<()> {
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

async fn write_response(
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    status_line: &str,
    body: &[u8],
) -> io::Result<()> {
    write_response_with_headers(stream, status_line, body).await
}

async fn write_response_with_headers(
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
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
