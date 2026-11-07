//! The server Scope owns each actual Hyper connection and H2 stream future.
//! Tonic supplies the same API service; its detached transport runner is unused.
use super::*;
pub(super) mod connections;
use std::{future::Future, io, num::NonZeroUsize};
use tokio_util::sync::CancellationToken;
use tower_service::Service;
use zero_core::{request::Destination, Address};
use zero_platform_tokio::contract::{
    component::{AcceptedTcpLimits, ResourceDeclaration},
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
use zero_platform_tokio::{ControlledTcpListener, ControlledTcpListenerOwner};

pub(super) const MAX_CONNECTIONS: usize = 4096;
const MAX_STREAMS: u32 = 32;
const PER_CONNECTION_TASKS: usize = MAX_STREAMS as usize + 1;
pub(super) fn deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(5)
}
fn io_error(error: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(error)
}
use crate::messages::StreamExecutor;
#[derive(Clone)]
struct StopOnDrop(CancellationToken);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn cold(addr: SocketAddr) -> io::Result<(TaskScope, ControlledTcpListenerOwner)> {
    let one = NonZeroUsize::new(1).unwrap();
    let sockets = NonZeroUsize::new(MAX_CONNECTIONS).unwrap();
    let tasks = NonZeroUsize::new(PER_CONNECTION_TASKS).unwrap();
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(MAX_CONNECTIONS * PER_CONNECTION_TASKS).unwrap(),
    );
    let address = match addr.ip() {
        std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
        std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
    };
    let declaration = ResourceDeclaration::TcpListener {
        destination: Destination::new(address, addr.port()),
        max_bind_attempts: one,
        accepted: Some(AcceptedTcpLimits {
            max_sockets: sockets,
            max_tasks_per_socket: tasks,
            max_resources_per_socket: 0,
        }),
    };
    let original = ControlledTcpListenerOwner::new_with_accepted_graph(
        &declaration,
        one,
        CancellationToken::new(),
        sockets,
        tasks,
        &scope.registrar(),
    )
    .map_err(io::Error::other)?;
    Ok((scope, original))
}

pub(super) async fn run<H>(
    mut listener: ControlledTcpListener,
    handle: H,
    auth: Option<GrpcServerAuth>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    owner: Arc<GrpcOriginals>,
) -> io::Result<()>
where
    H: CommandService + QueryService + EventSource + Clone + Send + Sync + 'static,
{
    // The original-body observer enforces each admitted configured limit
    // before the pinned decoder allocates. Its default remains 4 MiB; this
    // schema ceiling only lets explicitly raised optional limits take effect.
    let svc = tonic::transport::Server::builder()
        .add_service(
            ControlServer::new(ControlService::new(
                handle,
                auth,
                owner.subscriptions.clone(),
            ))
            .max_decoding_message_size(256 * 1024 * 1024),
        )
        .into_service();
    let receiver = owner
        .original
        .lock()
        .await
        .accepted_task_receiver()
        .expect("actual gRPC accepted graph");
    let mut scope = owner.scope.lock().await;
    let mut failure = None;
    let work = catch_work(async {
        loop {
            tokio::select! { biased;
                exit = scope.next_exit() => {
                    if let Some(exit) = exit {
                        receiver.observe_joined(exit.id);
                        owner.requests.reap();
                        owner.connections.reap(&receiver).await?;
                        if let Err(error) = exit.result {
                            if error.reason == Reason::Internal && error.impact == Impact::Component { failure.get_or_insert_with(|| io::Error::other(error)); break; }
                            tracing::debug!(%error, "gRPC original task joined with failure");
                        }
                    }
                }
                _ = &mut shutdown => break,
                accepted = listener.accept_owned_with_admission(|| owner.connections.before(&owner.subscriptions.budget), || owner.connections.no_native()) => {
                    let (accepted, _peer) = match accepted {
                        Ok(value) => value,
                        Err(error) if error.reason == Reason::Capacity => { tokio::time::sleep(Duration::from_millis(25)).await; continue; }
                        Err(error) => return Err(io::Error::other(error)),
                    };
                    let claim = accepted.claim();
                    owner.connections.installed(claim.clone());
                    let connection = owner.connections.local_budget(&claim)?;
                    let requests = owner.requests.clone();
                    let svc = svc.clone();
                    let tls = tls.clone();
                    let stop = owner.stop.child_token();
                    accepted.spawn_handler_bound(TaskClass::Auxiliary, move |socket, tasks| async move {
                        let _stop = StopOnDrop(stop.clone());
                        let executor = StreamExecutor { tasks, stop: stop.clone(), connection, requests };
                        let service = hyper::service::service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                            let mut svc = svc.clone();
                            Box::pin(async move {
                                // Hyper constructs this service future in the
                                // connection, then polls it in its original
                                // accepted stream task. Capture that context
                                // here, never from the parent's task number.
                                let original = crate::messages::current().ok_or_else(||
                                    io::Error::other("original gRPC stream registration absent"))?;
                                let (mut parts, incoming) = request.into_parts();
                                let body = match crate::messages::Input::new(incoming, original.clone()) {
                                    Ok(body) => body,
                                    Err(status) => return Ok(status.into_http()),
                                };
                                parts.extensions.insert(original.clone());
                                let request = hyper::Request::from_parts(parts, tonic::body::boxed(body));
                                svc.call(request).await.map(|response| response.map(|body|
                                    tonic::body::boxed(crate::messages::Output::new(body, original)))).map_err(std::io::Error::other)
                            }) as std::pin::Pin<Box<dyn Future<Output = io::Result<hyper::Response<tonic::body::BoxBody>>> + Send + 'static>>
                        });
                        let mut builder = hyper::server::conn::http2::Builder::new(executor);
                        builder.max_concurrent_streams(MAX_STREAMS);
                        // A separate finite physical H2 buffer after adapter
                        // queue handoff; poll_next never claims a native write.
                        builder.max_send_buf_size(32 * 1024);
                        builder.timer(hyper_util::rt::TokioTimer::new());
                        if let Some(tls) = tls {
                            let stream = tokio::select! { _ = stop.cancelled() => return Ok(()), result = tls.accept(socket) => result.map_err(io_error)? };
                            tokio::select! { _ = stop.cancelled() => Ok(()), result = builder.serve_connection(hyper_util::rt::TokioIo::new(stream), service) => result.map_err(io_error) }
                        } else {
                            tokio::select! { _ = stop.cancelled() => Ok(()), result = builder.serve_connection(hyper_util::rt::TokioIo::new(socket), service) => result.map_err(io_error) }
                        }
                    }).map_err(io::Error::other)?;
                }
            }
        }
        io::Result::Ok(())
    }).await;
    owner.stop.cancel();
    owner.subscriptions.stop.store(true, Ordering::Release);
    scope.begin_shutdown();
    while let Some(exit) = scope.next_exit().await {
        receiver.observe_joined(exit.id);
        owner.requests.reap();
        if let Err(error) = exit.result {
            if error.impact == Impact::Component {
                failure.get_or_insert_with(|| io::Error::other(error));
            }
        }
    }
    owner.requests.reap();
    // The listener view outlives original protocol work; graph cancellation
    // cannot drop its retained subscription cleanup futures prematurely.
    drop(listener);
    let jobs = owner.subscriptions.join_all().await;
    let native = owner
        .original
        .lock()
        .await
        .wait_closed(deadline())
        .await
        .map_err(io::Error::other);
    let result = match work {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    };
    result?;
    jobs?;
    if !native?.is_complete() {
        return Err(io::Error::other(
            "gRPC actual original closure remains incomplete",
        ));
    }
    owner.connections.confirm_all_closed();
    failure.map_or(Ok(()), Err)
}
pub(super) async fn catch_work<F: Future>(
    work: F,
) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Err(payload) => std::task::Poll::Ready(Err(payload)),
        }
    })
    .await
}
