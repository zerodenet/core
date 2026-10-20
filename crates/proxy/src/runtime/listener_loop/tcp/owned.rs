//! Actual registered handlers share the original listener's bounded entry table.
use super::connection::TcpListenerLoopRequest;
use crate::runtime::route_runtime::InboundRouteRuntime;
use futures_util::{stream::FuturesUnordered, StreamExt};
use std::future::Future;
use tracing::info;
use zero_engine::EngineError;
use zero_platform_tokio::contract::{lifecycle::TaskClass, ChannelError, Impact, Reason, Stage};

/// An explicitly audited single-task body can be tested without claiming that
/// arbitrary protocol transports/multiplexers have already migrated their users.
#[derive(Clone, Copy)]
pub(crate) enum UsersPlan {
    PrimaryOnly,
    Unaccounted,
}
pub(crate) struct OwnedLoop<H> {
    pub request: TcpListenerLoopRequest<H>,
    pub users: UsersPlan,
}
pub(crate) async fn run<H, Fut>(input: OwnedLoop<H>) -> Result<(), EngineError>
where
    H: Fn(InboundRouteRuntime, zero_platform_tokio::TokioSocket) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    let TcpListenerLoopRequest {
        runtime_factory,
        protocol_name,
        listener,
        mut shutdown,
        handler,
    } = input.request;
    let receiver = listener
        .accepted_task_receiver()
        .ok_or_else(|| std::io::Error::other("accepted graph is missing"))?;
    let limit = receiver
        .limit()
        .ok_or_else(|| std::io::Error::other("accepted owner has disappeared"))?;
    let mut closing = FuturesUnordered::new();
    #[cfg(feature = "udp-runtime")]
    let udp_stop = crate::runtime::route_runtime::ListenerStop::default();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! { biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            result = closing.next(), if !closing.is_empty() => {
                if let Some(result) = result { result?; }
            }
            accepted = listener.accept_owned(), if closing.len() < limit.get() => {
                let (accepted, remote) = accepted.map_err(std::io::Error::other)?;
                let claim = accepted.claim();
                let runtime = runtime_factory.for_connection(remote.map(zero_platform_tokio::socket_address_to_socket_addr));
                #[cfg(feature = "udp-runtime")]
                let runtime = runtime.with_listener_stop(udp_stop.clone());
                let handler = handler.clone();
                let users = input.users;
                let (_, witness) = accepted.spawn_handler_bound(TaskClass::Auxiliary, move |socket, tasks| async move {
                    let caller = tasks.caller()?;
                    if matches!(users, UsersPlan::Unaccounted) { caller.mark_unaccounted(); }
                    // Real task identity is installed before this first poll.
                    let runtime = runtime.with_local_addr(socket.local_addr().ok()).with_accepted_caller(caller);
                    handler(runtime, socket).await.map_err(|source| {
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
                    })
                }).map_err(std::io::Error::other)?;
                let receiver = receiver.clone();
                closing.push(async move {
                    let _joined = witness.wait().await;
                    // Existing inbound-owner cleanup grace. This is not a new
                    // task/socket budget or a task-exit/FD-close substitute.
                    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                    if receiver.close(&claim, deadline).await.map_err(std::io::Error::other)?.is_none() {
                        return Err(EngineError::Io(std::io::Error::other("accepted users remain unconfirmed")));
                    }
                    Ok(())
                });
            }
        }
    }
    #[cfg(feature = "udp-runtime")]
    {
        udp_stop.begin_shutdown();
        udp_stop.wait_quiescent().await;
    }
    // Original registry resource and Scope keep every entry/task if this outer
    // future is cancelled. Their structured shutdown drives both concurrently.
    info!(inbound_tag = %runtime_factory.inbound_tag(), protocol = protocol_name,
        transport = "tcp", "owned inbound listener stopped");
    Ok(())
}
