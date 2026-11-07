//! One original observer retains every actual accepted connection through joins.
use super::connection::serve_connection;
use super::*;
pub(super) struct ServerResources {
    pub(super) scope: Arc<tokio::sync::Mutex<TaskScope>>,
    pub(super) original: Arc<tokio::sync::Mutex<Option<ControlledTcpListenerOwner>>>,
    pub(super) receiver: zero_platform_tokio::AcceptedTcpTaskReceiver,
    pub(super) budget: Arc<zero_runtime::configuration::ControlBudget>,
    pub(super) connections: Arc<connections::Connections>,
}
pub(super) async fn run_server(
    mut listener: ControlledTcpListener,
    handle: ProxyHandle,
    auth: Option<Arc<HttpServerAuth>>,
    mut shutdown: oneshot::Receiver<()>,
    resources: ServerResources,
) -> io::Result<()> {
    let ServerResources {
        scope,
        original,
        receiver,
        budget,
        connections,
    } = resources;
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
                    let connection_original=connections.installed(accepted.claim());
                    let handle = handle.clone();
                    let auth = auth.clone();
                    let limiters = limiters.clone();
                    let connection_stop = stop.child_token();
                    let connection_budget = budget.clone();
                    match accepted.spawn_handler_bound(TaskClass::Auxiliary, move |stream, _tasks| async move {
                        if let Err(error) = serve_connection(stream, handle, auth.as_deref(), &limiters, connection_stop, connection_budget,connection_original).await {
                            if input::client_frame(&error) { debug!(?remote_addr,%error,"HTTP client frame rejected by its actual owner");return Ok(()); }
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
        if let Err(error) = exit.result {
            if error.impact == Impact::Component {
                failure.get_or_insert_with(|| io::Error::other(error));
            }
        }
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
    let native =
        native.and_then(|report| crate::ipc::owner::confirmed_report("HTTP original", report));
    if native.is_ok() {
        connections.confirm_all_closed();
    }
    info!("http api server cleanup driven");
    crate::ipc::owner::finish_caught(work, [native, failure.map_or(Ok(()), Err)])
}
