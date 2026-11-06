//! One original server drains every actual accepted join before returning faults.
use super::*;
pub(super) async fn run_ipc_server(
    mut listener: ControlledUnixListener,
    handle: ProxyHandle,
    mut shutdown: oneshot::Receiver<()>,
    originals: Arc<UnixOriginals>,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    let stop = CancellationToken::new();
    let mut failures = Vec::new();
    let active = Arc::new(AtomicU64::new(0));

    let work = super::super::owner::catch_work(async {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accept_result = listener.accept_with_admission(|| originals.before(&budget), || originals.no_native()) => {
                let (mut pending, control) = match accept_result {
                    Ok(value) => value,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        continue;
                    }
                    Err(error) => { failures.push(error); break; }
                };
                // Preserve both the real accepted original and its pre-native
                // charge before activation can suspend or fail.
                let charge = originals.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take().expect("IPC original charged before native accept");
                *originals.activating.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(UnixOriginal { control: tokio::sync::Mutex::new(control), charge: std::sync::Mutex::new(Some(charge)) }));
                let stream = match pending.activate(super::super::owner::close_deadline()).await {
                    Ok(stream) => stream,
                    Err(error) => { failures.push(error); break; }
                };
                drop(pending);
                let peer_addr = "unix";
                let handle = handle.clone();
                let active = active.clone();
                let pipe = format!("{:?}", peer_addr);
                active.fetch_add(1, Ordering::Relaxed);
                super::super::events::emit_connected(&handle, active.load(Ordering::Relaxed), &pipe);
                debug!(?peer_addr, active = active.load(Ordering::Relaxed),
                      "ipc client connected");

                let connection_stop = stop.child_token();
                let connection_budget = budget.clone();
                let task = connections.spawn(async move {
                    let emit_handle = handle.clone();
                    let result = connection::handle_ipc_connection_with_budget_until(stream, handle, connection_stop, connection_budget).await;

                    let n = active.fetch_sub(1, Ordering::Relaxed) - 1;
                    let error_str = match &result {
                        Ok(_) => None,
                        Err(ref e) if connection::is_transient_disconnect(e) => Some(e.to_string()),
                        Err(ref e) => Some(e.to_string()),
                    };
                    super::super::events::emit_disconnected(&emit_handle, n, &pipe, error_str.as_deref());
                    match result {
                        Ok(()) => {
                            debug!(?peer_addr, active = n, "ipc client disconnected cleanly");
                            Ok(())
                        }
                        Err(ref error) if connection::is_transient_disconnect(error) || connection::is_invalid_input_frame(error) => {
                            warn!(?peer_addr, error = %error, active = n,
                                  "ipc client disconnected");
                            Ok(())
                        }
                        Err(error) => {
                            warn!(?peer_addr, error = %error, active = n,
                                  "ipc connection failed");
                            Err(error)
                        }
                    }
                });
                let original = originals.activating.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take().expect("accepted original installed before activation");
                originals.active.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(task.id(), original);
            }
            result = connections.join_next_with_id(), if !connections.is_empty() => {
                if let Some(result) = result {
                    settle_connection(result, &originals, &mut failures).await;
                    if !failures.is_empty() { break; }
                }
            }
        }
    }

    }).await;
    stop.cancel();
    drop(listener);
    while let Some(result) = connections.join_next_with_id().await {
        settle_connection(result, &originals, &mut failures).await;
    }
    if let Err(error) = originals.close_all().await {
        failures.push(error);
    }
    if let Err(payload) = work {
        originals.retain_terminal_faults(failures);
        std::panic::resume_unwind(payload);
    }
    info!("ipc server stopped");
    super::super::owner::finish_work(failures)
}

async fn settle_connection(
    result: Result<(tokio::task::Id, io::Result<()>), tokio::task::JoinError>,
    originals: &UnixOriginals,
    failures: &mut Vec<io::Error>,
) {
    let id = match result {
        Ok((id, result)) => {
            if let Err(error) = result {
                failures.push(error);
            }
            id
        }
        Err(error) => {
            let id = error.id();
            failures.push(io::Error::other(error));
            id
        }
    };
    let original = originals
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&id)
        .cloned();
    if let Some(original) = original {
        match UnixOriginals::close(&original).await {
            Ok(()) => {
                originals
                    .active
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
            }
            Err(error) => {
                failures.push(error);
            }
        }
    }
}

#[cfg(test)]
mod tests;
