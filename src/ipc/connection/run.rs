//! Existing connection work and actual worker/command cleanup share one owner.
use super::*;
pub(super) async fn handle_ipc_connection_with_auth_until<S>(
    stream: S,
    handle: ProxyHandle,
    auth_ctx: AuthContext,
    stop: CancellationToken,
    budget: Arc<ControlBudget>,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(TokioMutex::new(BufWriter::new(writer)));

    let mut subscribed = false;
    let mut subscription_charge: Option<ControlBudgetPermit> = None;
    let mut raw_line = Vec::new();
    let mut bg_blocking: Option<tokio::task::JoinHandle<()>> = None;
    let mut bg_events: Option<tokio::task::JoinHandle<io::Result<()>>> = None;
    let mut concurrent_commands = ConcurrentCommandTasks::new(stop.clone());
    // Every error/cancellation exits this work future into the same retained
    // cleanup path. The original job handles live outside that future.
    let work = async {
        loop {
            raw_line.clear();
            let n = reader.read_until(b'\n', &mut raw_line).await?;
            if n == 0 {
                break;
            }
            let line = input::text(&raw_line)?;
            if line.trim().is_empty() {
                continue;
            }

            let request: IpcRequest = match serde_json::from_str(line.trim()) {
                Ok(req) => req,
                Err(_) => {
                    let resp = ipc_error(None, "invalid_argument", "invalid request frame");
                    write_ipc_response(&mut *writer.lock().await, &resp).await?;
                    continue;
                }
            };

            match request {
                IpcRequest::Ping { id } => {
                    let resp = ipc_ok(id, "pong");
                    write_ipc_response(&mut *writer.lock().await, &resp).await?;
                }
                IpcRequest::Query { id, request } => match handle.query(request) {
                    Ok(query_resp) => {
                        let value = serde_json::to_value(query_resp).map_err(io::Error::other)?;
                        let resp = ipc_ok(id, value);
                        write_ipc_response(&mut *writer.lock().await, &resp).await?;
                    }
                    Err(error) => {
                        let resp = ipc_api_error(id, &error);
                        write_ipc_response(&mut *writer.lock().await, &resp).await?;
                    }
                },
                IpcRequest::Command { id, method, params } => {
                    let command = parse_command(&method, &params);
                    match command {
                        Ok(ref cmd) if !auth_ctx.allows(cmd.required_permission()) => {
                            let error =
                                zero_api::ApiError::permission_denied(cmd.required_permission());
                            let resp = ipc_api_error(id, &error);
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        }
                        Ok(cmd) => {
                            if command_can_run_concurrently(&cmd) {
                                let command_handle = handle.clone();
                                let response_writer = writer.clone();
                                let rejected_id = id.clone();
                                let spawned = concurrent_commands.try_spawn(async move {
                                    let result = execute_ipc_command(command_handle, cmd).await;
                                    if let Err(error) =
                                        write_command_result(response_writer, id, result).await
                                    {
                                        if !is_transient_disconnect(&error) {
                                            return Err(error);
                                        }
                                    }
                                    Ok(())
                                });
                                if !spawned {
                                    let error = zero_api::ApiError::new(
                                    zero_api::ApiErrorCode::Conflict,
                                    format!(
                                        "this IPC connection already has {MAX_PENDING_CONCURRENT_IPC_COMMANDS} pending outbound probes"
                                    ),
                                );
                                    write_command_result(writer.clone(), rejected_id, Err(error))
                                        .await?;
                                }
                            } else {
                                let result = execute_ipc_command(handle.clone(), cmd).await;
                                write_command_result(writer.clone(), id, result).await?;
                            }
                        }
                        Err(error) => {
                            let resp = error.with_id(id);
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        }
                    }
                }
                IpcRequest::Subscribe { id, events } => {
                    if subscribed {
                        let resp = ipc_error(
                            id,
                            "already_subscribed",
                            "this connection is already subscribed to events",
                        );
                        write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        continue;
                    }

                    let charge = match budget.reserve(ControlCharge::Subscription) {
                        Ok(charge) => charge,
                        Err(error) => {
                            let resp = ipc_error(id, "resource_exhausted", error.to_string());
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                            continue;
                        }
                    };
                    let mut filter = EventFilter::default();
                    if let Some(types) = events {
                        filter.event_types = types;
                    }

                    // Ack only after the subscription actually succeeds, so the
                    // request `id` is moved exactly once (no clone) and
                    // "subscribed" truthfully reflects engine state.
                    match handle.subscribe(filter) {
                        Ok(subscriber) => {
                            subscribed = true;
                            subscription_charge = Some(charge);
                            let resp = ipc_ok(id, "subscribed");
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                            // Tokio (async) channel so the writer task can
                            // `recv().await` without blocking a worker thread, and so
                            // aborting it cancels at the await point immediately.
                            let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(32);

                            let cancel_for_task = stop.clone();
                            let event_budget = budget.clone();

                            // Drain ready events without throttling the subscription.
                            // Only idle polls sleep; the bounded bridge applies backpressure
                            // until the writer consumes events or drops its receiver.
                            bg_blocking = Some(tokio::task::spawn_blocking(move || {
                                loop {
                                    if cancel_for_task.is_cancelled() || event_tx.is_closed() {
                                        break;
                                    }
                                    if let Some(event) = subscriber.try_recv() {
                                        // Serialize the full ApiEvent envelope (same format
                                        // as SSE) so consumers use one parsing code path
                                        // for both channels.
                                        let bytes = super::super::owner::serialized_len(&event)
                                            .expect("event envelope is serializable")
                                            + 1;
                                        let Some(charge) =
                                            super::super::owner::reserve_event_bytes_blocking(
                                                &event_budget,
                                                bytes,
                                                &cancel_for_task,
                                                || event_tx.is_closed(),
                                            )
                                        else {
                                            break;
                                        };
                                        let frame = serialize_frame(&event)
                                            .expect("measured event envelope serialization");
                                        if event_tx.blocking_send((frame, charge)).is_err() {
                                            break;
                                        }
                                        continue;
                                    }
                                    std::thread::sleep(Duration::from_millis(25));
                                }
                            }));

                            // Writer task: pushes events and 30s heartbeats to the
                            // client. Runs concurrently with the main read loop so
                            // query/command/ping frames keep working after subscribe.
                            let event_writer = writer.clone();
                            let event_stop = stop.clone();
                            bg_events = Some(tokio::spawn(async move {
                                let work = async {
                                    loop {
                                        match tokio::time::timeout(
                                            Duration::from_secs(30),
                                            event_rx.recv(),
                                        )
                                        .await
                                        {
                                            Ok(Some((frame, _charge))) => {
                                                let mut w = event_writer.lock().await;
                                                w.write_all(&frame).await?;
                                                w.flush().await?;
                                            }
                                            Ok(None) => break, // sender dropped
                                            Err(_) => {
                                                // heartbeat (SSE comment line; clients ignore)
                                                let mut w = event_writer.lock().await;
                                                w.write_all(b":\n").await?;
                                                w.flush().await?;
                                            }
                                        }
                                    }
                                    io::Result::Ok(())
                                };
                                let result = tokio::select! {
                                    _ = event_stop.cancelled() => Ok(()),
                                    result = work => result,
                                };
                                event_stop.cancel();
                                result
                            }));
                        }
                        Err(error) => {
                            let resp = ipc_api_error(id, &error);
                            write_ipc_response(&mut *writer.lock().await, &resp).await?;
                        }
                    }
                }
            }
        }

        io::Result::Ok(())
    };
    let result = super::super::owner::catch_work(async {
        tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = work => result,
        }
    })
    .await;
    stop.cancel();
    // The event writer drops its actual receiver, waking a full blocking
    // bridge. Join both originals even when an earlier read/write failed.
    let events = match bg_events.take() {
        Some(task) => match task.await {
            Ok(Err(error)) if is_transient_disconnect(&error) => {
                tracing::debug!(%error, "IPC event writer observed peer disconnect");
                Ok(())
            }
            Ok(result) => result,
            Err(error) => Err(io::Error::other(error)),
        },
        None => Ok(()),
    };
    let bridge = match bg_blocking.take() {
        Some(task) => task.await.map_err(io::Error::other),
        None => Ok(()),
    };
    let commands = concurrent_commands.cancel_all().await;
    // Actual queue/writer/worker custody has returned before this capacity.
    drop(subscription_charge);
    super::super::owner::finish_caught(result, [events, bridge, commands])
}
