//! Existing connection work and actual worker/command cleanup share one owner.
use super::*;
pub(super) async fn handle_ipc_connection_with_auth_until<S>(
    stream: S,
    handle: ProxyHandle,
    auth_ctx: AuthContext,
    stop: CancellationToken,
    budget: Arc<ControlBudget>,
    local: zero_runtime::configuration::ControlConnectionBudget,
) -> io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    // These original buffering allocations can retain bytes after a cancelled
    // flush or a partial read. Their full capacity stays in the same byte book
    // until all actual buffer/worker owners have returned, even while idle.
    let messages = budget.limits().messages;
    let read_capacity = (messages.request_bytes_total / 4).clamp(1, 8192);
    let write_capacity = (messages.response_bytes_total / 4).clamp(1, 8192);
    let _read_workspace = budget
        .reserve(ControlCharge::RequestBytes(read_capacity))
        .map_err(input::budget)?;
    let _write_workspace = budget
        .reserve(ControlCharge::ResponseBytes(write_capacity))
        .map_err(input::budget)?;
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::with_capacity(read_capacity, reader);
    let writer = Arc::new(TokioMutex::new(BufWriter::with_capacity(
        write_capacity,
        writer,
    )));

    let mut subscribed = false;
    let mut subscription_charge: Option<ControlBudgetPermit> = None;
    let mut event_jobs = super::events::EventJobs::new();
    let mut concurrent_commands =
        ConcurrentCommandTasks::with_budget(stop.clone(), budget.clone(), local.clone());
    // Every error/cancellation exits this work future into the same retained
    // cleanup path. The original job handles live outside that future.
    let mut inline_command: Option<commands::CommandCredits> = None;
    let work = async {
        loop {
            let next = tokio::select! {biased;_=stop.cancelled()=>break,result=frames::next_request(&mut reader,&budget,&mut concurrent_commands)=>result?};
            let Some(raw_line) = next else {
                break;
            };
            let line = input::text(raw_line.bytes())?;
            if line.trim().is_empty() {
                continue;
            }

            let mut request_storage = None;
            let request = match super::super::owner::decode_ipc(line.trim(), &budget) {
                Ok(decoded) => {
                    let (request, storage) = decoded.into_parts();
                    request_storage = Some(storage);
                    request
                }
                Err(error) => {
                    if let Some(error) = error.into_capacity() {
                        return Err(input::budget(error));
                    }
                    let resp = ipc_error(None, "invalid_argument", "invalid request frame");
                    frames::write_response_until(&writer, &resp, &budget, &stop).await?;
                    continue;
                }
            };

            drop(raw_line);
            match request {
                IpcRequest::Ping { id } => {
                    let resp = ipc_ok(id, "pong");
                    frames::write_response_until(&writer, &resp, &budget, &stop).await?;
                }
                IpcRequest::Query { id, request } => {
                    if query::handles(&request) {
                        let response = query::prepare(&handle, &request, id, &budget)?;
                        tokio::select! {biased;_=stop.cancelled()=>{},result=async {
                            let mut writer=writer.lock().await;
                            writer.write_all(response.bytes()).await?;
                            writer.flush().await
                        }=>result?}
                    } else {
                        match handle.query(request) {
                            Ok(query_resp) => {
                                let resp = ApiResponse::ok_with_id(id, query_resp);
                                frames::write_response_until(&writer, &resp, &budget, &stop)
                                    .await?;
                            }
                            Err(error) => {
                                let resp = super::super::owner::BorrowedErrorResponse::new(
                                    id.as_ref(),
                                    &error,
                                );
                                frames::write_response_until(&writer, &resp, &budget, &stop)
                                    .await?;
                            }
                        }
                    }
                }
                IpcRequest::Command { id, method, params } => {
                    super::super::owner::request_json::grow_command(
                        &method,
                        &params,
                        request_storage.as_mut().unwrap(),
                        &budget,
                    )
                    .map_err(input::budget)?;
                    let command = parse_owned_command(method, params);
                    match command {
                        Ok(ref cmd) if !auth_ctx.allows(cmd.required_permission()) => {
                            let error =
                                zero_api::ApiError::permission_denied(cmd.required_permission());
                            let resp = super::super::owner::BorrowedErrorResponse::new(
                                id.as_ref(),
                                &error,
                            );
                            frames::write_response_until(&writer, &resp, &budget, &stop).await?;
                        }
                        Ok(cmd) => {
                            if command_can_run_concurrently(&cmd) {
                                let command_handle = handle.clone();
                                let response_writer = writer.clone();
                                let rejected_charge =
                                    budget.reserve(ControlCharge::RequestBytes(
                                        super::super::owner::request_json::clone_id(&id)?,
                                    ))?;
                                let rejected_id = id.clone();
                                let response_budget = budget.clone();
                                let response_stop = stop.clone();
                                let spawned = concurrent_commands.try_spawn_owned(
                                    async move {
                                        let result = execute_ipc_command(command_handle, cmd).await;
                                        if let Err(error) = command::write_command_after_ack(
                                            response_writer,
                                            id,
                                            result,
                                            &response_budget,
                                            &response_stop,
                                        )
                                        .await
                                        {
                                            if !is_transient_disconnect(&error) {
                                                return Err(error);
                                            }
                                        }
                                        Ok(())
                                    },
                                    vec![request_storage.take().unwrap()],
                                );
                                if !spawned {
                                    let error = zero_api::ApiError::new(
                                        zero_api::ApiErrorCode::Conflict,
                                        format!(
                                            "IPC command admission is full (per connection {})",
                                            budget.limits().messages.ipc_pending_commands
                                        ),
                                    );
                                    command::write_command_after_ack(
                                        writer.clone(),
                                        rejected_id,
                                        Err(error),
                                        &budget,
                                        &stop,
                                    )
                                    .await?;
                                }
                                drop(rejected_charge);
                            } else {
                                let command=match local.reserve(zero_runtime::configuration::ControlConnectionCharge::PendingCommand) {
                                    Ok(command)=>command,
                                    Err(error) if error.kind()==io::ErrorKind::WouldBlock=>{
                                        let error=zero_api::ApiError::new(zero_api::ApiErrorCode::Conflict,"IPC command admission is full");
                                        command::write_command_after_ack(writer.clone(),id,Err(error),&budget, &stop).await?;
                                        continue;
                                    }
                                    Err(error)=>return Err(error),
                                };
                                let mut original_input = vec![request_storage.take().unwrap()];
                                let credits = match commands::CommandCredits::new(
                                    command,
                                    &mut original_input,
                                ) {
                                    Ok(credits) => credits,
                                    Err(error) => {
                                        drop((cmd, id));
                                        drop(original_input);
                                        return Err(error);
                                    }
                                };
                                let _ = credits._command.register_original_connection();
                                let observation = credits.observation.clone();
                                inline_command = Some(credits);
                                let written = observation
                                    .scope_with_stop(stop.clone(), async {
                                        let result = execute_ipc_command(handle.clone(), cmd).await;
                                        command::write_command_after_ack(
                                            writer.clone(),
                                            id,
                                            result,
                                            &budget,
                                            &stop,
                                        )
                                        .await
                                    })
                                    .await;
                                let joined = inline_command.as_ref().unwrap().settle().await;
                                drop(inline_command.take());
                                super::super::owner::finish_work([written, joined])?;
                            }
                        }
                        Err(error) => {
                            let resp = error.with_id(id);
                            frames::write_response_until(&writer, &resp, &budget, &stop).await?;
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
                        frames::write_response_until(&writer, &resp, &budget, &stop).await?;
                        continue;
                    }

                    let charge = match budget.reserve(ControlCharge::Subscription) {
                        Ok(charge) => charge,
                        Err(error) => {
                            let resp = ipc_error(id, "resource_exhausted", error.to_string());
                            frames::write_response_until(&writer, &resp, &budget, &stop).await?;
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
                    let (subscriber, wants_snapshot) =
                        match handle.engine_handle().subscribe_control_live(filter) {
                            Ok(actual) => actual,
                            Err(error) => {
                                let code = if error.code == zero_api::ApiErrorCode::Conflict {
                                    "resource_exhausted"
                                } else {
                                    "subscribe_failed"
                                };
                                let resp = ipc_error(id, code, error.to_string());
                                frames::write_response_until(&writer, &resp, &budget, &stop)
                                    .await?;
                                continue;
                            }
                        };
                    let initial = if wants_snapshot {
                        let initial_slot = match local.reserve(
                            zero_runtime::configuration::ControlConnectionCharge::EventFrame,
                        ) {
                            Ok(slot) => slot,
                            Err(error) => {
                                let resp = ipc_error(id, "resource_exhausted", error.to_string());
                                frames::write_response_until(&writer, &resp, &budget, &stop)
                                    .await?;
                                continue;
                            }
                        };
                        match super::super::owner::prepare_initial_snapshot(
                            handle.engine_handle(),
                            &budget,
                            super::super::owner::EventFraming::Ipc,
                            0,
                        ) {
                            Ok((frame, _)) => Some((frame, initial_slot)),
                            Err(error) => {
                                let resp = ipc_error(id, "resource_exhausted", error.to_string());
                                frames::write_response_until(&writer, &resp, &budget, &stop)
                                    .await?;
                                continue;
                            }
                        }
                    } else {
                        None
                    };
                    {
                        subscribed = true;
                        subscription_charge = Some(charge);
                        let resp = ipc_ok(id, "subscribed");
                        frames::write_response_until(&writer, &resp, &budget, &stop).await?;
                        event_jobs.start(
                            subscriber,
                            initial,
                            writer.clone(),
                            stop.clone(),
                            budget.clone(),
                            local.clone(),
                        );
                    }
                }
            }
        }

        io::Result::Ok(())
    };
    let result = super::super::owner::catch_work(work).await;
    stop.cancel();
    // A polling panic or failed response may leave the accepted Apply alive.
    // This exact original record retains its input and command capacity.
    let inline = match inline_command.as_ref() {
        Some(command) => command.settle().await,
        None => Ok(()),
    };
    drop(inline_command.take());
    // The event writer drops its actual receiver, waking a full blocking
    // bridge. Join both originals even when an earlier read/write failed.
    let [events, bridge] = event_jobs.join().await;
    let commands = concurrent_commands.cancel_all().await;
    // Actual queue/writer/worker custody has returned before this capacity.
    drop(subscription_charge);
    super::super::owner::finish_caught(result, [inline, events, bridge, commands])
}
