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
    let (reader, writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let writer = Arc::new(TokioMutex::new(BufWriter::new(writer)));

    let mut subscribed = false;
    let mut subscription_charge: Option<ControlBudgetPermit> = None;
    let mut event_jobs = super::events::EventJobs::new();
    let mut concurrent_commands =
        ConcurrentCommandTasks::with_budget(stop.clone(), budget.clone(), local.clone());
    // Every error/cancellation exits this work future into the same retained
    // cleanup path. The original job handles live outside that future.
    let work = async {
        loop {
            let Some(raw_line) =
                frames::next_request(&mut reader, &budget, &mut concurrent_commands).await?
            else {
                break;
            };
            let line = input::text(raw_line.bytes())?;
            if line.trim().is_empty() {
                continue;
            }

            let mut request_storage = Some(raw_line.reserve_decoded(&budget)?);
            let request: IpcRequest = match serde_json::from_str(line.trim()) {
                Ok(req) => req,
                Err(_) => {
                    let resp = ipc_error(None, "invalid_argument", "invalid request frame");
                    frames::write_budgeted_response(&mut *writer.lock().await, &resp, &budget)
                        .await?;
                    continue;
                }
            };

            drop(raw_line);
            match request {
                IpcRequest::Ping { id } => {
                    let resp = ipc_ok(id, "pong");
                    frames::write_budgeted_response(&mut *writer.lock().await, &resp, &budget)
                        .await?;
                }
                IpcRequest::Query { id, request } => match handle.query(request) {
                    Ok(query_resp) => {
                        let resp = ApiResponse::ok_with_id(id, query_resp);
                        frames::write_budgeted_response(&mut *writer.lock().await, &resp, &budget)
                            .await?;
                    }
                    Err(error) => {
                        let resp =
                            super::super::owner::BorrowedErrorResponse::new(id.as_ref(), &error);
                        frames::write_budgeted_response(&mut *writer.lock().await, &resp, &budget)
                            .await?;
                    }
                },
                IpcRequest::Command { id, method, params } => {
                    let command = parse_owned_command(method, params);
                    match command {
                        Ok(ref cmd) if !auth_ctx.allows(cmd.required_permission()) => {
                            let error =
                                zero_api::ApiError::permission_denied(cmd.required_permission());
                            let resp = super::super::owner::BorrowedErrorResponse::new(
                                id.as_ref(),
                                &error,
                            );
                            frames::write_budgeted_response(
                                &mut *writer.lock().await,
                                &resp,
                                &budget,
                            )
                            .await?;
                        }
                        Ok(cmd) => {
                            if command_can_run_concurrently(&cmd) {
                                let command_handle = handle.clone();
                                let response_writer = writer.clone();
                                let rejected_charge =
                                    budget.reserve(ControlCharge::RequestBytes(
                                        super::super::owner::serialized_len(&id)?,
                                    ))?;
                                let rejected_id = id.clone();
                                let response_budget = budget.clone();
                                let spawned = concurrent_commands.try_spawn_owned(
                                    async move {
                                        let result = execute_ipc_command(command_handle, cmd).await;
                                        if let Err(error) = write_budgeted_command_result(
                                            response_writer,
                                            id,
                                            result,
                                            &response_budget,
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
                                    write_budgeted_command_result(
                                        writer.clone(),
                                        rejected_id,
                                        Err(error),
                                        &budget,
                                    )
                                    .await?;
                                }
                                drop(rejected_charge);
                            } else {
                                let _command=match local.reserve(zero_runtime::configuration::ControlConnectionCharge::PendingCommand) {
                                    Ok(command)=>command,
                                    Err(error) if error.kind()==io::ErrorKind::WouldBlock=>{
                                        let error=zero_api::ApiError::new(zero_api::ApiErrorCode::Conflict,"IPC command admission is full");
                                        write_budgeted_command_result(writer.clone(),id,Err(error),&budget).await?;
                                        continue;
                                    }
                                    Err(error)=>return Err(error),
                                };
                                let result = execute_ipc_command(handle.clone(), cmd).await;
                                write_budgeted_command_result(writer.clone(), id, result, &budget)
                                    .await?;
                            }
                        }
                        Err(error) => {
                            let resp = error.with_id(id);
                            frames::write_budgeted_response(
                                &mut *writer.lock().await,
                                &resp,
                                &budget,
                            )
                            .await?;
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
                        frames::write_budgeted_response(&mut *writer.lock().await, &resp, &budget)
                            .await?;
                        continue;
                    }

                    let charge = match budget.reserve(ControlCharge::Subscription) {
                        Ok(charge) => charge,
                        Err(error) => {
                            let resp = ipc_error(id, "resource_exhausted", error.to_string());
                            frames::write_budgeted_response(
                                &mut *writer.lock().await,
                                &resp,
                                &budget,
                            )
                            .await?;
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
                            frames::write_budgeted_response(
                                &mut *writer.lock().await,
                                &resp,
                                &budget,
                            )
                            .await?;
                            event_jobs.start(
                                subscriber,
                                writer.clone(),
                                stop.clone(),
                                budget.clone(),
                                local.clone(),
                            );
                        }
                        Err(error) => {
                            let resp = super::super::owner::BorrowedErrorResponse::new(
                                id.as_ref(),
                                &error,
                            );
                            frames::write_budgeted_response(
                                &mut *writer.lock().await,
                                &resp,
                                &budget,
                            )
                            .await?;
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
    let [events, bridge] = event_jobs.join().await;
    let commands = concurrent_commands.cancel_all().await;
    // Actual queue/writer/worker custody has returned before this capacity.
    drop(subscription_charge);
    super::super::owner::finish_caught(result, [events, bridge, commands])
}
