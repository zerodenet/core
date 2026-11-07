//! Existing acknowledged command dispatch and response wire format.
use super::*;
pub(super) fn command_can_run_concurrently(command: &CommandRequest) -> bool {
    matches!(command, CommandRequest::DiagnosticsProbeOutbound(_))
}

pub(super) fn execute_ipc_command(
    command_handle: ProxyHandle,
    command: CommandRequest,
) -> zero_platform_tokio::contract::BoxFuture<'static, zero_api::ApiResult<zero_api::CommandResponse>>
{
    // This same admitted command owns one bounded future, like the existing
    // CommandService entry. Do not propagate its complete probe/cleanup layout
    // into every generic connection writer and TaskLocal wrapper.
    Box::pin(async move {
        let command = match command {
            CommandRequest::DiagnosticsProbeOutbound(command) => {
                return command_handle
                    .execute_diagnostics_probe_outbound_command(command)
                    .await;
            }
            command => command,
        };
        // The service owns coordination and blocking execution. In particular,
        // endpoint controls and stats.reset must share config.apply's executor.
        command_handle.execute_acknowledged(command).await
    })
}

pub(super) async fn write_budgeted_command_result<W>(
    writer: Arc<TokioMutex<BufWriter<W>>>,
    id: Option<serde_json::Value>,
    result: zero_api::ApiResult<zero_api::CommandResponse>,
    budget: &Arc<ControlBudget>,
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    match result {
        Ok(command_response) => {
            let response = ApiResponse::ok_with_id(id, command_response);
            frames::write_budgeted_response(&mut *writer.lock().await, &response, budget).await
        }
        Err(error) => {
            let response = super::super::owner::BorrowedErrorResponse::new(id.as_ref(), &error);
            frames::write_budgeted_response(&mut *writer.lock().await, &response, budget).await
        }
    }
}

pub(super) async fn write_command_after_ack<W: tokio::io::AsyncWrite + Unpin>(
    writer: Arc<TokioMutex<BufWriter<W>>>,
    id: Option<serde_json::Value>,
    result: zero_api::ApiResult<zero_api::CommandResponse>,
    budget: &Arc<ControlBudget>,
    stop: &CancellationToken,
) -> io::Result<()> {
    // This helper receives the final operation result, never its pending future.
    tokio::select! {biased;_=stop.cancelled()=>Ok(()),result=write_budgeted_command_result(writer,id,result,budget)=>result}
}

#[cfg(test)]
pub(super) async fn write_command_result<W>(
    writer: Arc<TokioMutex<BufWriter<W>>>,
    id: Option<serde_json::Value>,
    result: zero_api::ApiResult<zero_api::CommandResponse>,
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
        &zero_config::RuntimeOptionsConfig::default(),
    ));
    write_budgeted_command_result(writer, id, result, &budget).await
}
