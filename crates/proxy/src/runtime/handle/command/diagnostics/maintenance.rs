//! A cold diagnostic is an explicitly accepted finite Service maintenance task.
use super::ProxyHandle;
impl ProxyHandle {
    /// Cold diagnostics accept finite Service maintenance into the host owner;
    /// a disconnected waiter cannot abandon component shutdown.
    pub async fn execute_diagnostics_probe_outbound_command(
        &self,
        cmd: zero_api::command::DiagnosticsProbeOutboundCommand,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        self.execute_outbound_probe(cmd, None).await
    }
    /// Explicit caller stop ends only this request's business participation;
    /// its original future remains awaited through host Native/Scope cleanup.
    pub async fn execute_diagnostics_probe_outbound_command_until(
        &self,
        cmd: zero_api::command::DiagnosticsProbeOutboundCommand,
        stop: zero_platform_tokio::contract::CancellationToken,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        self.execute_outbound_probe(cmd, Some(stop)).await
    }
    async fn execute_outbound_probe(
        &self,
        cmd: zero_api::command::DiagnosticsProbeOutboundCommand,
        stop: Option<zero_platform_tokio::contract::CancellationToken>,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        let stop = stop.or_else(zero_runtime::ApplyObservation::caller_stop);
        if stop.as_ref().is_some_and(|stop| stop.is_cancelled()) {
            return Err(zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                "diagnostic caller cancelled",
            ));
        }
        let handle = self.clone();
        let caller_stop = stop.clone();
        let work = self.proxy.with_diagnostic_service(async move {
            handle.execute_started_outbound_probe(cmd, stop).await
        });
        match caller_stop {
            Some(stop) => zero_runtime::ApplyObservation::with_caller_stop(stop, work).await,
            None => work.await,
        }
        .map_err(|cause| {
            zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, cause.to_string())
        })?
    }
}

#[cfg(all(test, feature = "managed-stream-runtime"))]
mod tests;
