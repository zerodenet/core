//! A cold diagnostic is an explicitly accepted finite Service maintenance task.
use super::ProxyHandle;
impl ProxyHandle {
    /// Cold diagnostics accept finite Service maintenance into the host owner;
    /// a disconnected waiter cannot abandon component shutdown.
    pub async fn execute_diagnostics_probe_outbound_command(
        &self,
        cmd: zero_api::command::DiagnosticsProbeOutboundCommand,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        let handle = self.clone();
        self.proxy
            .with_diagnostic_service(
                async move { handle.execute_started_outbound_probe(cmd).await },
            )
            .await
            .map_err(|cause| {
                zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, cause.to_string())
            })?
    }
}

#[cfg(all(test, feature = "managed-stream-runtime"))]
mod tests;
