//! A cold diagnostic is an explicitly accepted finite Service maintenance task.
use super::ProxyHandle;
impl ProxyHandle {
    /// Cold diagnostics accept finite Service maintenance into the host owner;
    /// a disconnected waiter cannot abandon component shutdown.
    pub async fn execute_diagnostics_probe_outbound_command(
        &self,
        cmd: zero_api::command::DiagnosticsProbeOutboundCommand,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        let snapshot = self.proxy.engine().runtime_snapshot();
        if self
            .proxy
            .protocols
            .current_components(snapshot.config())
            .is_some()
        {
            return self.execute_started_outbound_probe(cmd).await;
        }
        let handle = self.clone();
        self.proxy
            .apply_owner
            .run(self.proxy.reload_apply_lock.clone(), async move {
                let config = handle.proxy.engine().runtime_snapshot().config().clone();
                let protocols = handle.proxy.protocols.clone();
                let result = protocols
                    .maintain_service_components(
                        config,
                        tokio::time::Instant::now() + std::time::Duration::from_secs(5),
                        || handle.execute_started_outbound_probe(cmd),
                    )
                    .await
                    .map_err(|cause| {
                        zero_api::ApiError::new(
                            zero_api::ApiErrorCode::Internal,
                            format!("outbound Service maintenance failed: {cause}"),
                        )
                    });
                Ok(result.and_then(|result| result))
            })
            .await
            .map_err(|cause| zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, cause))?
    }
}

#[cfg(all(test, feature = "managed-stream-runtime"))]
mod tests;
