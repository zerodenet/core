use super::super::ProxyHandle;
use super::diagnostics::{
    execute_diagnostics_dns_cache, execute_diagnostics_dns_lookup,
    execute_diagnostics_fakeip_lookup, execute_diagnostics_probe_outbound,
    execute_diagnostics_probe_target, execute_diagnostics_trace_route,
};
use super::fake_ip::execute_fake_ip_clear;
use super::tun::{execute_tun_start, execute_tun_stop};

impl zero_api::CommandService for ProxyHandle {
    fn execute(
        &self,
        command: zero_api::CommandRequest,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        match &command {
            zero_api::CommandRequest::ConfigApply(_)
            | zero_api::CommandRequest::ConfigApplyRuntime(_) => Err(zero_api::ApiError::new(
                zero_api::ApiErrorCode::Internal,
                "proxy configuration replacement requires execute_acknowledged",
            )),
            zero_api::CommandRequest::TunStart(cmd) => execute_tun_start(self, cmd),
            zero_api::CommandRequest::TunStop(_) => execute_tun_stop(self),
            zero_api::CommandRequest::DiagnosticsProbeOutbound(cmd) => {
                execute_diagnostics_probe_outbound(self, cmd)
            }
            zero_api::CommandRequest::DiagnosticsProbeTarget(cmd) => {
                execute_diagnostics_probe_target(self, cmd)
            }
            zero_api::CommandRequest::DiagnosticsDnsLookup(cmd) => {
                execute_diagnostics_dns_lookup(self, cmd)
            }
            zero_api::CommandRequest::DiagnosticsDnsCache(cmd) => {
                execute_diagnostics_dns_cache(self, cmd)
            }
            zero_api::CommandRequest::DiagnosticsFakeipLookup(cmd) => {
                execute_diagnostics_fakeip_lookup(self, cmd)
            }
            zero_api::CommandRequest::FakeIpClear(cmd) => execute_fake_ip_clear(self, cmd),
            zero_api::CommandRequest::DiagnosticsTraceRoute(cmd) => {
                execute_diagnostics_trace_route(self, cmd)
            }
            _ => self.inner.execute(command),
        }
    }

    fn execute_acknowledged(
        &self,
        command: zero_api::CommandRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = zero_api::ApiResult<zero_api::CommandResponse>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            if let zero_api::CommandRequest::ConfigApply(request)
            | zero_api::CommandRequest::ConfigApplyRuntime(request) = &command
            {
                let persist = matches!(&command, zero_api::CommandRequest::ConfigApply(_));
                let raw = serde_json::to_string(&request.config).map_err(|error| {
                    zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error.to_string())
                })?;
                let config = zero_config::RuntimeConfig::parse_with_source_dir(
                    &raw,
                    self.proxy.engine.config().source_dir.clone(),
                )
                .map_err(|error| {
                    zero_api::ApiError::new(
                        zero_api::ApiErrorCode::InvalidArgument,
                        error.to_string(),
                    )
                })?;
                if let Some(reconciler) = &self.config_reconciler {
                    reconciler
                        .validate(self.proxy.engine.config().as_ref(), &config)
                        .map_err(|error| {
                            zero_api::ApiError::new(zero_api::ApiErrorCode::InvalidArgument, error)
                        })?;
                }
                let reconciled = self
                    .apply_config_transaction_and_wait(
                        config,
                        std::time::Duration::from_secs(15),
                        persist,
                    )
                    .await
                    .map_err(|error| {
                        zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error)
                    })?;
                return Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({
                        "applied": true,
                        "persistence": if persist { "source_file" } else { "runtime_only" },
                        "reconciled": true,
                        "core_instance_id": self.proxy.engine.core_instance_id(),
                        "config_revision": self.proxy.engine.config_revision(),
                        "application_components": reconciled.components,
                    })),
                });
            }

            self.execute(command)
        })
    }
}
