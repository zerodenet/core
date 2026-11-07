use super::*;
pub(in crate::runtime::handle::command) fn execute_diagnostics_probe_outbound(
    handle: &ProxyHandle,
    cmd: &zero_api::command::DiagnosticsProbeOutboundCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let handle = handle.clone();
    let cmd = cmd.clone();
    with_current_runtime(
        "no tokio runtime available for probe_outbound command",
        |rt| rt.block_on(handle.execute_diagnostics_probe_outbound_command(cmd)),
    )
}

impl ProxyHandle {
    pub(super) async fn execute_started_outbound_probe(
        &self,
        cmd: zero_api::command::DiagnosticsProbeOutboundCommand,
        stop: Option<zero_platform_tokio::contract::CancellationToken>,
    ) -> zero_api::ApiResult<zero_api::CommandResponse> {
        let handle = self;
        let proxy = handle.proxy.clone();
        let target_tag = cmd.target_tag.clone();
        let snapshot = handle.proxy.engine().runtime_snapshot();
        let core_instance_id = handle.proxy.engine().core_instance_id().to_owned();
        let config_revision = snapshot.config_revision();
        let operation_id = handle
            .proxy
            .engine()
            .operation_id(cmd.operation_id.as_deref());
        let url = snapshot
            .config()
            .runtime
            .latency_test_url_or(cmd.url.as_deref())
            .to_owned();
        let services = proxy.tcp_runtime_services_for_snapshot(snapshot);
        let probe_runtime = OutboundProbeRuntime::new(services);
        let deadline = probe_runtime.probe_deadline();
        let timeout_ms = deadline.timeout_ms;
        let dns_budget_ms = deadline.dns_budget_ms;
        let transport_budget_ms = deadline.transport_budget_ms;
        let deadline_capped = deadline.capped;

        let started_at_unix_ms = unix_timestamp_ms();
        let started = std::time::Instant::now();
        info!(
            source = "core",
            method = "diagnostics.probe_outbound",
            operation_kind = "diagnostic_outbound",
            phase = "started",
            operation_id,
            core_instance_id,
            config_revision,
            target_tag,
            url,
            started_at_unix_ms,
            timeout_ms,
            dns_budget_ms,
            transport_budget_ms,
            deadline_capped,
            "outbound diagnostic probe started"
        );
        let request = OutboundProbeRequest::parse(&url);
        let result = match request {
            Ok(request) => match stop {
                Some(stop) => {
                    probe_runtime
                        .probe_target_tag_until(&target_tag, &request, &stop)
                        .await
                }
                None => probe_runtime.probe_target_tag(&target_tag, &request).await,
            },
            Err(error) => Err(error),
        };
        let completed_at_unix_ms = unix_timestamp_ms();
        let duration_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(latency_ms) => {
                info!(
                    source = "core",
                    method = "diagnostics.probe_outbound",
                    operation_kind = "diagnostic_outbound",
                    phase = "completed",
                    operation_id,
                    core_instance_id,
                    config_revision,
                    target_tag,
                    url,
                    started_at_unix_ms,
                    completed_at_unix_ms,
                    duration_ms,
                    timeout_ms,
                    dns_budget_ms,
                    transport_budget_ms,
                    deadline_capped,
                    terminal_status = "succeeded",
                    reachable = true,
                    affects_policy_selection = false,
                    affects_outbound_health = false,
                    bypasses_outbound_health_quarantine = true,
                    latency_ms,
                    "outbound diagnostic probe completed"
                );
                Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({
                        "operation_id": operation_id,
                        "core_instance_id": core_instance_id,
                        "config_revision": config_revision,
                        "operation_kind": "diagnostic_outbound",
                        "target_tag": target_tag,
                        "url": url,
                        "via": "through_proxy",
                        "reachable": true,
                        "terminal_status": "succeeded",
                        "affects_policy_selection": false,
                        "affects_outbound_health": false,
                        "bypasses_outbound_health_quarantine": true,
                        "latency_ms": latency_ms,
                        "timeout_ms": timeout_ms,
                        "dns_budget_ms": dns_budget_ms,
                        "transport_budget_ms": transport_budget_ms,
                        "deadline_capped": deadline_capped,
                        "started_at_unix_ms": started_at_unix_ms,
                        "completed_at_unix_ms": completed_at_unix_ms,
                        "duration_ms": duration_ms,
                    })),
                })
            }
            Err(error) => {
                info!(
                    source = "core",
                    method = "diagnostics.probe_outbound",
                    operation_kind = "diagnostic_outbound",
                    phase = "completed",
                    operation_id,
                    core_instance_id,
                    config_revision,
                    target_tag,
                    url,
                    started_at_unix_ms,
                    completed_at_unix_ms,
                    duration_ms,
                    timeout_ms,
                    dns_budget_ms,
                    transport_budget_ms,
                    deadline_capped,
                    terminal_status = "failed",
                    reachable = false,
                    affects_policy_selection = false,
                    affects_outbound_health = false,
                    bypasses_outbound_health_quarantine = true,
                    error_code = error.code(),
                    error = error.message(),
                    "outbound diagnostic probe completed"
                );
                Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({
                        "operation_id": operation_id,
                        "core_instance_id": core_instance_id,
                        "config_revision": config_revision,
                        "operation_kind": "diagnostic_outbound",
                        "target_tag": target_tag,
                        "url": url,
                        "via": "through_proxy",
                        "reachable": false,
                        "terminal_status": "failed",
                        "affects_policy_selection": false,
                        "affects_outbound_health": false,
                        "bypasses_outbound_health_quarantine": true,
                        "latency_ms": null,
                        "timeout_ms": timeout_ms,
                        "dns_budget_ms": dns_budget_ms,
                        "transport_budget_ms": transport_budget_ms,
                        "deadline_capped": deadline_capped,
                        "error_code": error.code(),
                        "error": error.message(),
                        "started_at_unix_ms": started_at_unix_ms,
                        "completed_at_unix_ms": completed_at_unix_ms,
                        "duration_ms": duration_ms,
                    })),
                })
            }
        }
    }
}
