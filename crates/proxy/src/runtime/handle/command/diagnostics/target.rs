use super::*;
pub(in crate::runtime::handle::command) fn execute_diagnostics_probe_target(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsProbeTargetCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();
    let target_tag = cmd.target_tag.clone();
    let snapshot = proxy.engine().runtime_snapshot();
    let core_instance_id = proxy.engine().core_instance_id().to_owned();
    let config_revision = snapshot.config_revision();
    let operation_id = proxy.engine().operation_id(cmd.operation_id.as_deref());

    with_current_runtime(
        "no tokio runtime available for probe_target command",
        |rt| {
            rt.block_on(async move {
                let started_at_unix_ms = unix_timestamp_ms();
                let started = std::time::Instant::now();
                let Some((host, port)) = probe_target_endpoint(&proxy, &snapshot, &target_tag)? else {
                    return Ok(zero_api::CommandResponse {
                        accepted: true,
                        result: Some(serde_json::json!({
                            "operation_id": operation_id,
                            "core_instance_id": core_instance_id,
                            "config_revision": config_revision,
                            "target_tag": target_tag,
                            "reachable": false,
                            "terminal_status": "failed",
                            "error_code": "no_fixed_endpoint",
                            "error": "outbound has no probeable fixed server",
                            "started_at_unix_ms": started_at_unix_ms,
                            "completed_at_unix_ms": unix_timestamp_ms(),
                            "duration_ms": started.elapsed().as_millis() as u64,
                        })),
                    });
                };

                let stop=zero_runtime::ApplyObservation::caller_stop();
                let owner=proxy.clone();
                let server=host.clone();
                let reachable=matches!(proxy.with_diagnostic_service(async move {
                    OutboundProbeRuntime::new(owner.tcp_runtime_services())
                        .probe_server_endpoint(server,port,stop).await
                }).await,Ok(Ok(())));
                Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({
                        "operation_id": operation_id,
                        "core_instance_id": core_instance_id,
                        "config_revision": config_revision,
                        "target_tag": target_tag,
                        "server": host,
                        "port": port,
                        "reachable": reachable,
                        "terminal_status": if reachable { "succeeded" } else { "failed" },
                        "error_code": if reachable { None::<&str> } else { Some("connection_failed") },
                        "latency_ms": reachable.then(|| started.elapsed().as_millis() as u64),
                        "started_at_unix_ms": started_at_unix_ms,
                        "completed_at_unix_ms": unix_timestamp_ms(),
                        "duration_ms": started.elapsed().as_millis() as u64,
                    })),
                })
            })
        },
    )
}
