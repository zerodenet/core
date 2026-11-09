use super::*;
pub(in crate::runtime::handle::command) fn execute_diagnostics_trace_route(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsTraceRouteCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    with_current_runtime("no tokio runtime available for trace_route command", |rt| {
        rt.block_on(trace(handle.clone(), cmd.clone()))
    })
}
pub(super) fn trace(
    handle: ProxyHandle,
    cmd: zero_api::DiagnosticsTraceRouteCommand,
) -> zero_platform_tokio::contract::BoxFuture<'static, zero_api::ApiResult<zero_api::CommandResponse>>
{
    Box::pin(async move {
        let proxy = handle.proxy.clone();
        let target = cmd.target.clone();
        let port = cmd.port;
        let inbound_tag = cmd.inbound_tag.clone();
        let (protocol, _network) = trace_protocol(cmd.protocol.as_deref())?;

        let actual = proxy.clone();
        proxy.with_diagnostic_service(async move {
            let proxy = actual;
            let route_target = trace_target_address(&target);

            let services = proxy.tcp_runtime_services();
            let trace = resolution::settled(&proxy.resolver, route_trace_for_target(&services, &route_target, None, inbound_tag.as_deref()))
                .await.map_err(|source|zero_api::ApiError::new(zero_api::ApiErrorCode::Internal,source.to_string()))?;
            let matched_rule = trace.matched_rule.map(|matched| {
                serde_json::json!({
                    "index": matched.index,
                    "condition": matched.condition,
                })
            });

            Ok(zero_api::CommandResponse {
                accepted: true,
                result: Some(serde_json::json!({
                    "target": target,
                    "port": port,
                    "protocol": protocol,
                    "inbound_tag": inbound_tag,
                    "effective_mode": trace.mode,
                    "route_action": match trace.decision {
                        zero_engine::RouteDecision::Route(tag) => serde_json::json!({ "route": tag }),
                        zero_engine::RouteDecision::Direct => serde_json::json!("direct"),
                        zero_engine::RouteDecision::Reject => serde_json::json!("reject"),
                    },
                    "matched_rule": matched_rule,
                })),
            })
            }).await.map_err(|source|zero_api::ApiError::new(zero_api::ApiErrorCode::Internal,source.to_string()))?
    })
}
