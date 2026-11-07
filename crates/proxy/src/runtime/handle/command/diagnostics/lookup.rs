use super::*;
pub(in crate::runtime::handle::command) fn execute_diagnostics_dns_lookup(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsDnsLookupCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    with_current_runtime("no tokio runtime available for dns_lookup command", |rt| {
        rt.block_on(lookup(handle.clone(), cmd.clone()))
    })
}
pub(super) fn lookup(
    handle: ProxyHandle,
    cmd: zero_api::DiagnosticsDnsLookupCommand,
) -> zero_platform_tokio::contract::BoxFuture<'static, zero_api::ApiResult<zero_api::CommandResponse>>
{
    Box::pin(async move {
        let proxy = handle.proxy.clone();
        let hostname = cmd.hostname.clone();

        let actual = proxy.clone();
        proxy
            .with_diagnostic_service(async move {
                let proxy = actual;
                let addresses =
                    resolution::settled(&proxy.resolver, proxy.resolver.resolve(&hostname))
                        .await
                        .and_then(|result| result)
                        .map_err(|error| {
                            zero_api::ApiError::new(
                                zero_api::ApiErrorCode::InvalidArgument,
                                format!("failed to resolve `{hostname}`: {error}"),
                            )
                        })?
                        .into_iter()
                        .map(ip_address_string)
                        .collect::<Vec<_>>();
                let count = addresses.len();
                let attempts = proxy
                    .resolver
                    .recent_query_attempts(&hostname, DnsQueryRole::Default, 8)
                    .into_iter()
                    .map(|attempt| {
                        serde_json::json!({
                            "role": attempt.role.as_str(),
                            "server_tag": attempt.server_tag,
                            "transport": attempt.transport,
                            "server_endpoints": attempt.server_endpoints,
                            "outbound": attempt.outbound,
                            "success": attempt.success,
                            "failure_reason": attempt.failure_reason,
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({
                        "hostname": hostname,
                        "query_role": DnsQueryRole::Default.as_str(),
                        "resolved_addresses": addresses,
                        "count": count,
                        "attempts": attempts,
                    })),
                })
            })
            .await
            .map_err(|source| {
                zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, source.to_string())
            })?
    })
}
