use crate::runtime::outbound_probe::{
    OutboundProbeRequest, OutboundProbeRuntime, OUTBOUND_PROBE_TIMEOUT_MS,
};
use crate::runtime::route_runtime::route_trace_for_session;
use tracing::info;
use zero_core::{Network, ProtocolType, Session};
use zero_dns::DnsQueryRole;
use zero_traits::{DnsResolver, IpAddress};

use super::super::util::parse_ip_address;
use super::super::ProxyHandle;
use super::runtime::with_current_runtime;

pub(super) fn execute_diagnostics_probe_target(
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

                let reachable = matches!(
                    tokio::time::timeout(
                        std::time::Duration::from_secs(2),
                        proxy.protocols.direct_connector().connect_host(
                            &host,
                            port,
                            proxy.resolver.as_ref(),
                            &proxy.egress_interface,
                        ),
                    )
                    .await,
                    Ok(Ok(_))
                );
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

pub(super) fn execute_diagnostics_dns_lookup(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsDnsLookupCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();
    let hostname = cmd.hostname.clone();

    with_current_runtime("no tokio runtime available for dns_lookup command", |rt| {
        rt.block_on(async move {
            let addresses = proxy
                .resolver
                .resolve(&hostname)
                .await
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
    })
}

pub(super) fn execute_diagnostics_trace_route(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsTraceRouteCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();
    let target = cmd.target.clone();
    let port = cmd.port;
    let inbound_tag = cmd.inbound_tag.clone();
    let (protocol, network) = trace_protocol(cmd.protocol.as_deref())?;

    with_current_runtime("no tokio runtime available for trace_route command", |rt| {
        rt.block_on(async move {
            let mut session = Session::new(0, trace_target_address(&target), port, network, ProtocolType::UNKNOWN);
            session.inbound_tag = inbound_tag;

            let services = proxy.tcp_runtime_services();
            let trace = route_trace_for_session(&services, &session).await;
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
                    "inbound_tag": session.inbound_tag,
                    "effective_mode": trace.mode,
                    "route_action": match trace.decision {
                        zero_engine::RouteDecision::Route(tag) => serde_json::json!({ "route": tag }),
                        zero_engine::RouteDecision::Direct => serde_json::json!("direct"),
                        zero_engine::RouteDecision::Reject => serde_json::json!("reject"),
                    },
                    "matched_rule": matched_rule,
                })),
            })
        })
    })
}

fn probe_target_endpoint(
    proxy: &crate::runtime::Proxy,
    snapshot: &zero_engine::EngineRuntimeSnapshot,
    target_tag: &str,
) -> zero_api::ApiResult<Option<(String, u16)>> {
    let plan = snapshot.plan();
    let target_id = plan.target_id(target_tag).ok_or_else(|| {
        zero_api::ApiError::new(
            zero_api::ApiErrorCode::NotFound,
            format!("target `{target_tag}` was not found"),
        )
    })?;
    let (resolved, _plan) = proxy
        .engine()
        .resolve_target_id_in_snapshot(snapshot, target_id)
        .ok_or_else(|| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::NotFound,
                format!("target `{target_tag}` could not be resolved"),
            )
        })?;
    let leaf = match &resolved {
        zero_engine::ResolvedOutbound::Single(leaf) => Some(leaf),
        zero_engine::ResolvedOutbound::Fallback { candidates } => candidates.first(),
        zero_engine::ResolvedOutbound::Relay { .. } => None,
    };
    let Some(leaf) = leaf else {
        return Ok(None);
    };
    let runtime = proxy
        .protocols
        .claim_outbound_leaf(snapshot.config().as_ref(), leaf.clone())
        .map_err(|error| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::InvalidArgument,
                format!("failed to claim probe target `{target_tag}`: {error}"),
            )
        })?
        .runtime();
    Ok(runtime
        .endpoint
        .map(|endpoint| (endpoint.server, endpoint.port)))
}

fn ip_address_string(address: IpAddress) -> String {
    match address {
        IpAddress::V4(bytes) => std::net::Ipv4Addr::from(bytes).to_string(),
        IpAddress::V6(bytes) => std::net::Ipv6Addr::from(bytes).to_string(),
    }
}

fn trace_protocol(protocol: Option<&str>) -> zero_api::ApiResult<(&'static str, Network)> {
    match protocol {
        None => Ok(("tcp", Network::Tcp)),
        Some(value) if value.eq_ignore_ascii_case("tcp") => Ok(("tcp", Network::Tcp)),
        Some(value) if value.eq_ignore_ascii_case("udp") => Ok(("udp", Network::Udp)),
        Some(value) => Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::InvalidArgument,
            format!("invalid protocol `{value}`; expected `tcp` or `udp`"),
        )),
    }
}

fn trace_target_address(target: &str) -> zero_core::Address {
    match target.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(value)) => zero_core::Address::Ipv4(value.octets()),
        Ok(std::net::IpAddr::V6(value)) => zero_core::Address::Ipv6(value.octets()),
        Err(_) => zero_core::Address::Domain(target.to_owned()),
    }
}

pub(super) fn execute_diagnostics_probe_outbound(
    handle: &ProxyHandle,
    cmd: &zero_api::command::DiagnosticsProbeOutboundCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
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

    with_current_runtime(
        "no tokio runtime available for probe_outbound command",
        |rt| {
            rt.block_on(async move {
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
                    timeout_ms = OUTBOUND_PROBE_TIMEOUT_MS,
                    "outbound diagnostic probe started"
                );
                let request = OutboundProbeRequest::parse(&url);
                let result = match request {
                    Ok(request) => {
                        OutboundProbeRuntime::new(services)
                            .probe_target_tag(&target_tag, &request)
                            .await
                    }
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
                            timeout_ms = OUTBOUND_PROBE_TIMEOUT_MS,
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
                                "timeout_ms": OUTBOUND_PROBE_TIMEOUT_MS,
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
                            timeout_ms = OUTBOUND_PROBE_TIMEOUT_MS,
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
                                "timeout_ms": OUTBOUND_PROBE_TIMEOUT_MS,
                                "error_code": error.code(),
                                "error": error.message(),
                                "started_at_unix_ms": started_at_unix_ms,
                                "completed_at_unix_ms": completed_at_unix_ms,
                                "duration_ms": duration_ms,
                            })),
                        })
                    }
                }
            })
        },
    )
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(super) fn execute_diagnostics_dns_cache(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsDnsCacheCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();
    let domain = cmd.domain.clone();
    let limit = cmd.limit.unwrap_or(256);

    with_current_runtime("no tokio runtime available for dns_cache command", |rt| {
        rt.block_on(async move {
            let resolver = &proxy.resolver;
            let enabled = resolver.cache_enabled();
            let result = if let Some(domain) = domain {
                match resolver.inspect_cache(&domain).await {
                    Some((addresses, ttl_seconds)) => serde_json::json!({
                        "enabled": enabled,
                        "domain": domain,
                        "hit": true,
                        "addresses": addresses,
                        "ttl_seconds": ttl_seconds,
                    }),
                    None => serde_json::json!({
                        "enabled": enabled,
                        "domain": domain,
                        "hit": false,
                        "addresses": [],
                        "ttl_seconds": null,
                    }),
                }
            } else {
                let entries: Vec<_> = resolver
                    .list_cache(limit)
                    .await
                    .into_iter()
                    .map(|(domain, addresses, ttl_seconds)| {
                        serde_json::json!({
                            "domain": domain,
                            "addresses": addresses,
                            "ttl_seconds": ttl_seconds,
                        })
                    })
                    .collect();
                let count = entries.len();
                serde_json::json!({
                    "enabled": enabled,
                    "entries": entries,
                    "count": count,
                })
            };
            Ok(zero_api::CommandResponse {
                accepted: true,
                result: Some(result),
            })
        })
    })
}

pub(super) fn execute_diagnostics_fakeip_lookup(
    handle: &ProxyHandle,
    cmd: &zero_api::DiagnosticsFakeipLookupCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();
    let domain = cmd.domain.clone();
    let ip = cmd.ip.clone();

    with_current_runtime(
        "no tokio runtime available for fakeip_lookup command",
        |rt| {
            rt.block_on(async move {
                let resolver = &proxy.resolver;
                let enabled = resolver.fake_ip_enabled();
                let stats = resolver.fake_ip_stats().await.map(|stats| {
                    serde_json::json!({
                        "allocations": stats.allocations,
                        "expirations": stats.expirations,
                        "evictions": stats.evictions,
                        "exhaustions": stats.exhaustions,
                        "collisions": stats.collisions,
                        "reverse_misses": stats.reverse_misses,
                        "live_mappings": stats.live_mappings,
                        "capacity": stats.capacity,
                    })
                });
                let result = if let Some(domain) = domain {
                    let fake_ip = resolver.lookup_fake_ip_domain(&domain).await;
                    serde_json::json!({
                        "enabled": enabled,
                        "domain": domain,
                        "fake_ip": fake_ip,
                        "stats": stats,
                    })
                } else if let Some(ip) = ip {
                    let domain = match parse_ip_address(&ip) {
                        Some(addr) => resolver.lookup_fake_ip(&addr).await,
                        None => {
                            return Err(zero_api::ApiError::new(
                                zero_api::ApiErrorCode::InvalidArgument,
                                format!("invalid ip `{ip}`"),
                            ));
                        }
                    };
                    serde_json::json!({
                        "enabled": enabled,
                        "ip": ip,
                        "domain": domain,
                        "stats": stats,
                    })
                } else {
                    return Err(zero_api::ApiError::new(
                        zero_api::ApiErrorCode::InvalidArgument,
                        "fakeip_lookup requires `domain` or `ip`",
                    ));
                };
                Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(result),
                })
            })
        },
    )
}
