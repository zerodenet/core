mod maintenance;
mod resolution;
use crate::runtime::outbound_probe::{OutboundProbeRequest, OutboundProbeRuntime};
use crate::runtime::route_runtime::route_trace_for_session;
use tracing::info;
use zero_core::{Network, ProtocolType, Session};
use zero_dns::DnsQueryRole;
use zero_traits::{DnsResolver, IpAddress};

use super::super::util::parse_ip_address;
use super::super::ProxyHandle;
use super::runtime::with_current_runtime;

mod target;
pub(super) use target::execute_diagnostics_probe_target;

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
                        "retired_addresses": stats.retired_addresses,
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

mod outbound;
pub(super) use outbound::execute_diagnostics_probe_outbound;

mod lookup;
pub(super) use lookup::execute_diagnostics_dns_lookup;

mod trace;
pub(super) use trace::execute_diagnostics_trace_route;

pub(super) fn asynchronous(
    handle: ProxyHandle,
    command: zero_api::CommandRequest,
) -> Result<
    zero_platform_tokio::contract::BoxFuture<
        'static,
        zero_api::ApiResult<zero_api::CommandResponse>,
    >,
    zero_api::CommandRequest,
> {
    match command {
        zero_api::CommandRequest::DiagnosticsDnsLookup(request) => {
            Ok(lookup::lookup(handle, request))
        }
        zero_api::CommandRequest::DiagnosticsTraceRoute(request) => {
            Ok(trace::trace(handle, request))
        }
        zero_api::CommandRequest::DiagnosticsProbeTarget(request) => {
            Ok(target::target(handle, request))
        }
        zero_api::CommandRequest::DiagnosticsProbeOutbound(request) => Ok(Box::pin(async move {
            handle
                .execute_diagnostics_probe_outbound_command(request)
                .await
        })),
        other => Err(other),
    }
}
