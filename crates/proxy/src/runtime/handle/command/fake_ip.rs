use zero_dns::FakeIpClearTarget;

use super::super::util::parse_ip_address;
use super::super::ProxyHandle;
use super::runtime::with_current_runtime;

pub(super) fn execute_fake_ip_clear(
    handle: &ProxyHandle,
    cmd: &zero_api::FakeIpClearCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let domain = cmd.domain.clone();
    let ip = cmd.ip.clone();
    let (target, scope) = match (domain.as_ref(), ip.as_ref()) {
        (None, None) => (FakeIpClearTarget::All, "all"),
        (Some(domain), None) => (FakeIpClearTarget::Domain(domain.clone()), "domain"),
        (None, Some(ip)) => {
            let address = parse_ip_address(ip).ok_or_else(|| {
                zero_api::ApiError::new(
                    zero_api::ApiErrorCode::InvalidArgument,
                    format!("invalid ip `{ip}`"),
                )
            })?;
            (FakeIpClearTarget::Address(address), "ip")
        }
        (Some(_), Some(_)) => {
            return Err(zero_api::ApiError::new(
                zero_api::ApiErrorCode::InvalidArgument,
                "fakeip.clear accepts at most one of `domain` or `ip`",
            ));
        }
    };
    let proxy = handle.proxy.clone();

    with_current_runtime(
        "no tokio runtime available for fakeip.clear command",
        |rt| {
            rt.block_on(async move {
                let enabled = proxy.resolver.fake_ip_enabled();
                let cleared = proxy
                    .resolver
                    .clear_fake_ip(target)
                    .await
                    .map_err(|error| {
                        let code = if error.kind() == std::io::ErrorKind::InvalidInput {
                            zero_api::ApiErrorCode::InvalidArgument
                        } else {
                            zero_api::ApiErrorCode::Internal
                        };
                        zero_api::ApiError::new(code, error.to_string())
                    })?
                    .unwrap_or_default();
                tracing::info!(
                    scope,
                    domain = domain.as_deref(),
                    ip = ip.as_deref(),
                    removed_mappings = cleared.removed_mappings,
                    removed_addresses = cleared.removed_addresses,
                    live_mappings = cleared.live_mappings,
                    retired_addresses = cleared.retired_addresses,
                    "cleared Fake-IP mappings"
                );
                Ok(zero_api::CommandResponse {
                    accepted: true,
                    result: Some(serde_json::json!({
                        "core_instance_id": proxy.engine().core_instance_id(),
                        "config_revision": proxy.engine().config_revision(),
                        "enabled": enabled,
                        "scope": scope,
                        "domain": domain,
                        "ip": ip,
                        "removed_mappings": cleared.removed_mappings,
                        "removed_addresses": cleared.removed_addresses,
                        "live_mappings": cleared.live_mappings,
                        "retired_addresses": cleared.retired_addresses,
                    })),
                })
            })
        },
    )
}
