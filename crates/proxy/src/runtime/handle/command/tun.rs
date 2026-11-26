use std::io;

use zero_engine::EngineError;

use super::super::ProxyHandle;
use super::runtime::with_current_runtime;
use crate::{TunInterfaceOptions, TunRuntimeOptions};

pub(super) const TUN_PRIVILEGE_MESSAGE: &str =
    "TUN startup requires elevated host operating-system network privileges";

pub(super) async fn start(
    handle: &ProxyHandle,
    cmd: &zero_api::TunStartCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();
    let name = cmd.name.clone();
    let addr = cmd.addr.clone();
    let mask = cmd.mask.clone();
    let secondary_addr = cmd.secondary_addr.clone();
    let mtu = cmd
        .mtu
        .unwrap_or_else(|| proxy.engine().config().runtime.network.mtu);
    let tag = cmd.tag.clone();
    let auto_route = cmd.auto_route;
    let include_cidrs = cmd
        .include_cidrs
        .iter()
        .map(|cidr| cidr.parse::<ipnet::IpNet>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::InvalidArgument,
                format!("invalid TUN include CIDR: {error}"),
            )
        })?;
    let exclude_cidrs = cmd
        .exclude_cidrs
        .iter()
        .map(|cidr| cidr.parse::<ipnet::IpNet>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::InvalidArgument,
                format!("invalid TUN exclude CIDR: {error}"),
            )
        })?;
    if (!include_cidrs.is_empty() || !exclude_cidrs.is_empty()) && !auto_route {
        return Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::InvalidArgument,
            "TUN include/exclude CIDRs require `auto_route=true`",
        ));
    }
    if include_cidrs.len() > 128 {
        return Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::InvalidArgument,
            "TUN include CIDRs support at most 128 entries",
        ));
    }
    let unique = include_cidrs
        .iter()
        .collect::<std::collections::HashSet<_>>();
    if unique.len() != include_cidrs.len() {
        return Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::InvalidArgument,
            "TUN include CIDRs must not contain duplicates",
        ));
    }
    if exclude_cidrs.len() > 128 {
        return Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::InvalidArgument,
            "TUN exclude CIDRs support at most 128 entries",
        ));
    }
    let unique = exclude_cidrs
        .iter()
        .collect::<std::collections::HashSet<_>>();
    if unique.len() != exclude_cidrs.len() {
        return Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::InvalidArgument,
            "TUN exclude CIDRs must not contain duplicates",
        ));
    }
    let dual_stack = cmd.dual_stack;
    let strict_route = cmd.strict_route;
    let dns_hijack = cmd.dns_hijack;

    proxy
        .start_tun(
            TunInterfaceOptions {
                name: name.as_deref(),
                addr: &addr,
                mask: &mask,
                secondary_addr: secondary_addr.as_deref(),
            },
            mtu,
            &tag,
            TunRuntimeOptions {
                auto_route,
                include_cidrs,
                exclude_cidrs,
                dual_stack,
                strict_route,
                dns_hijack,
            },
        )
        .await
        .map_err(map_tun_start_error)?;
    wait_for_outbound_reconciliation(&proxy).await?;
    Ok(zero_api::CommandResponse::accepted())
}

#[cfg(feature = "raw-ip-runtime")]
pub(in crate::runtime::handle::command) async fn wait_for_outbound_reconciliation(
    proxy: &crate::Proxy,
) -> Result<(), zero_api::ApiError> {
    let mut active = proxy.device_reconciliation_activity();
    if !*active.borrow_and_update() {
        return Ok(());
    }
    let target = proxy.egress_interface.generation();
    let mut reconciled = proxy.reconciled_device_generations();
    if *reconciled.borrow_and_update() >= target {
        return Ok(());
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        tokio::select! {
            changed = reconciled.changed() => {
                if changed.is_err() || *reconciled.borrow_and_update() >= target {
                    return Ok(());
                }
            }
            changed = active.changed() => {
                if changed.is_err() || !*active.borrow_and_update() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(zero_api::ApiError::new(
                    zero_api::ApiErrorCode::Internal,
                    format!(
                        "timed out waiting for outbound devices to reconcile network generation {target}"
                    ),
                ));
            }
        }
    }
}

#[cfg(not(feature = "raw-ip-runtime"))]
async fn wait_for_outbound_reconciliation(_proxy: &crate::Proxy) -> Result<(), zero_api::ApiError> {
    Ok(())
}

pub(super) fn map_tun_start_error(error: EngineError) -> zero_api::ApiError {
    if matches!(&error, EngineError::Io(source) if source.kind() == io::ErrorKind::PermissionDenied)
    {
        return zero_api::ApiError {
            code: zero_api::ApiErrorCode::InsufficientOsPrivilege,
            message: TUN_PRIVILEGE_MESSAGE.to_owned(),
            field_path: None,
            cause: Some(error.to_string()),
            details: Vec::new(),
        };
    }
    if matches!(&error, EngineError::Io(source) if source.kind() == io::ErrorKind::InvalidInput) {
        return zero_api::ApiError::new(zero_api::ApiErrorCode::InvalidArgument, error.to_string());
    }
    zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error.to_string())
}

pub(super) async fn stop(handle: &ProxyHandle) -> zero_api::ApiResult<zero_api::CommandResponse> {
    handle.proxy.stop_tun().await.map_err(|error| {
        zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error.to_string())
    })?;
    wait_for_outbound_reconciliation(&handle.proxy).await?;
    Ok(zero_api::CommandResponse::accepted())
}

pub(super) async fn recover(
    handle: &ProxyHandle,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    handle
        .proxy
        .recover_tun()
        .await
        .map_err(map_tun_start_error)?;
    wait_for_outbound_reconciliation(&handle.proxy).await?;
    Ok(zero_api::CommandResponse::accepted())
}

pub(super) fn execute_tun_start(
    handle: &ProxyHandle,
    cmd: &zero_api::TunStartCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    with_current_runtime("no tokio runtime available for TUN command", |rt| {
        rt.block_on(start(handle, cmd))
    })
}
pub(super) fn execute_tun_stop(
    handle: &ProxyHandle,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    with_current_runtime("no tokio runtime available for TUN command", |rt| {
        rt.block_on(stop(handle))
    })
}
pub(super) fn execute_tun_recover(
    handle: &ProxyHandle,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    with_current_runtime("no tokio runtime available for TUN command", |rt| {
        rt.block_on(recover(handle))
    })
}
