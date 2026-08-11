use super::super::ProxyHandle;
use super::runtime::with_current_runtime;
use crate::{TunInterfaceOptions, TunRuntimeOptions};

pub(super) fn execute_tun_start(
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
    let dual_stack = cmd.dual_stack;
    let strict_route = cmd.strict_route;
    let dns_hijack = cmd.dns_hijack;

    with_current_runtime("no tokio runtime available for TUN command", |rt| {
        rt.block_on(async move {
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
                        dual_stack,
                        strict_route,
                        dns_hijack,
                    },
                )
                .await
                .map(|_| zero_api::CommandResponse::accepted())
                .map_err(|error| {
                    zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error.to_string())
                })
        })
    })
}

pub(super) fn execute_tun_stop(
    handle: &ProxyHandle,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let proxy = handle.proxy.clone();

    with_current_runtime("no tokio runtime available for TUN command", |rt| {
        rt.block_on(async move {
            proxy
                .stop_tun()
                .await
                .map(|_| zero_api::CommandResponse::accepted())
                .map_err(|error| {
                    zero_api::ApiError::new(zero_api::ApiErrorCode::Internal, error.to_string())
                })
        })
    })
}
