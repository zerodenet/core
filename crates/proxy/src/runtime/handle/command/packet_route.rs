use super::super::ProxyHandle;
pub(super) async fn close(
    handle: ProxyHandle,
    request: zero_api::PacketRouteCloseCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    handle
        .proxy
        .packet_close_owner
        .close(
            handle.proxy.engine().clone(),
            request,
            handle.proxy.reload_apply_lock.clone(),
            std::time::Duration::from_secs(5),
        )
        .await
}
