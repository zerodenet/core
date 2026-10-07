use super::super::ProxyHandle;
pub(super) async fn close(
    handle: ProxyHandle,
    request: zero_api::PacketRouteCloseCommand,
) -> zero_api::ApiResult<zero_api::CommandResponse> {
    let _guard = handle.proxy.reload_apply_lock.lock().await;
    let (snapshot, control) = handle.proxy.engine().begin_close_packet_route(&request)?;
    if tokio::time::timeout(std::time::Duration::from_secs(5), control.wait_released())
        .await
        .is_err()
    {
        // Keep the eventual receipt/event even when the acknowledgement times out.
        // The instance-bound ID and atomic close claim prevent a second waiter.
        let engine = handle.proxy.engine().clone();
        tokio::spawn(async move {
            control.wait_released().await;
            engine.record_packet_route_closed(snapshot);
        });
        return Err(zero_api::ApiError::new(
            zero_api::ApiErrorCode::Internal,
            "packet route close unconfirmed; query current state",
        ));
    }
    let snapshot = handle.proxy.engine().record_packet_route_closed(snapshot);
    Ok(zero_api::CommandResponse {
        accepted: true,
        result: Some(serde_json::json!({ "route": snapshot, "closed": true })),
    })
}
