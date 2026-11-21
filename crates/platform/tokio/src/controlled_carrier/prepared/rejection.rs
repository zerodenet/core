//! A failed native connect is retryable only after its own FD really closes.
use super::*;
pub(super) async fn reject_connect(
    slot: &Arc<Slot>,
    peer: std::net::SocketAddr,
    source: Arc<crate::TcpConnectError>,
    end: Instant,
) -> ChannelError {
    let native_rejection = source.stage() == "connect_socket";
    let mut failure = connect_error(slot.instance, peer, source);
    if !native_rejection || failure.reason != Reason::Io {
        return failure;
    }
    let control = { slot.state().control.clone() };
    let Some(control) = control else {
        return failure;
    };
    let closed = tokio::time::timeout_at(end, async {
        let mut control = control.lock().await;
        control.close_without_receipt(end).await
    })
    .await;
    if matches!(closed, Ok(Ok(true))) {
        // No stream escaped and no TLS/protocol preparation was polled.
        // Native/user/initializer/caller receipts remain on the same slot;
        // this grants no capacity refund or successful slot-close claim.
        failure.delivery = DeliveryState::NotSubmitted;
        failure.retry = crate::contract::RetryClass::BeforeSubmission;
    }
    failure
}
