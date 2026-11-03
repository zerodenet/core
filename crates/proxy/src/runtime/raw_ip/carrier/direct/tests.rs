use super::*;
use std::{future::Future, task::Poll};

#[tokio::test]
async fn cancelled_original_close_wait_retains_socket_until_real_receive_releases_it() {
    let original = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let carrier = DirectRawIpWireCarrier::new(original);
    let mut bytes = [0; 16];
    let mut receive = Box::pin(carrier.recv(&mut bytes));
    std::future::poll_fn(|cx| {
        assert!(receive.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let mut close = Box::pin(carrier.close_native());
    std::future::poll_fn(|cx| {
        assert!(close.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(close);
    assert!(
        carrier.close.lock().unwrap().is_none(),
        "cancelled waiter cannot claim a native outcome"
    );
    drop(receive);
    carrier.close_native().await.unwrap();
    assert!(carrier.original.read().await.is_none());
    assert!(
        matches!(*carrier.close.lock().unwrap(), Some(Ok(()))),
        "consume the actual original socket close result"
    );
    carrier.close_native().await.unwrap();
    assert!(carrier.recv(&mut bytes).await.is_err());
    assert!(carrier
        .send(b"stale", "127.0.0.1:9".parse().unwrap())
        .await
        .is_err());
}
