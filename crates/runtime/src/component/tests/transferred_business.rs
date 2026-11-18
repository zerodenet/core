//! Admission fencing cancels Business while real protocol cleanup keeps its loan.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::component::SessionId;

#[tokio::test]
async fn retirement_fence_cancels_pending_business_io_before_ordered_service_revocation() {
    let users = Arc::new(Users::new(ComponentId(71)));
    users.bind(true);
    let (business, _business_peer) = tokio::io::duplex(64);
    let mut business = users
        .enter(Some(SessionId(9)))
        .unwrap()
        .unwrap()
        .stream(Box::pin(business));
    let (service, mut peer) = tokio::io::duplex(64);
    let mut service = users
        .enter(None)
        .unwrap()
        .unwrap()
        .stream(Box::pin(service));
    {
        let mut byte = [0];
        let pending = business.read(&mut byte);
        tokio::pin!(pending);
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        users.fence_business();
        assert_eq!(
            pending.await.unwrap_err().kind(),
            std::io::ErrorKind::ConnectionAborted
        );
    }
    assert!(users.enter(Some(SessionId(10))).is_err());
    assert!(
        users.enter(None).is_err(),
        "no new Service loan enters a fenced generation"
    );
    peer.write_all(b"original protocol EOF work").await.unwrap();
    let expected = b"original protocol EOF work";
    let mut actual = vec![0; expected.len()];
    service.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, expected);
    assert!(!users.wait_idle(Instant::now()).await.is_complete());
    users.revoke();
    assert_eq!(
        service.read(&mut [0]).await.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    drop((business, service, peer));
    let report = users.wait_idle(Instant::now()).await;
    assert!(report.is_complete());
    assert!(
        report.closed_io.is_empty(),
        "logical custody never mints native receipts"
    );
}
