use std::time::Duration;

use tokio::io::sink;

use super::copy_one_way;
use crate::transport::SharedRateLimiter;

#[tokio::test]
async fn separate_tcp_copies_observe_one_shared_upload_timeline() {
    let limiter = SharedRateLimiter::new(1);
    let first_payload = vec![0_u8; 16 * 1024];
    copy_one_way(
        first_payload.as_slice(),
        sink(),
        |_| {},
        Some(limiter.clone()),
    )
    .await
    .expect("first TCP copy");

    let second_payload = vec![0_u8; 16 * 1024];
    assert!(
        tokio::time::timeout(
            Duration::from_millis(5),
            copy_one_way(second_payload.as_slice(), sink(), |_| {}, Some(limiter)),
        )
        .await
        .is_err(),
        "second TCP copy did not observe debt from the first copy"
    );
}
