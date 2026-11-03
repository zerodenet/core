//! External CA uses the ordinary production catalog and actual File owner.
use super::*;
#[path = "ca/failures.rs"]
mod failures;
#[path = "ca/replacement.rs"]
mod replacement;
#[tokio::test]
async fn ordinary_vmess_grpc_ca_datagram_dispatch_trusts_relative_file_snapshot_for_both_modes() {
    timeout(Duration::from_secs(15), async {
        for multi in [false, true] {
            let payloads = vec![b"grpc".to_vec(), vec![], vec![9; 37000]];
            let (proxy, server) = start_count_ca(
                true,
                multi,
                "aes-128-gcm",
                "node",
                payloads.clone(),
                1,
                true,
            )
            .await;
            // Fixture removed the CA file after actual host start. New demand
            // must use the original authorized snapshot, not reopen the path.
            exercise_datagram(proxy, server, payloads).await;
        }
    })
    .await
    .unwrap();
}
