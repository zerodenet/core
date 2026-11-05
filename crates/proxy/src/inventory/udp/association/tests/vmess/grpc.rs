//! Ordinary production selection, real H2 wire and actual native-close evidence.
use super::*;
#[path = "grpc/fixtures.rs"]
mod fixtures;
#[path = "grpc/network.rs"]
mod network;
#[path = "grpc/ownership.rs"]
mod ownership;
#[path = "grpc/replacement.rs"]
mod replacement;
use fixtures::*;

#[tokio::test]
async fn ordinary_vmess_grpc_datagram_dispatch_preserves_modes_ciphers_and_actual_flow_accounting()
{
    timeout(Duration::from_secs(20), async {
        for tls in [false, true] {
            for multi_mode in [false, true] {
                for (cipher, target) in [
                    ("aes-128-gcm", "node"),
                    ("zero", "choice"),
                    ("chacha20-poly1305", "backup"),
                ] {
                    let payloads = vec![b"grpc".to_vec(), vec![], vec![9; 37000]];
                    let (proxy, server) =
                        start(tls, multi_mode, cipher, target, payloads.clone()).await;
                    exercise_datagram(proxy, server, payloads).await;
                }
            }
        }
    })
    .await
    .unwrap();
}

#[path = "grpc/ca.rs"]
mod ca;

#[path = "grpc/dns.rs"]
mod dns;

#[cfg(feature = "socks5")]
#[path = "grpc/relay.rs"]
mod relay;

#[path = "grpc/stream.rs"]
mod stream;
