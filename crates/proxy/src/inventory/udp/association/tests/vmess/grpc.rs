//! Ordinary production selection, real H2 wire and actual native-close evidence.
use super::*;
#[path = "grpc/fixtures.rs"]
mod fixtures;
#[path = "grpc/flow.rs"]
pub(super) mod flow;
#[path = "grpc/network.rs"]
mod network;
#[path = "grpc/ownership.rs"]
mod ownership;
#[path = "grpc/replacement.rs"]
mod replacement;
use fixtures::*;

fn assert_retained_rejection(
    error: &zero_engine::EngineError,
    setup: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    stage: zero_platform_tokio::contract::Stage,
    reason: zero_platform_tokio::contract::Reason,
    delivery: zero_platform_tokio::contract::DeliveryState,
) {
    use zero_platform_tokio::contract::{ChannelError, RetryClass};

    let zero_engine::EngineError::Io(error) = error else {
        panic!("expected the original rejected operation: {error:?}");
    };
    let error = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ChannelError>())
        .expect("original channel error");
    assert_eq!(
        (error.stage, error.reason, error.delivery),
        (stage, reason, delivery)
    );
    assert_eq!(error.retry, RetryClass::Never);
    let joined = setup.try_joined().expect("actual setup joined");
    assert!(setup.matches_joined(&joined));
    assert!(joined.failure_accepted() && joined.registration_released());
    let failed = joined.result().unwrap_err();
    assert!(std::sync::Arc::ptr_eq(
        error.retained_source().expect("actual caller rejection"),
        failed.retained_source().expect("actual task rejection"),
    ));
}

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

#[path = "grpc/transition.rs"]
mod transition;
