#![cfg(feature = "socks5")]

use super::*;
use crate::{
    transport::{channel_failure, failure_origin, TransportFailureOrigin},
    Proxy,
};

#[tokio::test]
async fn a_datagram_reference_to_another_actual_owner_keeps_stale_plan_and_local_origin() {
    let config = zero_config::RuntimeConfig::parse(
        r#"{
        "outbounds": [
            {"tag":"first","protocol":{"type":"socks5","server":"127.0.0.1","port":1080}},
            {"tag":"second","protocol":{"type":"socks5","server":"127.0.0.1","port":1081}}
        ],
        "route":{"final":{"type":"route","outbound":"first"}}
    }"#,
    )
    .unwrap();
    let proxy = Proxy::new(config).unwrap();
    let execution = proxy.tcp_runtime_services().execution();
    let mut target =
        DatagramAssociationTarget::resolve(&execution, "first", "127.0.0.1", 1080).unwrap();
    let second = execution
        .components
        .as_ref()
        .unwrap()
        .configured("second")
        .unwrap();
    assert_ne!(target.instance, second.instance_id());
    // Both identities come from the actual host allocator. This is a stale
    // metadata admission test, not resource acquisition or close evidence.
    target.instance = second.instance_id();
    let failure = target
        .current_slot(&execution)
        .err()
        .expect("wrong owner must be rejected before I/O");
    let channel = channel_failure(&failure).expect("keep the standard typed cause");
    assert_eq!(channel.stage, Stage::Prepare);
    assert_eq!(channel.reason, Reason::StalePlan);
    assert_eq!(
        failure_origin(&failure),
        Some(TransportFailureOrigin::LocalNetwork)
    );
}
