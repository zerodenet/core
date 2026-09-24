use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use boringtun::x25519::{PublicKey, StaticSecret};
use zero_config::OutboundConfig;

use super::{protocol_identity, udp::WireguardRawIpPlan, CachedProfile, WireguardAdapter};
use crate::protocol_registry::OutboundLeafInput;
use crate::runtime::raw_ip::{RawIpOutboundPlan, SharedRawIpDevice};
use zero_platform_tokio::TokioDatagramSocket;

mod raw_ip;

fn outbound(private: u8) -> OutboundConfig {
    let peer_public = STANDARD.encode(PublicKey::from(&StaticSecret::from([2; 32])).as_bytes());
    serde_json::from_value(serde_json::json!({
        "tag": "wg",
        "protocol": {
            "type": "wireguard",
            "private_key": STANDARD.encode([private; 32]),
            "addresses": ["10.0.0.1/32"],
            "mtu": 1420,
            "peers": [{
                "public_key": peer_public,
                "endpoint": "127.0.0.1:51820",
                "allowed_ips": ["0.0.0.0/0"]
            }]
        }
    }))
    .unwrap()
}

#[test]
fn wireguard_adapter_claims_without_mutating_published_profile() {
    let adapter = WireguardAdapter::default();
    let first = outbound(1);
    assert!(adapter
        .claim_outbound_leaf_impl(OutboundLeafInput::Virtual { outbound: &first })
        .is_some());
    let first_plan = Arc::new(WireguardRawIpPlan::from_protocol(&first.protocol).unwrap());
    adapter.profiles.lock().unwrap().insert(
        "wg".to_owned(),
        CachedProfile {
            identity: protocol_identity(&first.protocol).unwrap(),
            plan: first_plan.clone(),
        },
    );
    adapter
        .claim_outbound_leaf_impl(OutboundLeafInput::Virtual { outbound: &first })
        .unwrap();
    adapter
        .claim_outbound_leaf_impl(OutboundLeafInput::Virtual { outbound: &first })
        .unwrap();
    let second_plan = adapter
        .profiles
        .lock()
        .unwrap()
        .get("wg")
        .unwrap()
        .plan
        .clone();
    assert!(Arc::ptr_eq(&first_plan, &second_plan));

    let changed = outbound(3);
    assert!(adapter
        .claim_outbound_leaf_impl(OutboundLeafInput::Virtual { outbound: &changed })
        .is_some());
    assert!(Arc::ptr_eq(
        &first_plan,
        &adapter.profiles.lock().unwrap().get("wg").unwrap().plan
    ));
}

#[tokio::test]
async fn wireguard_device_shutdown_releases_protocol_profile() {
    let outbound = outbound(1);
    let plan = WireguardRawIpPlan::from_protocol(&outbound.protocol).unwrap();
    let weak = plan.profile_weak();
    let tunnel = plan.build_tunnel(0).unwrap();
    let socket = TokioDatagramSocket::bind_addr("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let device = SharedRawIpDevice::start(
        plan.local_addresses(),
        plan.mtu(),
        "127.0.0.1:51820".parse().unwrap(),
        socket,
        tunnel,
    )
    .unwrap();
    device.wait_ready().await.unwrap();
    drop(plan);
    assert!(weak.upgrade().is_some());
    device.close_now();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closed device releases protocol profile");
}
