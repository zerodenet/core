use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_config::OutboundConfig;
use zero_traits::ProtocolMetadata;

use super::{udp::WireguardRawIpPlan, WireguardAdapter};
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
fn wireguard_outbound_selects_its_actual_standard_factory() {
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[outbound(1)],
            "route":{"final":{"type":"route","outbound":"wg"}}
        })
        .to_string(),
    )
    .unwrap();
    let (_, factories, _) = crate::register::compiled_registries();
    assert!(factories.has_factory_for("wireguard", &config.outbounds[0]));
}

#[test]
fn wireguard_capabilities_report_current_endpoint_limits() {
    let descriptor = WireguardAdapter::default().descriptor();
    assert!(descriptor
        .limitations
        .contains(&"opaque_outer_carrier_has_no_observed_source_for_roaming"));
    assert!(!descriptor
        .limitations
        .contains(&"link_or_unlink_running_endpoint_requires_restart"));
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

#[test]
fn wireguard_health_reports_configured_peer_before_device_start() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use zero_api::{QueryRequest, QueryResponse, QueryService};
    use zero_config::RuntimeConfig;
    use zero_engine::EngineHandle;

    use crate::runtime::{Proxy, ProxyHandle};
    use gotatun::x25519::{PublicKey, StaticSecret};
    let public_key = STANDARD.encode(PublicKey::from(&StaticSecret::from([2; 32])).as_bytes());
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds": [{
                "tag": "wg",
                "protocol": {
                    "type": "wireguard",
                    "private_key": STANDARD.encode([1; 32]),
                    "addresses": ["10.0.0.1/32"],
                    "mtu": 1420,
                    "peers": [{
                        "public_key": public_key,
                        "endpoint": "127.0.0.1:51820",
                        "allowed_ips": ["0.0.0.0/0"]
                    }]
                }
            }],
            "route": {"rules": [], "final": {"type": "direct"}}
        })
        .to_string(),
    )
    .expect("parse config");
    let proxy = Proxy::new(config).expect("build proxy");
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy);
    let response = handle
        .query(QueryRequest::Health(Default::default()))
        .expect("query health");
    let QueryResponse::Health(health) = response else {
        panic!("expected health response")
    };
    assert_eq!(health.outbound_devices.len(), 1);
    assert_eq!(health.outbound_devices[0].tag, "wg");
    assert_eq!(
        health.outbound_devices[0].state,
        zero_api::OutboundDeviceHealthState::NotStarted
    );
}
