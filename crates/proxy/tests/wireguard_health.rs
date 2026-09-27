#![cfg(feature = "wireguard")]

use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_api::{QueryRequest, QueryResponse, QueryService};
use zero_config::RuntimeConfig;
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle};

#[test]
fn wireguard_health_reports_configured_peer_before_device_start() {
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
