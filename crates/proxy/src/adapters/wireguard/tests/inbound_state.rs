use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};

fn config(private: u8) -> InboundConfig {
    serde_json::from_value(serde_json::json!({
        "tag":"wg", "listen":{"address":"127.0.0.1","port":51820},
        "protocol":{"type":"wireguard","private_key":STANDARD.encode([private;32]),
            "addresses":["10.0.0.1/32"],"mtu":1420,
            "peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([92;32])).as_bytes()),
                "allowed_ips":["10.0.0.2/32"]}]}
    })).unwrap()
}

#[test]
fn passive_candidates_preserve_same_private_owner_and_prune_unowned_keys() {
    let adapter = WireguardAdapter::default();
    let original = config(91);
    let live = adapter.passive_inbound_live(&original).unwrap();
    let incarnation = live.incarnation();
    assert!(adapter.active_inbound_live("wg").is_none());
    let lease = live.claim_listener().unwrap();
    assert!(live.claim_listener().is_err());
    let mut next = original.clone();
    next.listen.port += 1;
    let InboundProtocolConfig::Wireguard { mtu, .. } = &mut next.protocol else {
        unreachable!()
    };
    *mtu = 1280;
    let selected = adapter.passive_inbound_live(&next).unwrap();
    assert!(Arc::ptr_eq(&selected, &live));
    assert!(live.matches_identity(inbound_protocol_identity(&original.protocol).unwrap()));
    assert!(!live.matches_identity(inbound_protocol_identity(&next.protocol).unwrap()));
    assert_eq!(live.incarnation(), incarnation);
    let changed = adapter.passive_inbound_live(&config(93)).unwrap();
    assert!(!Arc::ptr_eq(&changed, &live));
    assert!(Arc::ptr_eq(
        &adapter.active_inbound_live("wg").unwrap(),
        &live
    ));
    assert_eq!(adapter.inbound_devices.lock().unwrap().len(), 2);
    drop(changed);
    adapter.prune_inbound_devices();
    assert_eq!(adapter.inbound_devices.lock().unwrap().len(), 1);
    drop(lease);
    assert!(adapter.active_inbound_live("wg").is_none());
    // Removing metadata does not remove the retained crypto owner/history.
    assert_eq!(live.incarnation(), incarnation);
    drop(selected);
    drop(live);
    adapter.prune_inbound_devices();
    assert!(adapter.inbound_devices.lock().unwrap().is_empty());
}

#[test]
fn cold_listener_preparation_never_claims_running_listener_or_mutates_device() {
    let adapter = WireguardAdapter::default();
    let original = config(91);
    let live = adapter.passive_inbound_live(&original).unwrap();
    let lease = live.claim_listener().unwrap();
    let mut next = original.clone();
    next.listen.port += 1;
    let InboundProtocolConfig::Wireguard { mtu, .. } = &mut next.protocol else {
        unreachable!()
    };
    *mtu = 1280;
    let cold = adapter.prepare_wireguard_listener(next, None).unwrap();
    assert!(live.listener_active());
    assert!(live.matches_identity(inbound_protocol_identity(&original.protocol).unwrap()));
    drop(cold);
    assert!(live.listener_active());
    drop(lease);
    assert!(!live.listener_active());
}
