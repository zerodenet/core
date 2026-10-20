use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use tokio::sync::{mpsc, watch};

fn candidate() -> (zero_config::InboundConfig, Arc<LiveInboundDevice>) {
    let config: zero_config::InboundConfig = serde_json::from_value(serde_json::json!({
        "tag":"wg-in", "listen":{"address":"127.0.0.1", "port":51820},
        "protocol":{"type":"wireguard", "private_key":STANDARD.encode([1;32]),
            "peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([2;32])).as_bytes()),
                "allowed_ips":["10.0.0.2/32"]}]}
    })).unwrap();
    let device = super::super::super::inbound::prepare_device(&config).unwrap();
    let identity = super::super::super::inbound_protocol_identity(&config.protocol).unwrap();
    let live = Arc::new(LiveInboundDevice::new(device, identity));
    (config, live)
}

#[test]
fn passive_candidate_has_no_ready_resource_and_never_takes_the_actual_packet_receiver() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let (config, live) = candidate();
    let pending = PendingLinkedEndpoint::new(config.listen.clone(), live.clone());
    assert!(Arc::ptr_eq(&pending.live(), &live));
    assert!(pending.ready().is_none());
    let (sender, receiver) = mpsc::channel(1);
    let (sender, _) = watch::channel(sender);
    let link = Arc::new(LinkedEndpoint::new(
        [3; 32],
        7,
        config.listen,
        live,
        vec![],
        vec![],
        vec![],
        sender,
        receiver,
    ));
    pending.install(link.clone()).unwrap();
    assert!(Arc::ptr_eq(&pending.ready().unwrap(), &link));
    assert!(pending.install(link.clone()).is_err());
    let (operation, retained_live) = super::super::super::inbound::prepare_linked(&link).unwrap();
    assert!(Arc::ptr_eq(&retained_live, &pending.live()));
    drop(operation);
}

#[test]
fn same_listen_with_different_actual_crypto_history_cannot_be_installed() {
    let (config, live) = candidate();
    let pending = PendingLinkedEndpoint::new(config.listen.clone(), live);
    let (_, different) = candidate();
    let (sender, receiver) = mpsc::channel(1);
    let (sender, _) = watch::channel(sender);
    let link = Arc::new(LinkedEndpoint::new(
        [3; 32],
        7,
        config.listen,
        different,
        vec![],
        vec![],
        vec![],
        sender,
        receiver,
    ));
    assert!(pending.install(link).is_err());
    assert!(pending.ready().is_none());
}

#[test]
fn cold_candidate_declares_exact_peer_slots_without_runtime_or_native_acquisition() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let (config, live) = candidate();
    let pending = PendingLinkedEndpoint::with_peer_slots(config.listen, live, 2).unwrap();
    let first = pending.peer_carrier(0).unwrap();
    let second = pending.peer_carrier(1).unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(&first, &pending.peer_carrier(0).unwrap()));
    assert!(pending.peer_carrier(2).is_none());
    assert!(pending.ready().is_none());
    let report = first.close_native();
    assert!(report.is_complete());
    assert_eq!(report.native.confirmed_closed, 0);
    assert_eq!(report.native.known_no_native, 0);
    assert!(second.close_native().is_complete());
}
