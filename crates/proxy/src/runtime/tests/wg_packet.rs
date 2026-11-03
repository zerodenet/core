//! Actual Packet source, original UDP custody and same-port replacement.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::time::Instant;
fn config() -> zero_config::RuntimeConfig {
    zero_config::RuntimeConfig::parse(&serde_json::json!({"inbounds":[{"tag":"wg-standard","listen":{"address":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":STANDARD.encode([81;32]),"addresses":["10.0.0.1/32"],"mtu":1280,"peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([82;32])).as_bytes()),"allowed_ips":["10.0.0.2/32"]}]}}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap()
}
#[tokio::test]
async fn actual_packet_source_closes_original_view_and_scope_before_same_port_replacement() {
    let proxy = Proxy::new(config()).unwrap();
    let mut inbound = proxy.engine().config().inbounds[0].clone();
    let mut original = None;
    let listener = super::test_listener::start_with_original(&proxy, inbound.clone(), |view| {
        let crate::runtime::inbound_owners::NativeListenerView::Datagram(grant) = view else {
            panic!("actual UDP grant")
        };
        original = Some(grant.tx.clone());
    })
    .await;
    assert!(
        listener.owner.is_ready(),
        "actual native receive was polled pending"
    );
    assert!(proxy.engine().active_sessions().is_empty());
    assert!(proxy.engine().completed_sessions().is_empty());
    let address = listener.address;
    let owner = listener.owner.clone();
    assert!(proxy
        .protocols
        .prepare_inbound_bind(inbound.clone(), None)
        .unwrap()
        .is_some());
    proxy
        .inbound_owners
        .retain_recovery(&inbound.tag, &inbound)
        .unwrap();
    assert!(proxy
        .inbound_owners
        .prepare_recovery_bind(inbound.clone(), None, true)
        .is_err());
    super::test_listener::close(&proxy, &inbound.tag, listener).await;
    let terminal = owner.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none());
    assert!(terminal.joined_tasks() >= 2);
    assert_eq!(terminal.closed_io_receipts(), 1);
    assert!(
        original
            .unwrap()
            .send_to_addr(b"retired", address)
            .await
            .is_err(),
        "transferred original view is retired"
    );
    inbound.listen.port = address.port();
    let cold = proxy
        .inbound_owners
        .prepare_recovery_bind(proxy.engine().config().inbounds[0].clone(), None, true)
        .unwrap()
        .unwrap();
    assert!(cold
        .into_recipe()
        .spec()
        .roles
        .contains(&zero_platform_tokio::contract::component::RoleKind::Inbound));
    let mut next = proxy.engine().config().as_ref().clone();
    next.inbounds[0] = inbound.clone();
    // A new Proxy is unnecessary: the same catalog and actual metadata owner
    // recover after the old terminal was published by this same host.
    let next = Arc::new(next);
    let prepared = proxy
        .protocols
        .prepare_inbound_bind(inbound.clone(), None)
        .unwrap()
        .unwrap();
    let (stop, _) = tokio::sync::watch::channel(false);
    let mut lease = proxy
        .inbound_owners
        .reserve_listener(
            &inbound.tag,
            &next,
            &proxy.protocols,
            prepared,
            stop,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.bind_native().unwrap();
    drop(lease);
    proxy
        .inbound_owners
        .retire_bind_reservation_reported(
            &inbound.tag,
            Instant::now() + Duration::from_secs(3),
            |closed| {
                assert_eq!(closed.closed_io_receipts(), 1);
                Ok(())
            },
        )
        .await
        .unwrap();
}
