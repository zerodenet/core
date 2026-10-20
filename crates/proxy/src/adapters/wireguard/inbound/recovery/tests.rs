use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use zero_config::RuntimeConfig;
use zero_platform_tokio::contract::component::ResourceDeclaration;

fn config() -> InboundConfig {
    serde_json::from_value(serde_json::json!({
        "tag":"wg", "listen":{"address":"127.0.0.1","port":0},
        "protocol":{"type":"wireguard","private_key":STANDARD.encode([91;32]),
            "addresses":["10.0.0.1/32"],"mtu":1420,
            "peers":[{"public_key":STANDARD.encode(PublicKey::from(&StaticSecret::from([92;32])).as_bytes()),
                "allowed_ips":["10.0.0.2/32"]}]}
    })).unwrap()
}

#[test]
fn passive_restore_and_port_move_keep_the_actual_crypto_owner_and_finite_declarations() {
    let adapter = super::super::super::WireguardAdapter::default();
    let inbound = config();
    let live = adapter.passive_inbound_live(&inbound).unwrap();
    let recovery = Recovery::new(
        &inbound,
        live.clone(),
        Binding::Standalone,
        adapter.pending_endpoints.clone(),
    )
    .unwrap();
    let original_incarnation = live.incarnation();
    let mut moved = inbound.clone();
    moved.listen.port = 51821;
    let cold = recovery
        .prepare_replacement(moved.clone(), None)
        .unwrap()
        .unwrap();
    let destination =
        zero_core::request::Destination::new(zero_core::Address::Ipv4([127, 0, 0, 1]), 51821);
    assert!(
        matches!(cold.resources(), [ResourceDeclaration::DatagramListener {
        destination: actual, max_bind_attempts, max_pending_sends,
    }, ResourceDeclaration::NativeIcmp { max_sockets }]
        if actual == &destination && max_bind_attempts.get() == 50
            && max_pending_sends.get() == 1 && max_sockets.get() == 64)
    );
    assert!(Arc::ptr_eq(
        &adapter.passive_inbound_live(&moved).unwrap(),
        &live
    ));
    assert_eq!(live.incarnation(), original_incarnation);
    assert!(!live.listener_active());
    assert!(recovery.prepare_restore(moved, None).is_err());
    let mut other = inbound.clone();
    let InboundProtocolConfig::Wireguard { private_key, .. } = &mut other.protocol else {
        unreachable!()
    };
    *private_key = serde_json::from_value(serde_json::json!(STANDARD.encode([93; 32]))).unwrap();
    assert!(recovery.prepare_replacement(other, None).unwrap().is_none());
    assert!(recovery.prepare_restore(inbound, None).is_ok());
}

#[test]
fn restore_requires_ready_same_crypto_candidate_and_rejects_another_actual_owner() {
    let adapter = super::super::super::WireguardAdapter::default();
    let inbound = config();
    let live = adapter.passive_inbound_live(&inbound).unwrap();
    let recovery = Recovery::new(
        &inbound,
        live.clone(),
        Binding::Standalone,
        adapter.pending_endpoints.clone(),
    )
    .unwrap();
    let pending = Arc::new(super::super::super::lifecycle::PendingLinkedEndpoint::new(
        inbound.listen.clone(),
        live,
    ));
    *adapter.pending_endpoints.lock().unwrap() = Some(std::collections::HashMap::from([(
        inbound.tag.clone(),
        pending,
    )]));
    assert!(recovery.prepare_restore(inbound.clone(), None).is_err());
    let another = Arc::new(LiveInboundDevice::new(
        prepare_device(&inbound).unwrap(),
        inbound_protocol_identity(&inbound.protocol).unwrap(),
    ));
    *adapter.pending_endpoints.lock().unwrap() = None;
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let (sender, _) = watch::channel(sender);
    let link = Arc::new(LinkedEndpoint::new(
        [3; 32],
        7,
        inbound.listen.clone(),
        another,
        vec![],
        vec![],
        vec![],
        sender,
        receiver,
    ));
    let recovery = Recovery::new(
        &inbound,
        adapter.passive_inbound_live(&inbound).unwrap(),
        Binding::Ready(link),
        adapter.pending_endpoints.clone(),
    )
    .unwrap();
    assert!(recovery.prepare_restore(inbound, None).is_err());
}

#[tokio::test]
async fn original_udp_pending_join_and_publication_precede_same_crypto_restore() {
    use crate::protocol_registry::InboundListenerCapability;
    use crate::runtime::{
        inbound_owners::{InboundOwnerRegistry, NativeListenerView},
        Proxy,
    };
    use std::{future::Future, num::NonZeroUsize, task::Poll, time::Duration};
    use tokio::{
        net::UdpSocket,
        sync::{oneshot, Notify},
        time::{timeout, Instant},
    };
    let adapter = super::super::super::WireguardAdapter::default();
    let inbound = config();
    let proxy = Proxy::new(
        RuntimeConfig::parse(
            &serde_json::json!({"inbounds":[inbound], "route":{"final":{"type":"direct"}}})
                .to_string(),
        )
        .unwrap(),
    )
    .unwrap();
    let config = proxy.engine().config();
    let inbound = config.inbounds[0].clone();
    let live = adapter.passive_inbound_live(&inbound).unwrap();
    let original_incarnation = live.incarnation();
    let owners = Arc::new(InboundOwnerRegistry::default());
    let prepared = adapter
        .prepare_inbound_bind(inbound.clone(), None)
        .unwrap()
        .unwrap();
    let (shutdown, _) = watch::channel(false);
    let mut lease = owners
        .reserve_listener(
            "wg",
            &config,
            &proxy.protocols,
            prepared,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    lease.bind_native().unwrap();
    let (mut operation, view, installed) = lease.finish().unwrap().promote_native().unwrap();
    let owner = installed.owner();
    owner
        .install_recovery(operation.recovery_state().unwrap())
        .unwrap();
    operation.install_resources(&installed).unwrap();
    owners.retain_recovery("wg", &inbound).unwrap();
    let NativeListenerView::Datagram(
        zero_platform_tokio::contract::component::GrantedDatagramListener { mut rx, tx },
    ) = view
    else {
        panic!("actual UDP grant required")
    };
    let address = rx.local_addr().unwrap();
    assert!(live.listener_active());
    let entered = Arc::new(Notify::new());
    let notified = entered.clone();
    let (resume, release) = oneshot::channel();
    owner
        .spawn_driver(async move {
            let mut bytes = [0; 64];
            let read = rx.recv_from_addr(&mut bytes);
            tokio::pin!(read);
            let result = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => {
                    notified.notify_one();
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await;
            assert!(result.is_err());
            release.await.map_err(std::io::Error::other)?;
            drop(tx);
            drop(operation);
            Ok(())
        })
        .unwrap();
    owner.seal_listener_installation();
    timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert!(owners
        .prepare_recovery_bind(inbound.clone(), None, true)
        .is_err());
    let mut closing = Box::pin(owner.shutdown(Instant::now() + Duration::from_secs(2)));
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    let probe = UdpSocket::bind(address).await.unwrap();
    assert!(owners
        .prepare_recovery_bind(inbound.clone(), None, true)
        .is_err());
    assert!(live.listener_active()); // Original real driver still owns its metadata lease.
    resume.send(()).unwrap();
    let receipt = owners
        .confirm_shutdown("wg", Instant::now() + Duration::from_secs(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.terminal().identity(), owner.identity());
    assert_eq!(receipt.terminal().joined_tasks(), 1);
    assert_eq!(receipt.terminal().closed_io_receipts(), 1);
    assert!(owners
        .prepare_recovery_bind(inbound.clone(), None, true)
        .is_err());
    owners.release_closed(receipt, |_| Ok(())).unwrap();
    assert!(!live.listener_active());
    let cold = owners
        .prepare_recovery_bind(inbound.clone(), None, true)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(
        &adapter.passive_inbound_live(&inbound).unwrap(),
        &live
    ));
    assert_eq!(live.incarnation(), original_incarnation);
    assert_eq!(probe.local_addr().unwrap(), address);
    drop(probe);
    let (shutdown, _) = watch::channel(false);
    let mut restored = owners
        .reserve_listener(
            "wg",
            &config,
            &proxy.protocols,
            cold,
            shutdown,
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
    assert_ne!(restored.owner().identity(), owner.identity());
    restored.bind_native().unwrap();
    drop(restored);
    owners
        .retire_bind_reservation_reported("wg", Instant::now() + Duration::from_secs(2), |_| Ok(()))
        .await
        .unwrap();
}

#[test]
fn capsule_uses_current_actual_private_identity_after_an_accepted_hot_update() {
    let adapter = super::super::super::WireguardAdapter::default();
    let inbound = config();
    let live = adapter.passive_inbound_live(&inbound).unwrap();
    let capsule = Recovery::new(
        &inbound,
        live.clone(),
        Binding::Standalone,
        adapter.pending_endpoints.clone(),
    )
    .unwrap();
    let mut accepted = inbound.clone();
    let InboundProtocolConfig::Wireguard { private_key, .. } = &mut accepted.protocol else {
        unreachable!()
    };
    *private_key = serde_json::from_value(serde_json::json!(STANDARD.encode([93; 32]))).unwrap();
    // Same operation used by the actual accepted same-port hot update. The
    // retained capsule cannot authorize the old private identity afterward.
    live.replace(
        prepare_device(&accepted).unwrap(),
        inbound_protocol_identity(&accepted.protocol).unwrap(),
    );
    adapter.cache_inbound_live(&accepted, &live).unwrap();
    assert!(capsule.prepare_restore(inbound.clone(), None).is_err());
    assert!(capsule
        .prepare_replacement(inbound, None)
        .unwrap()
        .is_none());
    assert!(capsule.prepare_restore(accepted.clone(), None).is_ok());
    assert!(Arc::ptr_eq(
        &adapter.passive_inbound_live(&accepted).unwrap(),
        &live
    ));
    assert_eq!(adapter.inbound_devices.lock().unwrap().len(), 1);
}
