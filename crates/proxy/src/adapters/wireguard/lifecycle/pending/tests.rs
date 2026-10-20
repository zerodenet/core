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
    let stack = pending.peer_stack(0).unwrap();
    assert!(Arc::ptr_eq(&stack, &pending.peer_stack(0).unwrap()));
    assert!(!Arc::ptr_eq(&stack, &pending.peer_stack(1).unwrap()));
    assert!(pending.peer_stack(2).is_none());
    assert!(pending.ready().is_none());
    let report = first.close_native();
    assert!(report.is_complete());
    assert_eq!(report.native.confirmed_closed, 0);
    assert_eq!(report.native.known_no_native, 0);
    assert!(second.close_native().is_complete());
}

#[tokio::test]
async fn same_candidate_joins_declared_original_stacks_before_confirming_unused_native_slots() {
    let (config, live) = candidate();
    let pending =
        PendingLinkedEndpoint::with_peer_slots(config.listen.clone(), live.clone(), 1).unwrap();
    let (sender, mut packets) = mpsc::channel(4);
    let (updates, endpoint) = watch::channel(sender);
    let recipe = crate::runtime::raw_ip::EndpointStackRecipe::prepare(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        0,
        endpoint,
        Default::default(),
    )
    .unwrap();
    let stack = pending.peer_stack(0).unwrap();
    let mut scope = zero_platform_tokio::contract::scope::TaskScope::new(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(crate::runtime::raw_ip::EndpointStackOwner::TASK_LIMIT)
            .unwrap(),
    );
    stack.bind_scope(scope.registrar().downgrade()).unwrap();
    let device = stack.start(recipe).unwrap();
    let mut connecting = Box::pin(device.open_tcp_observed(
        "10.0.0.1".parse().unwrap(),
        "192.0.2.2:443".parse().unwrap(),
        None,
    ));
    std::future::poll_fn(|cx| {
        use std::future::Future;
        assert!(connecting.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    let syn = tokio::time::timeout_at(deadline, packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(zero_stack::packet::parse_tcp(&syn.packet).unwrap().syn);
    let link = Arc::new(LinkedEndpoint::new(
        [3; 32],
        7,
        config.listen,
        live,
        vec![device.clone()],
        vec![None],
        vec![None],
        updates,
        packets,
    ));
    pending.install(link.clone()).unwrap();
    assert!(Arc::ptr_eq(&pending.ready().unwrap(), &link));
    let (report, joined) = tokio::join!(
        pending.stop_peer_resources(deadline),
        scope.shutdown(deadline)
    );
    assert!(joined.is_complete());
    assert!(joined.failed_tasks.is_empty());
    assert!(report.is_complete());
    assert!(report.failures.is_empty());
    assert!(stack.is_drained());
    assert_eq!(report.native.len(), 1);
    assert_eq!(report.native[0].native.confirmed_closed, 0); // never acquired, no forged close.
    assert!(connecting.await.is_err());
    assert!(pending.stop_peer_resources(deadline).await.is_complete());
}
#[tokio::test]
async fn legacy_ready_link_never_inherits_a_noop_candidate_close_grant() {
    let (config, live) = candidate();
    let pending = PendingLinkedEndpoint::new(config.listen, live);
    let report = pending
        .stop_peer_resources(tokio::time::Instant::now())
        .await;
    assert!(!report.is_complete());
    assert!(!report.workers_drained);
    assert_eq!(report.failures.len(), 1);
    assert!(report.native.is_empty());
}

#[tokio::test]
async fn owned_candidate_cannot_install_an_unrelated_legacy_stack_as_its_original() {
    let (config, live) = candidate();
    let pending =
        PendingLinkedEndpoint::with_peer_slots(config.listen.clone(), live.clone(), 1).unwrap();
    let other = crate::runtime::raw_ip::EndpointStackOwner::new();
    let (sender, packets) = mpsc::channel(4);
    let (updates, endpoint) = watch::channel(sender);
    let recipe = crate::runtime::raw_ip::EndpointStackRecipe::prepare(
        vec!["10.0.0.1".parse().unwrap()],
        1420,
        0,
        endpoint,
        Default::default(),
    )
    .unwrap();
    let device = other.start(recipe).unwrap();
    let link = Arc::new(LinkedEndpoint::new(
        [3; 32],
        7,
        config.listen,
        live,
        vec![device],
        vec![None],
        vec![None],
        updates,
        packets,
    ));
    assert!(pending.install(link).is_err());
    assert!(pending.ready().is_none());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    assert!(other.stop(deadline).await.unwrap().is_complete());
    assert!(pending.stop_peer_resources(deadline).await.is_complete());
}
