//! Ring policy belongs to real SDK originals; plain WG UDP keeps its live IO.
use super::*;
use std::sync::Arc;

#[tokio::test]
async fn ring_only_policy_keeps_plain_wg_source_stack_and_old_tcp_continuous() {
    let (endpoint, peer_task) = peer::echo_peer().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let config = configuration(endpoint, port, false);
    let tag = config.endpoints[0].inbound_tag();
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let memory = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .clone();
    let running = proxy.spawn();
    handle
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let original_owner = proxy.inbound_owners.owner(&tag).unwrap();
    original_owner.wait_ready().await.unwrap();
    assert!(!original_owner.requested_ring_source().unwrap());
    let original = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    let old_devices = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(old_devices.len(), 1);
    let mut client = tokio::time::timeout_at(end(), connect(port, false))
        .await
        .unwrap();
    client.write_all(b"before-ring-policy").await.unwrap();
    let mut first = [0; 18];
    tokio::time::timeout_at(end(), client.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&first, b"before-ring-policy");
    let workers = old_devices[0].test_original_worker_witnesses();
    assert!(!workers.is_empty());
    assert!(workers.iter().all(|worker| !worker.is_joined()));
    assert_eq!(memory.requested_rings().requested.occupied.frames, 0);

    let mut changed = config;
    changed.runtime.packet_windows_ring_capacity = Some(32 * 1024 * 1024);
    changed.runtime.packet_windows_ring_total_max_bytes = Some(256 * 1024 * 1024);
    handle
        .apply_runtime_config_and_wait(changed, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(
        &original_owner,
        &proxy.inbound_owners.owner(&tag).unwrap()
    ));
    let current = proxy
        .protocols
        .test_outbound_devices(proxy.engine().config().as_ref());
    assert_eq!(current.len(), 1);
    assert!(Arc::ptr_eq(&old_devices[0], &current[0]));
    let current_origin = proxy
        .protocols
        .test_packet_source_origins(proxy.engine().config().as_ref())
        .remove(0);
    assert!(original.same_original(&current_origin));
    assert!(original.closed().is_none());
    assert!(workers.iter().all(|worker| !worker.is_joined()));
    assert_eq!(memory.requested_rings().requested.occupied.frames, 0);
    client.write_all(b"after-ring-policy").await.unwrap();
    let mut last = [0; 17];
    tokio::time::timeout_at(end(), client.read_exact(&mut last))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&last, b"after-ring-policy");
    tokio::time::timeout_at(end(), running.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(workers.iter().all(|worker| worker.is_joined()));
    let receipt = original.closed().unwrap();
    assert!(receipt.matches(&original));
    assert!(receipt.had_native_acquisition());
    assert!(original
        .driver()
        .unwrap()
        .try_joined()
        .is_some_and(|joined| joined.registration_released()));
    assert_eq!(memory.native().occupied.frames, 0);
    assert_eq!(memory.native().occupied.bytes, 0);
    assert_eq!(memory.requested_rings().requested.occupied.frames, 0);
    peer_task.abort();
    assert!(peer_task.await.is_err_and(|source| source.is_cancelled()));
}
