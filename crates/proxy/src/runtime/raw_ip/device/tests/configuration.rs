#![cfg(feature = "wireguard")]
//! Actual host queue replacement, rejected candidate cleanup and recovery.
use crate::{
    runtime::{inbound_owners::InboundOwner, Proxy},
    ConfigApplyReconciler, ConfigReconcileResult, ProxyHandle,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use zero_config::{RuntimeConfig, UdpReceiveQueueConfig};
use zero_engine::{EngineHandle, EngineRuntimeSnapshot};

fn incarnations(proxy: &Proxy, config: &RuntimeConfig) -> Vec<u64> {
    proxy
        .protocols
        .test_outbound_devices(config)
        .into_iter()
        .filter(|device| device.is_usable())
        .map(|device| device.incarnation)
        .collect()
}
fn assert_closed(owner: &InboundOwner) {
    let terminal = owner
        .terminal_for_test()
        .expect("the same original listener Scope and native provider closed");
    assert_eq!(terminal.identity(), owner.identity());
    assert!(terminal.joined_tasks() > 0);
    assert!(terminal.closed_io_receipts() > 0);
}
struct RejectQueueAfterReady {
    proxy: Proxy,
    previous: Arc<EngineRuntimeSnapshot>,
    rejected: AtomicBool,
    candidate_listener: Mutex<Option<Arc<InboundOwner>>>,
}
#[async_trait::async_trait]
impl ConfigApplyReconciler for RejectQueueAfterReady {
    fn validate(&self, _: &RuntimeConfig, _: &RuntimeConfig) -> Result<(), String> {
        Ok(())
    }
    // This application fixture owns only the failure decision, no I/O/tasks.
    // Proxy's actual host remains responsible for the candidate's real cleanup.
    async fn retire_candidate(&self, _: Arc<RuntimeConfig>) -> Result<(), String> {
        Ok(())
    }
    async fn reconcile(&self, target: Arc<RuntimeConfig>) -> Result<ConfigReconcileResult, String> {
        if target.runtime.network.udp_receive_queue.capacity == 128
            && !self.rejected.swap(true, Ordering::AcqRel)
        {
            assert!(Arc::ptr_eq(
                &self.proxy.engine().runtime_snapshot(),
                &self.previous
            ));
            assert!(
                incarnations(&self.proxy, self.previous.config()).is_empty(),
                "retired original outbound stacks cannot route before recovery"
            );
            let tag = target.endpoints[0].inbound_tag();
            let owner = self.proxy.inbound_owners.owner(&tag).unwrap();
            owner.wait_ready().await.unwrap();
            *self.candidate_listener.lock().unwrap() = Some(owner);
            return Err("injected application failure after actual queue candidate Ready".into());
        }
        Ok(ConfigReconcileResult::default())
    }
}
#[tokio::test]
async fn unchanged_queue_policy_reuses_devices_and_failed_candidate_restores_after_confirmed_cleanup(
) {
    // The actual listener binds port0; the real peer socket remains reserved.
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let public = PublicKey::from(&StaticSecret::from([2; 32]));
    let config = RuntimeConfig::parse(
        &serde_json::json!({
            "endpoints":[{"tag":"test","directions":{"inbound":true,"outbound":true},
                "listen":{"address":"127.0.0.1","port":0},
                "protocol":{"type":"wireguard","private_key":STANDARD.encode([1;32]),
                    "addresses":["10.0.0.1/32"],"peers":[{
                        "public_key":STANDARD.encode(public.as_bytes()),
                        "endpoint":peer.local_addr().unwrap().to_string(),
                        "allowed_ips":["10.0.0.2/32"]}]}}],
            "route":{"rules":[],"final":{"type":"direct"}}
        })
        .to_string(),
    )
    .unwrap();
    let tag = config.endpoints[0].inbound_tag();
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
    let snapshot = proxy.engine().runtime_snapshot();
    let running = proxy.spawn();
    tokio::time::timeout(Duration::from_secs(5), async {
        while proxy
            .protocols
            .current_components(snapshot.config())
            .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let initial = incarnations(&proxy, &config);
    assert!(!initial.is_empty());
    let original_listener = proxy.inbound_owners.owner(&tag).unwrap();
    handle
        .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(incarnations(&proxy, &config), initial);
    assert!(Arc::ptr_eq(
        &original_listener,
        &proxy.inbound_owners.owner(&tag).unwrap()
    ));
    let previous = proxy.engine().runtime_snapshot();
    let originals = proxy.protocols.test_outbound_devices(previous.config());
    let original_joins: Vec<_> = originals
        .iter()
        .flat_map(|device| device.task.test_original_witnesses())
        .collect();
    assert!(!original_joins.is_empty());
    assert!(original_joins.iter().all(|joined| !joined.is_joined()));
    let mut changed = config.clone();
    changed.runtime.network.udp_receive_queue = UdpReceiveQueueConfig {
        capacity: 128,
        max_payload_bytes: Some(65_527),
        max_stack_payload_bytes: Some(131_054),
    };
    let failure = Arc::new(RejectQueueAfterReady {
        proxy: proxy.clone(),
        previous: previous.clone(),
        rejected: AtomicBool::new(false),
        candidate_listener: Mutex::new(None),
    });
    let rejected = handle.clone().with_config_apply_reconciler(failure.clone());
    let error = rejected
        .apply_runtime_config_and_wait(changed.clone(), Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(error.contains("injected application failure after actual queue candidate Ready"));
    assert!(
        error.contains("restored last-known-good configuration"),
        "{error}"
    );
    assert!(Arc::ptr_eq(&proxy.engine().runtime_snapshot(), &previous));
    assert!(originals.iter().all(|device| !device.is_usable()));
    assert!(original_joins.iter().all(|witness| {
        witness
            .try_joined()
            .is_some_and(|joined| joined.result().is_ok())
    }));
    let restored_listener = proxy.inbound_owners.owner(&tag).unwrap();
    let candidate_listener = failure.candidate_listener.lock().unwrap().clone().unwrap();
    if Arc::ptr_eq(&candidate_listener, &original_listener) {
        // A genuinely reused current listener is not candidate-owned native I/O.
        assert!(Arc::ptr_eq(&restored_listener, &original_listener));
        assert!(original_listener.terminal_for_test().is_none());
        assert!(!original_listener.cancellation().is_cancelled());
    } else {
        assert_closed(&original_listener);
        assert_closed(&candidate_listener);
        assert!(!Arc::ptr_eq(&candidate_listener, &restored_listener));
    }
    let restored = incarnations(&proxy, previous.config());
    assert!(!restored.is_empty());
    assert_ne!(
        restored, initial,
        "rollback reconstructs only after discarded candidate cleanup"
    );
    handle
        .apply_runtime_config_and_wait(changed.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    let replaced = incarnations(&proxy, &changed);
    assert_ne!(
        replaced, restored,
        "new limits replace the active stack policy"
    );
    handle
        .apply_runtime_config_and_wait(changed.clone(), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(incarnations(&proxy, &changed), replaced);
    let final_listener = proxy.inbound_owners.owner(&tag).unwrap();
    running.shutdown().await.unwrap();
    assert_closed(&final_listener);
    drop(peer);
}
